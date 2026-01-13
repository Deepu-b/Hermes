package store

import (
	"hermes/snapshot"
	"hermes/wal"
	"os"
	"sync"
	"time"
)

/*
walStore implements the Decorator Pattern to add durability to an in-memory store.

Design Philosophy:
- Composite: Wraps any implementation of DataStore.
- Write-Ahead Logging: Always persists to disk BEFORE modifying memory.
- Crash Recovery: Coordinates snapshotting and WAL rotation AND Reconstructs state by replaying the snapshot and log on startup.

Trade-off (Consistency vs Latency):
- This implementation chooses Strong Durability.
*/
type walStore struct {
	// store is the underlying in-memory store.
	// It can be locked, sharded, or event-loop based.
	store DataStore

	// wal is the durability layer.
	// It records intent (SET / EXPIRE), not internal mutations.
	wal   wal.WAL

	// snapshotPath is the on-disk snapshot location.
	// Snapshot + WAL together form the full recovery state.
	snapshotPath string

	/*
		mu coordinates compaction with live traffic.

		RLock:
		- Used by Write / Expire
		- Allows concurrent writes

		Lock:
		- Used by Compact()
		- Stops the world to take a consistent snapshot

		This mirrors real-world designs (Redis, RocksDB early phases)
		where compaction is rare but correctness-critical.
	*/
	mu           sync.RWMutex

	// doneChan signals background goroutines (snapshot supervisor)
	// to shut down gracefully.
	doneChan     chan struct{}

	// wg tracks background goroutines to ensure clean shutdown.
	wg           sync.WaitGroup
}

/*
NewWalStore initializes the durability layer and performs crash recovery.

Recovery Strategy (Replay):
On startup, we iterate through the entire WAL. The underlying 'store' starts empty
and is brought up-to-date by re-executing every historical command.

Startup sequence (very intentional order):
1. Load snapshot (if present)
   - Fast path for large datasets
   - Snapshot represents a consistent point-in-time view

2. Replay WAL
   - WAL is the source of truth
   - Re-applies mutations AFTER snapshot

3. Start snapshot supervisor (optional)
   - Periodic background compaction


Note: Replay is synchronous and blocking. The system is not available for reads
until the entire log is processed.
*/
func NewWalStore(
	store DataStore,
	w wal.WAL,
	snapshotPath string,
	snapshotInterval time.Duration,
) (DataStore, error) {

	// Phase 1: Load snapshot if it exists
	if f, err := os.Open(snapshotPath); err == nil {
		defer f.Close()

		/*
			The loader function adapts snapshot.Item
			back into store.Entry.

			PutOverwrite is forced because snapshots
			represent authoritative state.
		*/
		loader := func(item snapshot.Item) {
			store.Write(item.Key, Entry{
				Value:           item.Value,
				ExpiresAtMillis: item.ExpiresAt,
			}, PutOverwrite)
		}

		if err = snapshot.Load(f, loader); err != nil {
			return nil, err
		}
	}

	// Phase 2: Replay WAL
	err := w.Replay(func(r wal.WALRecord) error {
		switch r.Type {
		case wal.RecordSet:
			// Replay Logic:
			// We force PutOverwrite because the log represents the definitive
			// history. If the log says "A=1" then "A=2", replaying them
			// in order naturally results in the correct final state "A=2".
			return store.Write(
				r.Key,
				Entry{Value: []byte(r.Value), ExpiresAtMillis: 0},
				PutOverwrite,
			)

		case wal.RecordExpire:
			// Invalid expiration values are rejected early
			if r.Expire < 0 {
				return wal.ErrInvalidRecord
			}
			_ = store.Expire(r.Key, r.Expire)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	ws := &walStore{
		store:        store,
		wal:          w,
		snapshotPath: snapshotPath,
		doneChan:     make(chan struct{}),
	}

	// Phase 3: Start snapshot supervisor (optional)
	if snapshotInterval > 0 {
		ws.startSnapshotSupervisor(snapshotInterval)
	}

	return ws, nil
}

/*
Read bypasses the WAL entirely.

Rationale:
- WAL is only for durability of mutations
- Reads should be memory-fast
- WAL replay already ensures memory correctness
*/
func (s *walStore) Read(key string) (Entry, bool) {
	return s.store.Read(key)
}

/*
Write performs a durable write with strict ordering guarantees.

Write ordering:
1. Validate in-memory state (fail fast)
2. Append intent to WAL
3. Mutate memory

Why validation BEFORE WAL append:
- Prevents "phantom writes"
- A rejected operation must not appear in the WAL

Locking:
- RLock allows concurrent writers
- Blocks if compaction is running
*/
func (s *walStore) Write(key string, value Entry, mode PutMode) error {
	s.mu.RLock() // Allows concurrent writes, but blocks if Compact holds Lock
	defer s.mu.RUnlock()

	// 1. Validation Logic (Fail Fast)
	// We check memory state BEFORE touching disk to prevent "Phantom Writes"
	// (failed writes that end up in the log anyway).
	switch mode {
	case PutIfAbsent:
		if _, exists := s.store.Read(key); exists {
			return ErrKeyExists
		}

	case PutUpdate:
		if _, exists := s.store.Read(key); !exists {
			return ErrKeyNotFound
		}
	}

	value.ExpiresAtMillis = 0
	err := s.wal.Append(wal.WALRecord{
		Type:  wal.RecordSet,
		Key:   key,
		Value: string(value.Value),
	})
	if err != nil {
		return err
	}

	// Only after disk success do we make the data visible to readers
	return s.store.Write(key, value, mode)
}

/*
Expire sets an absolute expiration timestamp.

Design choices:
- TTL is stored as absolute Unix milliseconds
- WAL records EXPIRE as a first-class operation
- Expire of non-existent keys is ignored

Consistency:
- WAL append happens BEFORE memory mutation
*/
func (s *walStore) Expire(key string, unixTimestampMilli int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.store.Read(key); !exists {
		return false
	}

	if unixTimestampMilli < 0 {
		return false
	}

	err := s.wal.Append(wal.WALRecord{
		Type:   wal.RecordExpire,
		Key:    key,
		Expire: unixTimestampMilli,
	})
	if err != nil {
		// If persistence fails, we fail the operation to maintain consistency properties.
		return false
	}

	return s.store.Expire(key, unixTimestampMilli)
}

/*
Close shuts down the walStore and releases all resources.

Shutdown order (important):

1. Stop snapshot supervisor
2. Wait for background goroutines
3. Perform final snapshot (best-effort)
4. Close WAL (flush + fsync)

Why Close() exists on DataStore:
- Allows composite stores (walStore) to own resources
- Enables clean shutdown in servers and tests
- In-memory stores can implement Close() as a no-op
*/
func (s *walStore) Close() error {
	// Stop Supervisor
	close(s.doneChan)
	s.wg.Wait()

	// Best-effort final snapshot to reduce recovery time
	if err := s.Compact(); err != nil {
		return err
	}

	return s.wal.Close()
}
