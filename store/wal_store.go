package store

import "hermes/wal"

/*
walStore implements the Decorator Pattern to add durability to an in-memory store.

Design Philosophy:
- Composite: Wraps any implementation of DataStore.
- Write-Ahead Logging: Always persists to disk BEFORE modifying memory.
- Crash Recovery: Reconstructs state by replaying the log on startup.

Trade-off (Consistency vs Latency):
This implementation chooses Strong Durability. Every Write() blocks until
the data is fsync'd to disk. This is safer but slower than asynchronous flushing.
*/
type walStore struct {
	store DataStore
	wal   wal.WAL
}

/*
NewWalStore initializes the durability layer and performs crash recovery.

Recovery Strategy (Replay):
On startup, we iterate through the entire WAL. The underlying 'store' starts empty
and is brought up-to-date by re-executing every historical command.

Note: Replay is synchronous and blocking. The system is not available for reads
until the entire log is processed.
*/
func NewWalStore(store DataStore, w wal.WAL) (DataStore, error) {
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
			_ = store.Expire(r.Key, r.Expire)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &walStore{
		store: store,
		wal:   w,
	}, nil
}

// Read bypasses the WAL entirely, offering memory-speed reads.
func (s *walStore) Read(key string) (Entry, bool) {
	return s.store.Read(key)
}

/*
Write executes a durable write operation.

Consistency Model (TOCTOU Warning):
There is a Time-of-Check to Time-of-Use race condition here.
1. We check 'exists' in memory.
2. We append to WAL.
3. We write to memory.
In a highly concurrent environment without external locking, another goroutine
could modify the key between step 1 and 3. Ideally, the underlying store
should provide a "Lock/Unlock" mechanism to make this atomic.
*/
func (s *walStore) Write(key string, value Entry, mode PutMode) error {
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
Expire sets a TTL on a key.

The key is checked if it exists before logging to avoid filling
the disk with useless commands for keys that don't exist.
*/
func (s *walStore) Expire(key string, unixTimestampMilli int64) bool {
	if _, exists := s.store.Read(key); !exists {
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
