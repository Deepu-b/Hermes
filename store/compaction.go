package store

import (
	"errors"
	"hermes/snapshot"
	"os"
	"path/filepath"
	"time"
)

/*
Compact performs snapshot-based compaction.

High-level algorithm (Stop-the-World):
1. Block all writes using a global lock
2. Stream all live entries into a temporary snapshot
3. fsync snapshot to guarantee durability
4. Rotate WAL to establish a new clean baseline
5. Atomically promote snapshot

Design trade-offs:
- Writes are paused during compaction
- Simpler correctness model
- Snapshot frequency should be low
*/
func (s *walStore) Compact() error {
	// Capability check: store must support iteration
	iterStore, ok := s.store.(Iterable)
	if !ok {
		return errors.New("underlying store does not support iteration")
	}

	// Capability check: WAL must support rotation
	rotator, ok := s.wal.(interface{ Rotate() error })
	if !ok {
		return errors.New("wal does not support rotation")
	}

	// Stop-the-world: block all writers
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ensure snapshot directory exists
	dir := filepath.Dir(s.snapshotPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
        	return err
    	}

	// Write snapshot to a temporary file
	tempSnap, err := os.CreateTemp(dir, "snapshot-*.bin")
	if err != nil {
		return err
	}

	tempName := tempSnap.Name()
	defer func() {
		tempSnap.Close()
		// Cleanup on failure
		if err != nil {
			os.Remove(tempName)
		}
	}()

	// Adapter bridges store.Iterate to snapshot.Streamer
	adaptor := func(yield func(snapshot.Item) bool) {
		iterStore.Iterate(func(key string, value Entry) bool {
			return yield(snapshot.Item{
				Key:       key,
				Value:     value.Value,
				ExpiresAt: value.ExpiresAtMillis,
			})
		})
	}

	// Persist snapshot
	if err = snapshot.Write(tempSnap, adaptor); err != nil {
		return err
	}

	// Ensure snapshot durability
	if err = tempSnap.Sync(); err != nil {
		return err
	}

	// Atomically promote snapshot
	if err = os.Rename(tempName, s.snapshotPath); err != nil {
		return err
	}

	// Rotate WAL AFTER snapshot is durable
	if err = rotator.Rotate(); err != nil {
		return err
	}

	return nil
}

/*
startSnapshotSupervisor periodically triggers compaction.

This runs independently of client operations but respects
the global write lock during snapshot creation.

Snapshots are best-effort:
- Failures do not affect correctness
- WAL remains the source of truth
*/
func (s *walStore) startSnapshotSupervisor(interval time.Duration) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				_ = s.Compact()
			case <-s.doneChan:
				return
			}
		}
	}()
}
