package store

import (
	"hermes/wal"
	"os"
	"testing"
)

/*
Fake store that does NOT implement Iterable.
Used to validate capability guards.
*/
type nonIterableStore struct{}

func (n *nonIterableStore) Write(string, Entry, PutMode) error { return nil }
func (n *nonIterableStore) Read(string) (Entry, bool)          { return Entry{}, false }
func (n *nonIterableStore) Expire(string, int64) bool          { return false }
func (n *nonIterableStore) Close() error                       { return nil }

/*
Fake WAL that does NOT implement Rotate().
*/
type walWithoutRotate struct{}

func (w *walWithoutRotate) Append(wal.WALRecord) error             { return nil }
func (w *walWithoutRotate) Replay(func(wal.WALRecord) error) error { return nil }
func (w *walWithoutRotate) Close() error                           { return nil }

func TestCompact_FailsWithoutIterable(t *testing.T) {
	ws := &walStore{
		store:        &nonIterableStore{},
		wal:          &walWithoutRotate{},
		snapshotPath: "dummy",
	}

	err := ws.Compact()
	if err == nil {
		t.Fatalf("expected error for non-iterable store")
	}
}

func TestCompact_FailsWithoutRotate(t *testing.T) {
	tmp, _ := os.CreateTemp("", "snap_*.bin")
	defer os.Remove(tmp.Name())

	ws := &walStore{
		store:        NewLockedStore(),
		wal:          &walWithoutRotate{},
		snapshotPath: tmp.Name(),
	}

	err := ws.Compact()
	if err == nil {
		t.Fatalf("expected error for wal without Rotate")
	}
}
