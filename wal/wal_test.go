package wal

import (
	"os"
	"sync"
	"testing"
	"time"
)

func newTempWAL(t *testing.T, policy SyncPolicy) (WAL, string, func()) {
	t.Helper()

	f, err := os.CreateTemp("", "wal_test_*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)

	w, err := NewWAL(Config{
		Path:       path,
		SyncPolicy: policy,
	})
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		w.Close()
		os.Remove(path)
	}

	return w, path, cleanup
}

func TestWAL_AppendAndReplay(t *testing.T) {
	w, _, cleanup := newTempWAL(t, SyncEveryWrite)
	defer cleanup()

	if err := w.Append(WALRecord{
		Type:  RecordSet,
		Key:   "foo",
		Value: "bar",
	}); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	count := 0
	err := w.Replay(func(r WALRecord) error {
		count++
		if r.Key != "foo" || r.Value != "bar" {
			t.Fatalf("unexpected record: %+v", r)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 record, got %d", count)
	}
}

func TestWAL_CloseIsIdempotent(t *testing.T) {
	w, _, cleanup := newTempWAL(t, SyncEveryWrite)
	defer cleanup()

	if err := w.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second close failed: %v", err)
	}

	err := w.Append(WALRecord{Type: RecordSet, Key: "k", Value: "v"})
	if err != ErrWALClosed {
		t.Fatalf("expected ErrWALClosed, got %v", err)
	}
}

func TestWAL_ConcurrentAppends(t *testing.T) {
	w, _, cleanup := newTempWAL(t, SyncEveryWrite)
	defer cleanup()

	const writers = 50
	var wg sync.WaitGroup

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = w.Append(WALRecord{
				Type:  RecordSet,
				Key:   "k",
				Value: "v",
			})
		}(i)
	}

	wg.Wait()
	w.Close()

	count := 0
	err := w.Replay(func(r WALRecord) error {
		count++
		if r.Type != RecordSet || r.Key == "" {
			t.Fatalf("corrupt record: %+v", r)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if count != writers {
		t.Fatalf("expected %d records, got %d", writers, count)
	}
}

func TestWAL_BatchSyncFlushOnClose(t *testing.T) {
	w, path, cleanup := newTempWAL(t, SyncPolicy(100*time.Millisecond))
	defer cleanup()

	if err := w.Append(WALRecord{
		Type:  RecordSet,
		Key:   "batched",
		Value: "value",
	}); err != nil {
		t.Fatal(err)
	}

	w.Close()

	w2, err := NewWAL(Config{Path: path, SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	found := false
	err = w2.Replay(func(r WALRecord) error {
		if r.Key == "batched" {
			found = true
		}
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("data lost in batch mode on close")
	}
}

func TestWAL_BatchSyncFlushOnTick(t *testing.T) {
	interval := 10 * time.Millisecond
	w, path, cleanup := newTempWAL(t, SyncPolicy(interval))
	defer cleanup()

	if err := w.Append(WALRecord{
		Type:  RecordSet,
		Key:   "tick",
		Value: "flush",
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(interval * 3)

	w2, err := NewWAL(Config{Path: path, SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	count := 0
	err = w2.Replay(func(r WALRecord) error {
		count++
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 record after tick flush, got %d", count)
	}
}

func TestWAL_InvalidRecordFails(t *testing.T) {
	_, _, cleanup := newTempWAL(t, SyncEveryWrite)
	defer cleanup()

	_, err := DecodeRecord("SET only_one_arg")
	if err == nil {
		t.Fatal("expected decode error for invalid record")
	}
}

func TestWAL_ReplayStopsOnCorruption(t *testing.T) {
	f, err := os.CreateTemp("", "wal_corrupt_*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	defer os.Remove(path)

	f.WriteString("SET a b\n")
	f.WriteString("INVALID LINE\n")
	f.Close()

	w, err := NewWAL(Config{Path: path, SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	err = w.Replay(func(WALRecord) error { return nil })
	if err == nil {
		t.Fatal("expected replay to fail on corrupt WAL")
	}
}
