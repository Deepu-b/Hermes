package wal

import (
	"os"
	"path/filepath"
	"strings"
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
		_ = w.Close()
		_ = os.Remove(path)
	}

	return w, path, cleanup
}

func TestWAL_AppendAndReplay(t *testing.T) {
	w, _, cleanup := newTempWAL(t, SyncEveryWrite)
	defer cleanup()

	err := w.Append(WALRecord{
		Type:  RecordSet,
		Key:   "foo",
		Value: "bar",
	})
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	count := 0
	err = w.Replay(func(r WALRecord) error {
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

func TestWAL_ReplayEmptyLog(t *testing.T) {
	w, _, cleanup := newTempWAL(t, SyncEveryWrite)
	defer cleanup()

	err := w.Replay(func(WALRecord) error {
		t.Fatal("should not replay any records")
		return nil
	})

	if err != nil {
		t.Fatalf("replay failed on empty log: %v", err)
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
		go func() {
			defer wg.Done()
			_ = w.Append(WALRecord{
				Type:  RecordSet,
				Key:   "k",
				Value: "v",
			})
		}()
	}

	wg.Wait()
	_ = w.Close()

	count := 0
	err := w.Replay(func(WALRecord) error {
		count++
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

	_ = w.Append(WALRecord{
		Type:  RecordSet,
		Key:   "batched",
		Value: "value",
	})

	_ = w.Close()

	w2, err := NewWAL(Config{Path: path, SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	found := false
	_ = w2.Replay(func(r WALRecord) error {
		if r.Key == "batched" {
			found = true
		}
		return nil
	})

	if !found {
		t.Fatal("data lost in batch mode on close")
	}
}

func TestWAL_BatchSyncFlushOnTick(t *testing.T) {
	interval := 10 * time.Millisecond
	w, path, cleanup := newTempWAL(t, SyncPolicy(interval))
	defer cleanup()

	_ = w.Append(WALRecord{
		Type:  RecordSet,
		Key:   "tick",
		Value: "flush",
	})

	time.Sleep(interval * 3)

	w2, err := NewWAL(Config{Path: path, SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	count := 0
	_ = w2.Replay(func(WALRecord) error {
		count++
		return nil
	})

	if count != 1 {
		t.Fatalf("expected 1 record after tick flush, got %d", count)
	}
}

func TestWAL_Rotate(t *testing.T) {
	w, path, cleanup := newTempWAL(t, SyncEveryWrite)
	defer cleanup()

	_ = w.Append(WALRecord{Type: RecordSet, Key: "a", Value: "1"})
	rotator, ok := w.(interface{ Rotate() error })
	if !ok {
		t.Fatalf("wal does not support rotation")
	}
	err := rotator.Rotate()
	if err != nil {
		t.Fatalf("rotate failed: %v", err)
	}

	_ = w.Append(WALRecord{Type: RecordSet, Key: "b", Value: "2"})
	_ = w.Close()

	dir := filepath.Dir(path)
	baseName := filepath.Base(path)

	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	rotatedFound := false
	for _, f := range files {
		if len(f.Name()) > len(baseName) && strings.HasPrefix(f.Name(), baseName) {
			rotatedFound = true
			// Cleanup the rotated file so we don't pollute /tmp
			_ = os.Remove(filepath.Join(dir, f.Name()))
		}
	}

	if !rotatedFound {
		t.Fatal("rotated WAL file not found")
	}
}

func TestWAL_RotateAfterClose(t *testing.T) {
	w, _, cleanup := newTempWAL(t, SyncEveryWrite)
	defer cleanup()

	_ = w.Close()
	rotator, ok := w.(interface{ Rotate() error })
	if !ok {
		t.Fatalf("wal does not support rotation")
	}
	if err := rotator.Rotate(); err != ErrWALClosed {
		t.Fatalf("expected ErrWALClosed, got %v", err)
	}
}

func TestWAL_ReplayStopsOnCorruption(t *testing.T) {
	f, err := os.CreateTemp("", "wal_corrupt_*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	defer os.Remove(path)

	_, _ = f.WriteString("SET key dmFs\n") 
	
	_, _ = f.WriteString("INVALID LINE\n")
	f.Close()

	w, err := NewWAL(Config{Path: path, SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	count := 0
	err = w.Replay(func(r WALRecord) error {
		if r.Key == "key" && r.Value == "val" {
			count++
		}
		return nil
	})

	if err != nil {
		t.Fatalf("replay should succeed with truncation, got %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 valid record before corruption, got %d", count)
	}
}