package wal

import (
	"errors"
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

func TestNewWAL_OpenFileError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nope", "wal.log") 

	_, err := NewWAL(Config{Path: path})
	if err == nil {
		t.Fatal("expected error opening WAL file")
	}
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

func TestWAL_AppendEncodeError(t *testing.T) {
	w, _, cleanup := newTempWAL(t, SyncEveryWrite)
	defer cleanup()

	err := w.Append(WALRecord{
		Type:  RecordSet,
		Key:   "",
		Value: "v",
	})

	if err != ErrInvalidRecord {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
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

func TestWAL_ReplaySkipsEmptyLines(t *testing.T) {
	f, _ := os.CreateTemp("", "wal_empty_*.log")
	path := f.Name()
	defer os.Remove(path)

	_, _ = f.WriteString("\n\nSET a YQ==\n\n")
	f.Close()

	w, _ := NewWAL(Config{Path: path})
	defer w.Close()

	count := 0
	_ = w.Replay(func(WALRecord) error {
		count++
		return nil
	})

	if count != 1 {
		t.Fatalf("expected 1 record, got %d", count)
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
func TestWAL_AppendAfterCloseFastPath(t *testing.T) {
	w, _, cleanup := newTempWAL(t, SyncEveryWrite)
	defer cleanup()

	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	err := w.Append(WALRecord{
		Type:  RecordSet,
		Key:   "race",
		Value: "test",
	})

	if err != ErrWALClosed {
		t.Fatalf("expected ErrWALClosed, got %v", err)
	}
}

func TestWAL_AppendWhileClosing_NoPanic(t *testing.T) {
	w, _, cleanup := newTempWAL(t, SyncEveryWrite)
	defer cleanup()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
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

	_ = w.Close()
	wg.Wait()
}

func TestWAL_CloseWorkerStuck(t *testing.T) {
	f, err := os.CreateTemp("", "wal_stuck_*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	w := &wal{
		path:     path,
		file:     nil, // worker will panic if run, so we don't run it
		reqChan:  make(chan request),
		doneChan: make(chan struct{}),
	}

	err = w.Close()
	if err != ErrWorkerStuck {
		t.Fatalf("expected ErrWorkerStuck, got %v", err)
	}
}

func TestWAL_ReplayApplyError(t *testing.T) {
	w, _, cleanup := newTempWAL(t, SyncEveryWrite)
	defer cleanup()

	_ = w.Append(WALRecord{
		Type:  RecordSet,
		Key:   "x",
		Value: "y",
	})
	_ = w.Close()

	err := w.Replay(func(WALRecord) error {
		return errors.New("apply failed")
	})

	if err == nil {
		t.Fatal("expected apply error, got nil")
	}
}

func TestWAL_ReplayFileMissing(t *testing.T) {
	w, path, cleanup := newTempWAL(t, SyncEveryWrite)
	cleanup() // removes file

	err := w.Replay(func(WALRecord) error { return nil })
	if err == nil {
		t.Fatal("expected error when WAL file missing")
	}

	_ = os.Remove(path)
}

func TestWorker_SyncError(t *testing.T) {
	f, err := os.CreateTemp("", "wal_sync_err_*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	defer os.Remove(path)

	w, err := NewWAL(Config{Path: path, SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}

	// Close file under worker
	real := w.(*wal)
	_ = real.file.Close()

	err = w.Append(WALRecord{
		Type:  RecordSet,
		Key:   "k",
		Value: "v",
	})

	if err == nil {
		t.Fatal("expected sync/write error, got nil")
	}
}

func TestWAL_RotateRenameFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal")

	w, err := NewWAL(Config{Path: path, SyncPolicy: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}

	real := w.(*wal)

	// Break rename by removing directory permissions
	_ = os.Chmod(dir, 0500)
	defer os.Chmod(dir, 0700)

	err = real.rotate()
	if err == nil {
		t.Fatal("expected rotate failure")
	}
}
