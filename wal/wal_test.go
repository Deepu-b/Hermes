package wal

import (
	"os"
	"sync"
	"testing"
)

// helper to create a temp file
func createTempWAL(t *testing.T) (WAL, string) {
	tmpFile, err := os.CreateTemp("", "wal_test_*.log")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close() // Close it, let NewWAL open it
	os.Remove(tmpFile.Name()) // Start fresh

	w, err := NewWAL(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	return w, tmpFile.Name()
}

func TestWAL_BasicOperations(t *testing.T) {
	w, path := createTempWAL(t)
	defer os.Remove(path)
	defer w.Close()

	// 1. Append
	rec := WALRecord{Type: RecordSet, Key: "foo", Value: "bar"}
	if err := w.Append(rec); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// 2. Replay (on same instance or new instance)
	// Usually Replay is called on fresh start, so let's close and re-open
	w.Close()

	w2, err := NewWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	count := 0
	err = w2.Replay(func(r WALRecord) error {
		count++
		if r.Key != "foo" || r.Value != "bar" {
			t.Errorf("Replay got wrong data: %+v", r)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 record, got %d", count)
	}
}

func TestWAL_Concurrency(t *testing.T) {
	w, path := createTempWAL(t)
	defer os.Remove(path)
	defer w.Close()

	var wg sync.WaitGroup
	workers := 50
	
	// Spawn 50 goroutines writing simultaneously
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Append(WALRecord{Type: RecordSet, Key: "k", Value: "v"})
		}()
	}

	wg.Wait()

	// Verify file has 50 lines
	w.Close()
	_, _ = os.ReadFile(path)
	// Rough check: assuming "SET k dm...=" is X bytes. 
	// Better check: Replay and count.
	
	w2, _ := NewWAL(path)
	count := 0
	w2.Replay(func(r WALRecord) error {
		count++
		return nil
	})
	
	if count != workers {
		t.Errorf("Concurrency test lost data. Wanted %d, got %d", workers, count)
	}
}

func TestWAL_CloseIdempotency(t *testing.T) {
	w, path := createTempWAL(t)
	defer os.Remove(path)

	if err := w.Close(); err != nil {
		t.Errorf("First Close failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("Second Close failed: %v", err)
	}
	
	// Append after close should fail
	err := w.Append(WALRecord{Type: RecordSet, Key: "k", Value: "v"})
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}
}