package store

import (
	"bytes"
	"hermes/wal"
	"os"
	"testing"
	"time"
)

type storeCase struct {
	name string
	new  func() DataStore
}

var storeCases = []storeCase{
	{
		name: "Locked",
		new: func() DataStore {
			return NewLockedStore()
		},
	},
	{
		name: "Sharded",
		new: func() DataStore {
			return NewShardedStore(16)
		},
	},
	{
		name: "EventLoop",
		new: func() DataStore {
			return NewEventloopStore(100)
		},
	},
}

// Returns: store, walPath, closeWAL, cleanupFile
type StoreFactory func() (DataStore, string, func(), func())

func setupFactory(t *testing.T, newStore func() DataStore) StoreFactory {
	return func() (DataStore, string, func(), func()) {
		tmp, err := os.CreateTemp("", "wal_store_*.log")
		if err != nil {
			t.Fatalf("temp WAL: %v", err)
		}
		path := tmp.Name()
		tmp.Close()

		cfg := wal.Config{
			Path:       path,
			SyncPolicy: wal.SyncEveryWrite,
		}

		w, err := wal.NewWAL(cfg)
		if err != nil {
			t.Fatalf("new WAL: %v", err)
		}

		mem := newStore()

		ds, err := NewWalStore(mem, w)
		if err != nil {
			t.Fatalf("wal store: %v", err)
		}

		closeWAL := func() {
			_ = w.Close()
		}

		cleanup := func() {
			_ = os.Remove(path)
		}

		return ds, path, closeWAL, cleanup
	}
}

func TestWALStore_Compatibility(t *testing.T) {
	for _, sc := range storeCases {
		t.Run(sc.name, func(t *testing.T) {
			factory := setupFactory(t, sc.new)

			t.Run("Persistence", func(t *testing.T) {
				testPersistence(t, factory)
			})

			t.Run("Recovery", func(t *testing.T) {
				testRecovery(t, factory, sc.new)
			})

			t.Run("PhantomWriteProtection", func(t *testing.T) {
				testPhantomWrite(t, factory)
			})

			t.Run("Ordering", func(t *testing.T) {
				testOrdering(t, factory)
			})
		})
	}
}

func testPersistence(t *testing.T, factory StoreFactory) {
	store, _, closeWAL, cleanup := factory()
	defer closeWAL()
	defer cleanup()

	key := "pkey"
	val := []byte("pval")

	if err := store.Write(key, Entry{Value: val}, PutOverwrite); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	got, ok := store.Read(key)
	if !ok {
		t.Fatalf("read failed")
	}

	if !bytes.Equal(got.Value, val) {
		t.Fatalf("value mismatch")
	}
}

func testRecovery(t *testing.T, factory StoreFactory, newStore func() DataStore) {
	store, walPath, closeWAL, _ := factory()

	key := "survivor"
	val := []byte("alive")

	if err := store.Write(key, Entry{Value: val}, PutOverwrite); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Simulate crash
	closeWAL()

	cfg := wal.Config{
		Path:       walPath,
		SyncPolicy: wal.SyncEveryWrite,
	}

	w2, err := wal.NewWAL(cfg)
	if err != nil {
		t.Fatalf("new WAL: %v", err)
	}
	defer w2.Close()
	defer os.Remove(walPath)

	mem2 := newStore()
	store2, err := NewWalStore(mem2, w2)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	got, ok := store2.Read(key)
	if !ok || !bytes.Equal(got.Value, val) {
		t.Fatalf("recovery lost data")
	}
}

func testPhantomWrite(t *testing.T, factory StoreFactory) {
	store, walPath, closeWAL, cleanup := factory()
	defer closeWAL()
	defer cleanup()

	key := "exists"
	val1 := []byte("v1")
	val2 := []byte("v2")

	_ = store.Write(key, Entry{Value: val1}, PutOverwrite)

	err := store.Write(key, Entry{Value: val2}, PutIfAbsent)
	if err != ErrKeyExists {
		t.Fatalf("expected ErrKeyExists")
	}

	got, _ := store.Read(key)
	if !bytes.Equal(got.Value, val1) {
		t.Fatalf("memory corrupted")
	}

	cfg := wal.Config{
		Path:       walPath,
		SyncPolicy: wal.SyncEveryWrite,
	}

	raw, _ := wal.NewWAL(cfg)
	defer raw.Close()

	count := 0
	raw.Replay(func(r wal.WALRecord) error {
		if r.Key == key {
			count++
			if r.Value != string(val1) {
				t.Fatalf("unexpected WAL value: %s", r.Value)
			}
		}
		return nil
	})

	if count != 1 {
		t.Fatalf("phantom write detected: expected 1 record, got %d", count)
	}
}

func testOrdering(t *testing.T, factory StoreFactory) {
	store, walPath, closeWAL, cleanup := factory()
	defer closeWAL()
	defer cleanup()

	_ = store.Write("k", Entry{Value: []byte("1")}, PutOverwrite)
	_ = store.Write("k", Entry{Value: []byte("2")}, PutOverwrite)

	cfg := wal.Config{
		Path:       walPath,
		SyncPolicy: wal.SyncEveryWrite,
	}

	w2, _ := wal.NewWAL(cfg)
	defer w2.Close()

	mem := NewLockedStore()
	recovered, _ := NewWalStore(mem, w2)

	e, _ := recovered.Read("k")
	if string(e.Value) != "2" {
		t.Fatalf("ordering violated")
	}
}

func TestWalStore_Expire(t *testing.T) {
	factory := setupFactory(t, NewLockedStore)
	store, walPath, closeWAL, cleanup := factory()
	defer closeWAL()
	defer cleanup()

	key := "ttl"
	future := time.Now().Add(5 * time.Second).UnixMilli()

	_ = store.Write(key, Entry{Value: []byte("v")}, PutOverwrite)
	store.Expire(key, future)

	cfg := wal.Config{
		Path:       walPath,
		SyncPolicy: wal.SyncEveryWrite,
	}

	w2, _ := wal.NewWAL(cfg)
	defer w2.Close()

	mem := NewLockedStore()
	recovered, _ := NewWalStore(mem, w2)

	e, ok := recovered.Read(key)
	if !ok {
		t.Fatalf("key missing after recovery")
	}

	if e.ExpiresAtMillis != future {
		t.Fatalf("ttl mismatch")
	}
}
