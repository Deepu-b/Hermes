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

// Returns: store, walPath, snapPath, closeFn, cleanup
type StoreFactory func() (DataStore, string, string, func(), func())

func setupFactory(t *testing.T, newStore func() DataStore) StoreFactory {
	return func() (DataStore, string, string, func(), func()) {
		walFile, err := os.CreateTemp("", "wal_*.log")
		if err != nil {
			t.Fatal(err)
		}
		snapFile, err := os.CreateTemp("", "snapshot_*.bin")
		if err != nil {
			t.Fatal(err)
		}

		walPath := walFile.Name()
		snapPath := snapFile.Name()

		walFile.Close()
		snapFile.Close()

		cfg := wal.Config{
			Path:       walPath,
			SyncPolicy: wal.SyncEveryWrite,
		}

		w, err := wal.NewWAL(cfg)
		if err != nil {
			t.Fatal(err)
		}

		mem := newStore()

		ds, err := NewWalStore(mem, w, snapPath, 0)
		if err != nil {
			t.Fatal(err)
		}

		closeFn := func() {
			_ = ds.Close()
		}

		cleanup := func() {
			_ = os.Remove(walPath)
			_ = os.Remove(snapPath)
		}

		return ds, walPath, snapPath, closeFn, cleanup
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
	store, _, _, closeFn, cleanup := factory()
	defer closeFn()
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
	store, walPath, snapPath, closeFn, cleanup := factory()
	defer cleanup()

	key := "survivor"
	val := []byte("alive")

	if err := store.Write(key, Entry{Value: val}, PutOverwrite); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Simulate crash
	closeFn()

	cfg := wal.Config{
		Path:       walPath,
		SyncPolicy: wal.SyncEveryWrite,
	}

	w2, err := wal.NewWAL(cfg)
	if err != nil {
		t.Fatalf("new WAL: %v", err)
	}
	defer w2.Close()

	mem2 := newStore()
	store2, err := NewWalStore(mem2, w2, snapPath, 0)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	got, ok := store2.Read(key)
	if !ok || !bytes.Equal(got.Value, val) {
		t.Fatalf("recovery lost data")
	}
}

func testPhantomWrite(t *testing.T, factory StoreFactory) {
	store, walPath, _, closeFn, cleanup := factory()
	defer closeFn()
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

	raw, err := wal.NewWAL(cfg)
	if err != nil {
		t.Fatal(err)
	}
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
	store, walPath, snapPath, closeFn, cleanup := factory()
	defer closeFn()
	defer cleanup()

	_ = store.Write("k", Entry{Value: []byte("1")}, PutOverwrite)
	_ = store.Write("k", Entry{Value: []byte("2")}, PutOverwrite)

	cfg := wal.Config{
		Path:       walPath,
		SyncPolicy: wal.SyncEveryWrite,
	}

	w2, err := wal.NewWAL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	mem := NewLockedStore()
	recovered, err := NewWalStore(mem, w2, snapPath, 0)
	if err != nil {
		t.Fatal(err)
	}

	e, _ := recovered.Read("k")
	if string(e.Value) != "2" {
		t.Fatalf("ordering violated")
	}
}

func TestWalStore_Expire(t *testing.T) {
	factory := setupFactory(t, NewLockedStore)
	store, walPath, snapPath, closeFn, cleanup := factory()
	defer closeFn()
	defer cleanup()

	key := "ttl"
	future := time.Now().Add(5 * time.Second).UnixMilli()

	_ = store.Write(key, Entry{Value: []byte("v")}, PutOverwrite)
	store.Expire(key, future)

	cfg := wal.Config{
		Path:       walPath,
		SyncPolicy: wal.SyncEveryWrite,
	}

	w2, err := wal.NewWAL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	mem := NewLockedStore()
	recovered, err := NewWalStore(mem, w2, snapPath, 0)
	if err != nil {
		t.Fatal(err)
	}

	e, ok := recovered.Read(key)
	if !ok {
		t.Fatalf("key missing after recovery")
	}

	if e.ExpiresAtMillis != future {
		t.Fatalf("ttl mismatch")
	}
}

func TestWalStore_SnapshotRecovery(t *testing.T) {
	factory := setupFactory(t, NewLockedStore)

	store, walPath, snapPath, closeFn, cleanup := factory()
	defer cleanup()

	_ = store.Write("a", Entry{Value: []byte("1")}, PutOverwrite)
	_ = store.Write("b", Entry{Value: []byte("2")}, PutOverwrite)

	closeFn() // triggers snapshot + WAL close

	cfg := wal.Config{Path: walPath, SyncPolicy: wal.SyncEveryWrite}
	w2, err := wal.NewWAL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	mem := NewLockedStore()
	recovered, err := NewWalStore(mem, w2, snapPath, 0)
	if err != nil {
		t.Fatal(err)
	}

	v, ok := recovered.Read("b")
	if !ok || string(v.Value) != "2" {
		t.Fatalf("snapshot recovery failed")
	}
}

func TestWalStore_SnapshotPhantomProtection(t *testing.T) {
	factory := setupFactory(t, NewLockedStore)
	store, _, _, closeFn, cleanup := factory()
	defer cleanup()

	_ = store.Write("x", Entry{Value: []byte("1")}, PutOverwrite)
	err := store.Write("x", Entry{Value: []byte("2")}, PutIfAbsent)
	if err != ErrKeyExists {
		t.Fatal("expected ErrKeyExists")
	}

	closeFn()

	val, _ := store.Read("x")
	if string(val.Value) != "1" {
		t.Fatalf("phantom write leaked into snapshot")
	}
}

func TestWalStore_SnapshotExpire(t *testing.T) {
	factory := setupFactory(t, NewLockedStore)
	store, walPath, snapPath, closeFn, cleanup := factory()
	defer cleanup()

	exp := time.Now().Add(time.Hour).UnixMilli()
	_ = store.Write("ttl", Entry{Value: []byte("v")}, PutOverwrite)
	store.Expire("ttl", exp)

	closeFn()

	cfg := wal.Config{Path: walPath, SyncPolicy: wal.SyncEveryWrite}
	w2, err := wal.NewWAL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	mem := NewLockedStore()
	recovered, err := NewWalStore(mem, w2, snapPath, 0)
	if err != nil {
		t.Fatal(err)
	}

	e, ok := recovered.Read("ttl")
	if !ok || e.ExpiresAtMillis != exp {
		t.Fatalf("TTL lost during snapshot recovery")
	}
}

func TestSnapshotSupervisor_RunsAndStops(t *testing.T) {
	walFile, _ := os.CreateTemp("", "wal_*.log")
	snapFile, _ := os.CreateTemp("", "snap_*.bin")
	defer os.Remove(walFile.Name())
	defer os.Remove(snapFile.Name())

	w, _ := wal.NewWAL(wal.Config{
		Path:       walFile.Name(),
		SyncPolicy: wal.SyncEveryWrite,
	})

	ds, err := NewWalStore(
		NewLockedStore(),
		w,
		snapFile.Name(),
		10*time.Millisecond,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Let supervisor tick at least once
	time.Sleep(25 * time.Millisecond)

	if err := ds.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestWalStore_PutUpdateSemantics(t *testing.T) {
	factory := setupFactory(t, NewLockedStore)
	store, walPath, _, closeFn, cleanup := factory()
	defer closeFn()
	defer cleanup()

	// Update missing key
	err := store.Write("x", Entry{Value: []byte("v")}, PutUpdate)
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound")
	}

	// Insert then update
	_ = store.Write("x", Entry{Value: []byte("1")}, PutOverwrite)
	err = store.Write("x", Entry{Value: []byte("2")}, PutUpdate)
	if err != nil {
		t.Fatalf("update failed")
	}

	cfg := wal.Config{Path: walPath, SyncPolicy: wal.SyncEveryWrite}
	raw, _ := wal.NewWAL(cfg)
	defer raw.Close()

	count := 0
	raw.Replay(func(r wal.WALRecord) error {
		if r.Key == "x" {
			count++
		}
		return nil
	})

	if count != 2 {
		t.Fatalf("expected exactly 2 WAL records, got %d", count)
	}
}

func TestWalStore_ExpireOnMissingKey(t *testing.T) {
	factory := setupFactory(t, NewLockedStore)
	store, _, _, closeFn, cleanup := factory()
	defer closeFn()
	defer cleanup()

	if store.Expire("missing", time.Now().UnixMilli()) {
		t.Fatalf("expire should fail on missing key")
	}
}

func TestWalStore_ExpireNegativeTimestamp(t *testing.T) {
	factory := setupFactory(t, NewLockedStore)
	store, _, _, closeFn, cleanup := factory()
	defer closeFn()
	defer cleanup()

	_ = store.Write("k", Entry{Value: []byte("v")}, PutOverwrite)
	if store.Expire("k", -1) {
		t.Fatalf("expire should fail for negative timestamp")
	}
}

func TestWalStore_ReplayRejectsInvalidExpire(t *testing.T) {
	walFile, _ := os.CreateTemp("", "wal_*.log")
	snapFile, _ := os.CreateTemp("", "snap_*.bin")
	defer os.Remove(walFile.Name())
	defer os.Remove(snapFile.Name())

	walFile.WriteString("EXPIRE key -10\n")
	walFile.Close()

	w, _ := wal.NewWAL(wal.Config{
		Path:       walFile.Name(),
		SyncPolicy: wal.SyncEveryWrite,
	})
	defer w.Close()

	_, err := NewWalStore(NewLockedStore(), w, snapFile.Name(), 0)
	if err == nil {
		t.Fatalf("expected recovery failure for invalid EXPIRE")
	}
}

func TestWalStore_CorruptSnapshotFailsRecovery(t *testing.T) {
	walFile, _ := os.CreateTemp("", "wal_*.log")
	snapFile, _ := os.CreateTemp("", "snap_*.bin")
	defer os.Remove(walFile.Name())
	defer os.Remove(snapFile.Name())

	// Write garbage snapshot
	snapFile.Write([]byte("corrupt data"))
	snapFile.Close()

	w, _ := wal.NewWAL(wal.Config{
		Path:       walFile.Name(),
		SyncPolicy: wal.SyncEveryWrite,
	})
	defer w.Close()

	_, err := NewWalStore(NewLockedStore(), w, snapFile.Name(), 0)
	if err == nil {
		t.Fatalf("expected snapshot load failure")
	}
}
