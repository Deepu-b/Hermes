package store

import (
	"testing"
	"time"
)

func TestPutOverwrite(t *testing.T) {
	store := NewStore()

	err := store.Write("a", Entry{Value: []byte("1")}, PutOverwrite)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = store.Write("a", Entry{Value: []byte("2")}, PutOverwrite)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val, ok := store.Read("a")
	if !ok {
		t.Fatalf("expected key to exist")
	}

	if string(val.Value) != "2" {
		t.Fatalf("expected value '2', got '%s'", val.Value)
	}
}

func TestPutIfAbsent(t *testing.T) {
	store := NewStore()

	err := store.Write("a", Entry{Value: []byte("1")}, PutIfAbsent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = store.Write("a", Entry{Value: []byte("2")}, PutIfAbsent)
	if err != ErrKeyExists {
		t.Fatalf("expected ErrKeyExists, got %v", err)
	}

	val, _ := store.Read("a")
	if string(val.Value) != "1" {
		t.Fatalf("value should not have been overwritten")
	}
}

func TestPutUpdate(t *testing.T) {
	store := NewStore()

	err := store.Write("a", Entry{Value: []byte("1")}, PutUpdate)
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	_ = store.Write("a", Entry{Value: []byte("1")}, PutOverwrite)

	err = store.Write("a", Entry{Value: []byte("2")}, PutUpdate)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val, _ := store.Read("a")
	if string(val.Value) != "2" {
		t.Fatalf("expected updated value")
	}
}

func TestInvalidPutMode(t *testing.T) {
	store := NewStore()

	err := store.Write("a", Entry{Value: []byte("1")}, PutMode(10))
	if err != ErrInvalidPutMode {
		t.Fatalf("expected ErrInvalidPutMode, got %v", err)
	}
}

func TestReadWithoutTTL(t *testing.T) {
	store := NewStore()

	_ = store.Write("a", Entry{Value: []byte("1")}, PutOverwrite)

	val, ok := store.Read("a")
	if !ok {
		t.Fatalf("expected key to exist")
	}

	if string(val.Value) != "1" {
		t.Fatalf("unexpected value")
	}
}

func TestExpireExistingKey(t *testing.T) {
	store := NewStore()

	_ = store.Write("a", Entry{Value: []byte("1")}, PutOverwrite)

	expireTime := time.Now().Add(50*time.Millisecond)
	ok := store.Expire("a", GetUnixTimestamp(expireTime))
	if !ok {
		t.Fatalf("expected expire to succeed")
	}

	val, ok := store.Read("a")
	if !ok {
		t.Fatalf("key should exist before expiry")
	}

	if string(val.Value) != "1" {
		t.Fatalf("unexpected value")
	}
}

func TestExpiredKeyIsDeletedOnRead(t *testing.T) {
	store := NewStore()

	_ = store.Write("a", Entry{Value: []byte("1")}, PutOverwrite)
	_ = store.Expire("a", GetUnixTimestamp(time.Now().Add(10*time.Millisecond)))

	time.Sleep(20 * time.Millisecond)

	_, ok := store.Read("a")
	if ok {
		t.Fatalf("expected key to be expired")
	}

	_, ok = store.Read("a")
	if ok {
		t.Fatalf("expired key should not reappear")
	}
}

func TestExpireMissingKey(t *testing.T) {
	store := NewStore()

	ok := store.Expire("missing", GetUnixTimestamp(time.Now().Add(10*time.Second)))
	if ok {
		t.Fatalf("expected expire to fail for missing key")
	}
}

func TestExpireDoesNotResurrectExpiredKey(t *testing.T) {
	store := NewStore()

	_ = store.Write("a", Entry{Value: []byte("1")}, PutOverwrite)
	_ = store.Expire("a", GetUnixTimestamp(time.Now().Add(10*time.Millisecond)))

	time.Sleep(20 * time.Millisecond)

	ok := store.Expire("a", GetUnixTimestamp(time.Now().Add(time.Second)))
	if ok {
		t.Fatalf("expected expire to fail on expired key")
	}
}

func TestEventLoopStore_Close(t *testing.T) {
	s := NewEventloopStore(1)

	err := s.Close()
	if err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestLockedStore_Close(t *testing.T) {
	s := NewLockedStore()
	if err := s.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestShardedStore_IterateEarlyStop(t *testing.T) {
	s := NewShardedStore(4)

	_ = s.Write("a", Entry{Value: []byte("1")}, PutOverwrite)
	_ = s.Write("b", Entry{Value: []byte("2")}, PutOverwrite)

	count := 0
	iterable, ok := s.(Iterable)
	if !ok {
		t.Fatalf("store is not iterable")
	}
	iterable.Iterate(func(key string, value Entry) bool {
		count++
		return false // stop immediately
	})

	if count != 1 {
		t.Fatalf("expected early stop, got %d", count)
	}
}

func TestShardedStore_Close(t *testing.T) {
	s := NewShardedStore(2)
	if err := s.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}


func TestStore_IterateSkipsExpired(t *testing.T) {
	s := &store{data: make(map[string]Entry)}

	s.set("live", Entry{Value: []byte("ok")})
	s.set("dead", Entry{
		Value: []byte("x"),
		ExpiresAtMillis: GetUnixTimestamp(time.Now()) - 1,
	})

	keys := map[string]bool{}
	s.Iterate(func(k string, _ Entry) bool {
		keys[k] = true
		return true
	})

	if keys["dead"] {
		t.Fatal("expired key was iterated")
	}
	if !keys["live"] {
		t.Fatal("live key missing")
	}
}

func TestStore_IterateEarlyStop(t *testing.T) {
	s := &store{data: make(map[string]Entry)}
	s.set("a", Entry{Value: []byte("1")})
	s.set("b", Entry{Value: []byte("2")})

	count := 0
	s.Iterate(func(string, Entry) bool {
		count++
		return false
	})

	if count != 1 {
		t.Fatalf("expected early stop, got %d", count)
	}
}

func TestStore_Close(t *testing.T) {
	s := &store{data: make(map[string]Entry)}
	if err := s.Close(); err != nil {
		t.Fatalf("close failed")
	}
}
