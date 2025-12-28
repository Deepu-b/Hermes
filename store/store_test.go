package store

import "testing"

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
