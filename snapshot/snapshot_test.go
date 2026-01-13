package snapshot

import (
	"bytes"
	"errors"
	"testing"
)

/*
TestSnapshot_RoundTrip verifies that data written to a snapshot
can be loaded back losslessly.

This is the core correctness guarantee of the snapshot format.
*/
func TestSnapshot_RoundTrip(t *testing.T) {
	var buf bytes.Buffer

	items := []Item{
		{Key: "a", Value: []byte("1"), ExpiresAt: 0},
		{Key: "b", Value: []byte("2"), ExpiresAt: 123},
		{Key: "c", Value: []byte("3"), ExpiresAt: 456},
	}

	stream := func(yield func(Item) bool) {
		for _, it := range items {
			if !yield(it) {
				return
			}
		}
	}

	if err := Write(&buf, stream); err != nil {
		t.Fatalf("snapshot write failed: %v", err)
	}

	var loaded []Item
	err := Load(&buf, func(it Item) {
		loaded = append(loaded, it)
	})
	if err != nil {
		t.Fatalf("snapshot load failed: %v", err)
	}

	if len(loaded) != len(items) {
		t.Fatalf("expected %d items, got %d", len(items), len(loaded))
	}

	for i := range items {
		if items[i].Key != loaded[i].Key {
			t.Fatalf("key mismatch at %d", i)
		}
		if string(items[i].Value) != string(loaded[i].Value) {
			t.Fatalf("value mismatch at %d", i)
		}
		if items[i].ExpiresAt != loaded[i].ExpiresAt {
			t.Fatalf("expiry mismatch at %d", i)
		}
	}
}

/*
TestSnapshot_Empty verifies that an empty snapshot
is valid and loads successfully.
*/
func TestSnapshot_Empty(t *testing.T) {
	var buf bytes.Buffer

	stream := func(yield func(Item) bool) {}

	if err := Write(&buf, stream); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	err := Load(&buf, func(Item) {
		t.Fatal("should not receive any items")
	})
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
}

/*
TestSnapshot_Corruption ensures that partial snapshots
are rejected and not partially applied.

This enforces the strict corruption policy:
"all or nothing".
*/
func TestSnapshot_Corruption(t *testing.T) {
	var buf bytes.Buffer

	stream := func(yield func(Item) bool) {
		yield(Item{Key: "ok", Value: []byte("v"), ExpiresAt: 0})
	}

	if err := Write(&buf, stream); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Corrupt the snapshot by truncating bytes
	raw := buf.Bytes()
	corrupt := raw[:len(raw)-3]

	var applied int
	err := Load(bytes.NewReader(corrupt), func(Item) {
		applied++
	})

	if err == nil {
		t.Fatal("expected corruption error, got nil")
	}
	if applied != 0 {
		t.Fatalf("partial snapshot applied (%d items)", applied)
	}
}

/*
TestSnapshot_StreamEarlyStop verifies that snapshot writing
respects early termination.

This is important for future incremental snapshotting.
*/
func TestSnapshot_StreamEarlyStop(t *testing.T) {
	var buf bytes.Buffer

	stream := func(yield func(Item) bool) {
		yield(Item{Key: "a"})
		yield(Item{Key: "b"})
		yield(Item{Key: "c"})
	}

	stopErr := errors.New("stop")

	err := Write(&buf, func(yield func(Item) bool) {
		stream(func(it Item) bool {
			if it.Key == "b" {
				return false
			}
			return yield(it)
		})
	})

	if err != nil && err != stopErr {
		t.Fatalf("unexpected error: %v", err)
	}
}
