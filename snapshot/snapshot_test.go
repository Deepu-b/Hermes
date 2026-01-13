package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

type failingWriter struct {
	writes int
	failAt int
}

func (f *failingWriter) Write(p []byte) (int, error) {
	f.writes++
	if f.writes >= f.failAt {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("synthetic read error")
}

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

func TestSnapshot_WriteStopsAfterError(t *testing.T) {
	w := &failingWriter{failAt: 2}

	err := Write(w, func(yield func(Item) bool) {
		yield(Item{Key: "a", Value: []byte("1")})
		yield(Item{Key: "b", Value: []byte("2")})
	})

	if err == nil {
		t.Fatal("expected write error")
	}
}

func TestSnapshot_LoadBinaryReadError(t *testing.T) {
	err := Load(errorReader{}, func(Item) {})
	if err == nil {
		t.Fatal("expected read error")
	}
}

func TestSnapshot_LoadNegativeKeyLen(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, int32(-1))

	err := Load(&buf, func(Item) {})
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected ErrUnexpectedEOF, got %v", err)
	}
}


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

func TestSnapshot_LoadKeyReadFailure(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, int32(5))
	buf.Write([]byte("ab")) // truncated

	err := Load(&buf, func(Item) {})
	if err == nil {
		t.Fatal("expected read error")
	}
}

func TestSnapshot_LoadNegativeValueLen(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, int32(1))
	buf.Write([]byte("k"))
	_ = binary.Write(&buf, binary.LittleEndian, int32(-1))

	err := Load(&buf, func(Item) {})
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestSnapshot_LoadValueReadFailure(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, int32(1))
	buf.Write([]byte("k"))
	_ = binary.Write(&buf, binary.LittleEndian, int32(5))
	buf.Write([]byte("ab")) // truncated

	err := Load(&buf, func(Item) {})
	if err == nil {
		t.Fatal("expected read error")
	}
}

func TestSnapshot_LoadExpireReadFailure(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, int32(1))
	buf.Write([]byte("k"))
	_ = binary.Write(&buf, binary.LittleEndian, int32(1))
	buf.Write([]byte("v"))
	// missing expire int64

	err := Load(&buf, func(Item) {})
	if err == nil {
		t.Fatal("expected expire read error")
	}
}

func TestSnapshot_LoadValueLenReadError(t *testing.T) {
	var buf bytes.Buffer

	// keyLen = 1
	_ = binary.Write(&buf, binary.LittleEndian, int32(1))
	buf.Write([]byte("k"))

	// INTENTIONALLY truncate before valLen (needs 4 bytes)
	// so binary.Read(&valLen) fails

	err := Load(&buf, func(Item) {})
	if err == nil {
		t.Fatal("expected error while reading valLen, got nil")
	}
}
