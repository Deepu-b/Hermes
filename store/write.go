package store

import (
	"errors"
	"time"
)

/*
Errors returned by write operations to signal
explicit write semantics violations.
*/
var (
	ErrKeyExists      = errors.New("key already exists")
	ErrKeyNotFound    = errors.New("key not found")
	ErrInvalidPutMode = errors.New("invalid put mode")
)

/*
PutMode defines the write semantics applied when
writing a key-value pair to the store.
*/
type PutMode int

const (
	PutOverwrite PutMode = iota // always write
	PutIfAbsent                 // write only if key does not exist
	PutUpdate                   // write only if key exists
)

/*
DataStore is the public interface exposed to consumers.
It defines the minimal contract for interacting with the store.
*/
type DataStore interface {
	// Write stores a value for a key using the specified write semantics.
	Write(key string, value Entry, mode PutMode) error

	// Read returns the value for a key if it exists and is not expired.
	Read(key string) (Entry, bool)

	// Expire sets an absolute expiration time (in Unix milliseconds) for a key.
	// Returns false if the key does not exist or is already expired.
	Expire(key string, unixTimestampMilli int64) bool
}

/*
writeContext is an internal capability interface used by write strategies.
It intentionally exposes only minimal read/write primitives to avoid
leaking the underlying store implementation.
*/
type writeContext interface {
	get(key string) (Entry, bool)
	set(key string, value Entry)
	remove(key string)
}

/*
PutFunc represents a write strategy implementing specific write semantics.
*/
type PutFunc func(wctx writeContext, key string, value Entry) error

var putFactories = map[PutMode]PutFunc{
	PutOverwrite: overWriteStrategy,
	PutIfAbsent:  absentStrategy,
	PutUpdate:    updateStrategy,
}

func overWriteStrategy(wctx writeContext, key string, value Entry) error {
	wctx.set(key, value)
	return nil
}

func absentStrategy(wctx writeContext, key string, value Entry) error {
	_, ok := wctx.get(key)
	if ok {
		return ErrKeyExists
	}

	wctx.set(key, value)
	return nil
}

func updateStrategy(wctx writeContext, key string, value Entry) error {
	_, ok := wctx.get(key)
	if !ok {
		return ErrKeyNotFound
	}

	wctx.set(key, value)
	return nil
}

/*
Entry represents a single value stored in memory along with expiry.
Additional metadata (versioning, etc.) will be added later.
ExpiresAtUnix store expiration time as Unix milli-seconds; value of 0
means no expiration
*/
type Entry struct {
	Value           []byte
	ExpiresAtMillis int64 // 0 means no expiration
}

func GetUnixTimestamp(t time.Time) int64 {
	return t.UnixMilli()
}
