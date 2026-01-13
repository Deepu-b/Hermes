package store

import (
	"sync"
)

/*
lockedStore is a concurrency wrapper that protects the store
using a single global mutex.

All operations are serialized to ensure correctness under
concurrent access.
*/
type lockedStore struct {
	mu    sync.RWMutex
	store *store
}

/*
NewLockedStore creates a store protected by a global lock.
This serves as a simple and safe baseline concurrency model.
*/
func NewLockedStore() DataStore {
	return &lockedStore{
		store: &store{
			data: make(map[string]Entry),
		},
	}
}

/*
Read acquires the global lock before delegating to the store.

Reads take an exclusive lock because lazy expiration may
delete expired keys.
*/
func (s *lockedStore) Read(key string) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Read(key)
}

/*
Write acquires the global lock and applies write semantics
atomically.
*/
func (s *lockedStore) Write(key string, value Entry, mode PutMode) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Write(key, value, mode)
}

/*
Expire acquires the global lock and updates expiry metadata.
*/
func (s *lockedStore) Expire(key string, unixTimestampMilli int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Expire(key, unixTimestampMilli)
}

func (s *lockedStore) Close() error {
	return s.store.Close()
}

func (s *lockedStore) Iterate(fn func(key string, value Entry) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.store.Iterate(fn)
}