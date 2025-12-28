package store

import "time"

type store struct {
	data map[string]Entry
}

func NewStore() DataStore {
	return &store{
		data: make(map[string]Entry),
	}
}

func (s *store) Read(key string) (Entry, bool) {
	val, ok := s.get(key)
	if !ok {
		return Entry{}, false
	}

	if !val.ExpiresAt.IsZero() && time.Now().After(val.ExpiresAt) {
		s.remove(key)
		return Entry{}, false
	}
	return val, true
}

func (s *store) Write(key string, value Entry, mode PutMode) error {
	strategy, ok := putFactories[mode]
	if !ok {
		return ErrInvalidPutMode
	}

	return strategy(s, key, value)
}

func (s *store) Expire(key string, ttl time.Duration) bool {
	val, ok := s.get(key)
	if !ok {
		return false
	}

	if !val.ExpiresAt.IsZero() && time.Now().After(val.ExpiresAt) {
		s.remove(key)
		return false
	}

	val.ExpiresAt = time.Now().Add(ttl)
	s.set(key, val)
	return true
}

func (s *store) get(key string) (Entry, bool) {
	val, ok := s.data[key]
	return val, ok
}

func (s *store) set(key string, value Entry) {
	s.data[key] = value
}

func (s *store) remove(key string) {
	delete(s.data, key)
}
