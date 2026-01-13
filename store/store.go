package store

import "time"

/*
store is the core in-memory key-value store.
It contains no concurrency control and must be accessed
by a single goroutine or protected by an external mechanism.
*/
type store struct {
	data map[string]Entry
}

/*
NewStore creates a non-concurrent store.
Callers are responsible for ensuring safe access.
*/
func NewStore() DataStore {
	return &store{
		data: make(map[string]Entry),
	}
}

/*
Read returns the value for a key if present and not expired.

Expired keys are removed lazily during reads to ensure
they are never observable.
*/
func (s *store) Read(key string) (Entry, bool) {
	val, ok := s.get(key)
	if !ok {
		return Entry{}, false
	}

	now := GetUnixTimestamp(time.Now())
	if val.ExpiresAtMillis != 0 && now >= val.ExpiresAtMillis {
		s.remove(key)
		return Entry{}, false
	}
	return val, true
}

/*
Write applies the specified write semantics to the store.
The write behavior is determined by the provided PutMode.
*/
func (s *store) Write(key string, value Entry, mode PutMode) error {
	strategy, ok := putFactories[mode]
	if !ok {
		return ErrInvalidPutMode
	}

	return strategy(s, key, value)
}

/*
Expire attaches a TTL to an existing key.

If the key is already expired, it is removed and the
operation fails.
*/
func (s *store) Expire(key string, unixTimestampMilli int64) bool{
	val, ok := s.get(key)
	if !ok {
		return false
	}

	now := GetUnixTimestamp(time.Now())
	if val.ExpiresAtMillis != 0 && now >= val.ExpiresAtMillis {
		s.remove(key)
		return false
	}

	val.ExpiresAtMillis = unixTimestampMilli
	s.set(key, val)
	return true
}

func (s *store) Close() error {
	return nil
}

/*
Iterate traverses all live entries in the store.

Expired entries are skipped to guarantee snapshots
never persist dead keys.

Early-exit is honored to support efficient snapshot streaming.
*/
func (s *store) Iterate(fn func(key string, value Entry) bool) {
	for k, v := range s.data {
		if v.ExpiresAtMillis > 0 && GetUnixTimestamp(time.Now()) >= v.ExpiresAtMillis {
			continue
		}

		if !fn(k, v) {
			break
		}
	}
}

/*
get retrieves the raw entry without applying expiration logic.
Intended for internal use only.
*/
func (s *store) get(key string) (Entry, bool) {
	val, ok := s.data[key]
	return val, ok
}

/*
set inserts or overwrites a value in the store.
*/
func (s *store) set(key string, value Entry) {
	s.data[key] = value
}

/*
remove deletes a key from the store.
*/
func (s *store) remove(key string) {
	delete(s.data, key)
}
