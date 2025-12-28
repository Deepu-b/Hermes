package store

type store struct {
	data map[string]Entry
}

func NewStore() DataStore {
	return &store{
		data: make(map[string]Entry),
	}
}

func (s *store) Read(key string) (Entry, bool) {
	return s.get(key)
}

func (s *store) Write(key string, value Entry, mode PutMode) error {
	strategy, ok := putFactories[mode]
	if !ok {
		return ErrInvalidPutMode
	}

	return strategy(s, key, value)
}

func (s *store) get(key string) (Entry, bool) {
	val, ok := s.data[key]
	return val, ok
}

func (s *store) set(key string, value Entry) {
	s.data[key] = value
}