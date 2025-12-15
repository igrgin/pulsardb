package storage

import "sync"

type Store struct {
	data map[string]any
	mu   sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]any),
	}
}

func (s *Store) getData() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data
}

func (s *Store) len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *Store) Set(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *Store) Get(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}
