package storage

import "log/slog"

type Service struct {
	store *Store
}

func NewStorageService() *Service {
	return &Service{
		store: NewStore(),
	}
}

func (s *Service) Set(key string, value any) {
	slog.Debug("storage set", "key", key)
	s.store.Set(key, value)
}

func (s *Service) Get(key string) (any, bool) {
	slog.Debug("storage get", "key", key)
	return s.store.Get(key)
}

func (s *Service) Delete(key string) {
	slog.Debug("storage delete", "key", key)
	s.store.Delete(key)
}

func (s *Service) DescribeType(key string) string {
	t := s.store.DescribeType(key)
	slog.Debug("storage describe", "key", key, "type", t)
	return t
}
