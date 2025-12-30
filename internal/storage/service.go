package storage

import (
	"fmt"
	"pulsardb/convert"
	"pulsardb/internal/metrics"

	snapshotpb "pulsardb/internal/raft/gen"

	"google.golang.org/protobuf/proto"
)

type Service struct {
	store *Store
}

func NewService() *Service {
	return &Service{store: NewStore()}
}

func (s *Service) Get(key string) (any, bool) {
	return s.store.Get(key)
}

func (s *Service) Set(key string, value any) {
	s.store.Set(key, value)
}

func (s *Service) Delete(key string) {
	s.store.Delete(key)
}

func (s *Service) Len() int {
	return s.store.Len()
}

func (s *Service) Snapshot() ([]byte, error) {
	s.store.mu.RLock()
	defer s.store.mu.RUnlock()

	data := s.store.Data()

	snap := &snapshotpb.KVSnapshot{
		Entries: make([]*snapshotpb.KeyValue, 0, len(data)),
	}

	for k, v := range data {
		pbVal, err := convert.ToSnapshotProto(v)
		if err != nil {
			return nil, fmt.Errorf("convert key %q: %w", k, err)
		}
		snap.Entries = append(snap.Entries, &snapshotpb.KeyValue{
			Key:   k,
			Value: pbVal,
		})
	}

	bytes, err := proto.Marshal(snap)
	if err != nil {
		return nil, err
	}

	metrics.StorageSnapshotSize.Set(float64(len(bytes)))
	return bytes, nil
}

func (s *Service) Restore(data []byte) error {
	var snap snapshotpb.KVSnapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	newData := make(map[string]any, len(snap.Entries))
	for _, entry := range snap.Entries {
		newData[entry.Key] = convert.FromSnapshotProto(entry.Value)
	}

	s.store.Replace(newData)
	return nil
}
