package store

import (
	"fmt"
	"pulsardb/internal/types"

	snapshotpb "pulsardb/internal/raft/gen"

	"google.golang.org/protobuf/proto"
)

type Service struct {
	kv *Store
}

func NewService() *Service {
	return &Service{
		kv: NewStore(),
	}
}

func (s *Service) Get(key string) (any, bool) {
	return s.kv.Get(key)
}

func (s *Service) Set(key string, value any) {
	s.kv.Set(key, value)
}

func (s *Service) Delete(key string) {
	s.kv.Delete(key)
}

func (s *Service) Snapshot() ([]byte, error) {
	s.kv.mu.RLock()
	defer s.kv.mu.RUnlock()

	snap := &snapshotpb.KVSnapshot{
		Entries: make([]*snapshotpb.KeyValue, 0, len(s.kv.data)),
	}

	for k, v := range s.kv.data {
		pbVal, err := types.ToProtoValue(v)
		if err != nil {
			return nil, fmt.Errorf("convert key %q: %w", k, err)
		}
		snap.Entries = append(snap.Entries, &snapshotpb.KeyValue{
			Key:   k,
			Value: pbVal,
		})
	}

	return proto.Marshal(snap)
}

func (s *Service) Restore(data []byte) error {
	var snap snapshotpb.KVSnapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	s.kv.mu.Lock()
	defer s.kv.mu.Unlock()

	s.kv.data = make(map[string]any, len(snap.Entries))
	for _, entry := range snap.Entries {
		s.kv.data[entry.Key] = types.FromProtoValue(entry.Value)
	}

	return nil
}

func (s *Service) Len() int {
	s.kv.mu.RLock()
	defer s.kv.mu.RUnlock()
	return len(s.kv.data)
}
