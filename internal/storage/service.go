package storage

import (
	"encoding/json"
)

// Service provides thread-safe key-value storage.
type Service struct {
	kv *Store
}

// NewStorageService creates a new storage service.
func NewStorageService() *Service {
	return &Service{
		kv: NewStore(),
	}
}

// Get retrieves a value by key.
func (s *Service) Get(key string) (any, bool) {
	val, ok := s.kv.Get(key)
	return val, ok
}

// Set stores a key-value pair.
func (s *Service) Set(key string, value any) {
	s.kv.Set(key, value)
}

// Delete removes a key.
func (s *Service) Delete(key string) {
	s.kv.Delete(key)
}

// GetSnapshot returns a JSON-encoded snapshot of all kv.
func (s *Service) GetSnapshot() ([]byte, error) {

	// Convert to a serializable format
	snapshot := make(map[string]snapshotValue)
	for k, v := range s.kv.GetData() {
		snapshot[k] = toSnapshotValue(v)
	}

	return json.Marshal(snapshot)
}

// RestoreFromSnapshot restores state from a JSON-encoded snapshot.
func (s *Service) RestoreFromSnapshot(data []byte) error {

	var snapshot map[string]snapshotValue
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	s.kv.data = make(map[string]any)
	for k, v := range snapshot {
		s.kv.data[k] = fromSnapshotValue(v)
	}

	return nil
}

// Len returns the number of keys.
func (s *Service) Len() int {
	return s.kv.len()
}

// snapshotValue is a JSON-serializable wrapper for typed values.
type snapshotValue struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

func toSnapshotValue(v any) snapshotValue {
	switch val := v.(type) {
	case string:
		return snapshotValue{Type: "string", Value: val}
	case int64:
		return snapshotValue{Type: "int64", Value: val}
	case float64:
		return snapshotValue{Type: "float64", Value: val}
	case bool:
		return snapshotValue{Type: "bool", Value: val}
	case []byte:
		return snapshotValue{Type: "bytes", Value: val}
	default:
		return snapshotValue{Type: "unknown", Value: val}
	}
}

func fromSnapshotValue(sv snapshotValue) any {
	switch sv.Type {
	case "string":
		if s, ok := sv.Value.(string); ok {
			return s
		}
	case "int64":
		// JSON unmarshal numbers as int64
		if f, ok := sv.Value.(int64); ok {
			return f
		}
	case "float64":
		if f, ok := sv.Value.(float64); ok {
			return f
		}
	case "bool":
		if b, ok := sv.Value.(bool); ok {
			return b
		}
	case "bytes":
		// JSON unmarshal []byte as base64 string
		if s, ok := sv.Value.(string); ok {
			return []byte(s)
		}
	}
	return sv.Value
}
