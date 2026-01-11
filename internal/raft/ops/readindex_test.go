package ops

import (
	"testing"
)

func TestRequestIDGenerator(t *testing.T) {
	gen := NewRequestIDGenerator()

	id1 := gen.Next()
	id2 := gen.Next()
	id3 := gen.Next()

	if id1 != 1 {
		t.Errorf("First ID = %d, want 1", id1)
	}
	if id2 != 2 {
		t.Errorf("Second ID = %d, want 2", id2)
	}
	if id3 != 3 {
		t.Errorf("Third ID = %d, want 3", id3)
	}
}

func TestEncodeDecodeReadIndexContext(t *testing.T) {
	tests := []uint64{1, 100, 12345, 999999}

	for _, reqID := range tests {
		encoded := EncodeReadIndexContext(reqID)
		decoded, err := DecodeReadIndexContext(encoded)

		if err != nil {
			t.Errorf("DecodeReadIndexContext(%q) error = %v", encoded, err)
			continue
		}

		if decoded != reqID {
			t.Errorf("Round trip requestID = %d, want %d", decoded, reqID)
		}
	}
}

func TestDecodeReadIndexContext_InvalidFormat(t *testing.T) {
	invalidContexts := [][]byte{
		[]byte("invalid"),
		[]byte("index-123"),
		[]byte(""),
	}

	for _, ctx := range invalidContexts {
		_, err := DecodeReadIndexContext(ctx)
		if err == nil {
			t.Errorf("DecodeReadIndexContext(%q) expected error, got nil", ctx)
		}
	}
}

func TestEncodeAppliedWaiterKey(t *testing.T) {
	key := EncodeAppliedWaiterKey(100, 5)

	expected := "applied-100-5"
	if key != expected {
		t.Errorf("EncodeAppliedWaiterKey(100, 5) = %q, want %q", key, expected)
	}
}

func TestEncodeAppliedWaiterKey_Uniqueness(t *testing.T) {
	key1 := EncodeAppliedWaiterKey(100, 1)
	key2 := EncodeAppliedWaiterKey(100, 2)
	key3 := EncodeAppliedWaiterKey(200, 1)

	if key1 == key2 {
		t.Error("Keys with different requestIDs should be different")
	}
	if key1 == key3 {
		t.Error("Keys with different indexes should be different")
	}
}
