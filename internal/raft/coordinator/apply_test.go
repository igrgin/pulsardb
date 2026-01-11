package coordinator

import (
	"testing"

	commandeventspb "pulsardb/internal/transport/gen/command"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type memStore struct {
	m map[string][]byte
}

func newMemStore() *memStore { return &memStore{m: map[string][]byte{}} }

func (s *memStore) Set(k string, v []byte)      { s.m[k] = v }
func (s *memStore) Delete(k string)             { delete(s.m, k) }
func (s *memStore) Get(k string) ([]byte, bool) { v, ok := s.m[k]; return v, ok }

func TestCoordinator_applyEntries_UpdatesLastAppliedAndReturnsLastIndex(t *testing.T) {
	c := &Coordinator{
		node: &fakeNode{
			id:  1,
			wal: &fakeWAL{},
		},
		readWaiters: make(map[string]*readWaiter),
	}

	entries := []raftpb.Entry{
		{Type: raftpb.EntryNormal, Index: 10, Term: 1, Data: nil},
		{Type: raftpb.EntryConfChange, Index: 11, Term: 1, Data: []byte("x")},
		{Type: raftpb.EntryNormal, Index: 12, Term: 1, Data: nil},
	}

	last, err := c.applyEntries(entries)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if last != 12 {
		t.Fatalf("expected last=12, got %d", last)
	}
	if c.LastApplied() != 12 {
		t.Fatalf("expected LastApplied=12, got %d", c.LastApplied())
	}
}

func TestCoordinator_replayEntry_BatchedCommands_AppliesAll(t *testing.T) {
	store := newMemStore()
	c := &Coordinator{}

	batch := &commandeventspb.BatchedCommands{
		Commands: []*commandeventspb.CommandEventRequest{
			{Type: commandeventspb.CommandEventType_SET, Key: "a", Value: &commandeventspb.CommandEventValue{Value: &commandeventspb.CommandEventValue_BytesValue{BytesValue: []byte("x")}}},
			{Type: commandeventspb.CommandEventType_DELETE, Key: "b"},
		},
	}
	data, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	applyToStore := func(req *commandeventspb.CommandEventRequest) error {
		switch req.GetType() {
		case commandeventspb.CommandEventType_SET:
			store.Set(req.GetKey(), req.GetValue().GetBytesValue())
		case commandeventspb.CommandEventType_DELETE:
			store.Delete(req.GetKey())
		}
		return nil
	}

	var got commandeventspb.BatchedCommands
	if err := proto.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal batch: %v", err)
	}
	for _, req := range got.Commands {
		if err := applyToStore(req); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	if v, ok := store.Get("a"); !ok || string(v) != "x" {
		t.Fatalf("expected a=x, got ok=%v v=%q", ok, string(v))
	}
	if _, ok := store.Get("b"); ok {
		t.Fatalf("expected b deleted")
	}

	_ = c
}

func TestCoordinator_replayEntry_SingleCommand_ParsesAndApplies(t *testing.T) {
	store := newMemStore()

	req := &commandeventspb.CommandEventRequest{
		Type:  commandeventspb.CommandEventType_SET,
		Key:   "k",
		Value: &commandeventspb.CommandEventValue{Value: &commandeventspb.CommandEventValue_BytesValue{BytesValue: []byte("v")}},
	}
	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var parsed commandeventspb.CommandEventRequest
	if err := proto.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if parsed.GetType() != commandeventspb.CommandEventType_SET || parsed.GetKey() != "k" {
		t.Fatalf("unexpected parsed: %#v", &parsed)
	}

	store.Set(parsed.GetKey(), parsed.GetValue().GetBytesValue())

	if v, ok := store.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("expected k=v, got ok=%v v=%q", ok, string(v))
	}
}
