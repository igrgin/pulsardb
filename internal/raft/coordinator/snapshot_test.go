package coordinator

import (
	"errors"
	"testing"

	etcdraft "go.etcd.io/raft/v3"
)

type mockStoreForSnapshot struct {
	snapData []byte
	snapErr  error
	lenVal   int
}

func (s *mockStoreForSnapshot) Set(key string, value any)  {}
func (s *mockStoreForSnapshot) Get(key string) (any, bool) { return nil, false }
func (s *mockStoreForSnapshot) Delete(key string)          {}
func (s *mockStoreForSnapshot) Snapshot() ([]byte, error)  { return s.snapData, s.snapErr }
func (s *mockStoreForSnapshot) Restore([]byte) error       { return nil }
func (s *mockStoreForSnapshot) Len() int                   { return s.lenVal }

func TestCoordinator_maybeTriggerSnapshot_SnapCountZero_NoOp(t *testing.T) {
	c := &Coordinator{snapCount: 0}
	if err := c.maybeTriggerSnapshot(100); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestCoordinator_maybeTriggerSnapshot_NotEnoughDistance_NoOp(t *testing.T) {
	w := &mockWAL{SnapIndex: 90}
	n := &mockNode{id: 1, wal: w}

	c := &Coordinator{
		node:      n,
		snapCount: 20,
		store:     &mockStoreForSnapshot{snapData: []byte("x"), lenVal: 1},
	}

	if err := c.maybeTriggerSnapshot(100); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if w.CreateSnapshotCalled {
		t.Fatalf("expected no snapshot creation")
	}
}

func TestCoordinator_triggerSnapshot_AppliedIndexZero_NoOp(t *testing.T) {
	c := &Coordinator{}
	if err := c.triggerSnapshot(0, nil); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestCoordinator_triggerSnapshot_StoreSnapshotError(t *testing.T) {

	w := &mockWAL{}
	n := &mockNode{id: 1, wal: w}

	sentinel := errors.New("snap")
	c := &Coordinator{
		node:  n,
		store: &mockStoreForSnapshot{snapErr: sentinel},
	}

	err := c.triggerSnapshot(10, nil)
	if err == nil || !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped sentinel, got %v", err)
	}
}

func TestCoordinator_triggerSnapshot_EmptyData_NoOp(t *testing.T) {
	w := &mockWAL{}
	n := &mockNode{id: 1, wal: w}

	c := &Coordinator{
		node:  n,
		store: &mockStoreForSnapshot{snapData: nil, lenVal: 0},
	}

	if err := c.triggerSnapshot(10, nil); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if w.CreateSnapshotCalled || w.SaveSnapshotCalled || w.CompactCalled {
		t.Fatalf("expected no WAL calls when snapshot data empty")
	}
}

func TestCoordinator_triggerSnapshot_SnapOutOfDate_IsIgnored(t *testing.T) {
	w := &mockWAL{CreateSnapshotErr: etcdraft.ErrSnapOutOfDate}
	n := &mockNode{id: 1, wal: w}

	c := &Coordinator{
		node:      n,
		snapCount: 5,
		store:     &mockStoreForSnapshot{snapData: []byte("data"), lenVal: 1},
	}

	if err := c.triggerSnapshot(10, nil); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if !w.CreateSnapshotCalled {
		t.Fatalf("expected CreateSnapshot called")
	}
	if w.SaveSnapshotCalled {
		t.Fatalf("expected SaveSnapshot NOT called on out-of-date")
	}
}

func TestCoordinator_triggerSnapshot_Success_CompactsUsingSnapCount(t *testing.T) {
	w := &mockWAL{}
	n := &mockNode{id: 1, wal: w}

	c := &Coordinator{
		node:      n,
		snapCount: 5,
		store:     &mockStoreForSnapshot{snapData: []byte("data"), lenVal: 1},
	}

	if err := c.triggerSnapshot(10, nil); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !w.CreateSnapshotCalled || !w.SaveSnapshotCalled || !w.CompactCalled {
		t.Fatalf("expected WAL create/save/compact all called")
	}
	if w.CompactArg != 5 {
		t.Fatalf("expected compact index 5, got %d", w.CompactArg)
	}
}
