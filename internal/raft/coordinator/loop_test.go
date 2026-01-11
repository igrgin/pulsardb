package coordinator

import (
	"errors"
	"testing"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestCoordinator_processReady_SaveReadyErrorBubblesUp(t *testing.T) {
	w := &fakeWAL{SaveReadyErr: errors.New("wal")}
	n := &fakeNode{id: 1, wal: w}
	tr := &fakeTransport{}

	c := &Coordinator{
		node:        n,
		transport:   tr,
		readWaiters: map[string]*readWaiter{},
	}

	c.shuttingDown.Store(true)

	err := c.processReady(etcdraft.Ready{})
	if err == nil || err.Error() != "wal" {
		t.Fatalf("expected wal error, got %v", err)
	}
	if !w.SaveReadyCalled {
		t.Fatalf("expected SaveReady called")
	}
	if n.AdvanceCalled {
		t.Fatalf("expected Advance not called on SaveReady error")
	}
}

func TestCoordinator_processReady_SendsMessagesAndAdvances(t *testing.T) {

	w := &fakeWAL{}
	n := &fakeNode{id: 1, wal: w}
	tr := &fakeTransport{}

	c := &Coordinator{
		node:        n,
		transport:   tr,
		readWaiters: map[string]*readWaiter{},
	}

	c.shuttingDown.Store(true)

	msgs := []raftpb.Message{{Type: raftpb.MsgApp, From: 1, To: 2}}
	rd := etcdraft.Ready{Messages: msgs}

	err := c.processReady(rd)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(tr.sent) != 1 {
		t.Fatalf("expected 1 message sent, got %d", len(tr.sent))
	}
	if !n.AdvanceCalled {
		t.Fatalf("expected Advance called")
	}
}
