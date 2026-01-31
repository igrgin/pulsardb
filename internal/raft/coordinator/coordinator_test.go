package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestCoordinator_Propose_ShuttingDown(t *testing.T) {
	c := &Coordinator{}
	c.shuttingDown.Store(true)

	if err := c.Propose(context.Background(), []byte("x")); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
}

func TestCoordinator_Propose_NoLeader(t *testing.T) {
	c := &Coordinator{
		node: &mockNode{
			id:     1,
			status: etcdraft.Status{BasicStatus: etcdraft.BasicStatus{SoftState: etcdraft.SoftState{Lead: 0}}},
			wal:    &mockWAL{},
		},
	}

	if err := c.Propose(context.Background(), []byte("x")); !errors.Is(err, ErrNoLeader) {
		t.Fatalf("expected ErrNoLeader, got %v", err)
	}
}

func TestCoordinator_Propose_ForwardsToNode(t *testing.T) {
	called := false

	n := &mockNode{
		id:     1,
		status: etcdraft.Status{BasicStatus: etcdraft.BasicStatus{SoftState: etcdraft.SoftState{Lead: 2}}},
		wal:    &mockWAL{},
		ProposeFn: func(ctx context.Context, d []byte) error {
			called = true
			if string(d) != "abc" {
				t.Fatalf("unexpected data: %q", string(d))
			}
			return nil
		},
	}

	c := &Coordinator{node: n}

	if err := c.Propose(context.Background(), []byte("abc")); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !called {
		t.Fatalf("expected propose called")
	}
}

func TestCoordinator_ReadIndex_ShuttingDown(t *testing.T) {
	c := &Coordinator{}
	c.shuttingDown.Store(true)

	_, err := c.ReadIndex(context.Background())
	if !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
}

func TestCoordinator_GetPeerAddr_ReturnsClientAddr(t *testing.T) {
	tr := &mockTransport{}
	tr.AddPeer(2, "raft:1", "client:1")

	c := &Coordinator{transport: tr}
	got := c.GetPeerAddr(2)
	if got != "client:1" {
		t.Fatalf("expected client:1, got %q", got)
	}
}

func TestCoordinator_Step_RoundTripWithoutRunningMainLoop(t *testing.T) {
	n := &mockNode{
		id:  1,
		wal: &mockWAL{},
		StepFn: func(ctx context.Context, msg raftpb.Message) error {
			if msg.Type != raftpb.MsgHeartbeat {
				t.Fatalf("unexpected msg type: %v", msg.Type)
			}
			return nil
		},
	}

	c := &Coordinator{
		node:      n,
		stepInbox: make(chan StepRequest, 1),
	}

	go func() {
		req := <-c.stepInbox
		req.Resp <- n.Step(req.Ctx, req.Msg)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := c.Step(ctx, raftpb.Message{Type: raftpb.MsgHeartbeat})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}
