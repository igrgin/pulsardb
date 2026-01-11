package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"pulsardb/internal/raft/ops"

	etcdraft "go.etcd.io/raft/v3"
)

func TestCoordinator_doReadIndex_NoLeader(t *testing.T) {

	n := &fakeNode{
		id:     1,
		status: etcdraft.Status{BasicStatus: etcdraft.BasicStatus{SoftState: etcdraft.SoftState{Lead: 0, RaftState: etcdraft.StateLeader}}},
		wal:    &fakeWAL{},
	}
	c := &Coordinator{
		node:        n,
		idGen:       ops.NewRequestIDGenerator(),
		readWaiters: map[string]*readWaiter{},
	}

	_, err := c.doReadIndex(context.Background())
	if !errors.Is(err, ErrNoLeader) {
		t.Fatalf("expected ErrNoLeader, got %v", err)
	}
}

func TestCoordinator_doReadIndex_LeaseBasedRead_NotLeader(t *testing.T) {

	n := &fakeNode{
		id:     1,
		status: etcdraft.Status{BasicStatus: etcdraft.BasicStatus{SoftState: etcdraft.SoftState{Lead: 2, RaftState: etcdraft.StateFollower}}},
		wal:    &fakeWAL{},
	}
	c := &Coordinator{
		node:           n,
		leaseBasedRead: true,
		idGen:          ops.NewRequestIDGenerator(),
		readWaiters:    map[string]*readWaiter{},
	}

	_, err := c.doReadIndex(context.Background())
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestCoordinator_doReadIndex_ReadIndexCallError(t *testing.T) {
	sentinel := errors.New("ri")

	n := &fakeNode{
		id:     1,
		status: etcdraft.Status{BasicStatus: etcdraft.BasicStatus{SoftState: etcdraft.SoftState{Lead: 2, RaftState: etcdraft.StateLeader}}},
		wal:    &fakeWAL{},
		ReadIndexFn: func(ctx context.Context, rctx []byte) error {
			return sentinel
		},
	}

	c := &Coordinator{
		node:           n,
		leaseBasedRead: true,
		idGen:          ops.NewRequestIDGenerator(),
		readWaiters:    map[string]*readWaiter{},
	}

	_, err := c.doReadIndex(context.Background())
	if err == nil || !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped sentinel error, got %v", err)
	}
}

func TestCoordinator_doReadIndex_SuccessViaHandleReadStates(t *testing.T) {
	n := &fakeNode{
		id:          1,
		status:      etcdraft.Status{BasicStatus: etcdraft.BasicStatus{SoftState: etcdraft.SoftState{Lead: 2, RaftState: etcdraft.StateLeader}}},
		wal:         &fakeWAL{},
		ReadIndexFn: func(ctx context.Context, rctx []byte) error { return nil },
	}

	c := &Coordinator{
		node:           n,
		leaseBasedRead: true,
		idGen:          ops.NewRequestIDGenerator(),
		readWaiters:    map[string]*readWaiter{},
	}

	c.SetLastApplied(100)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan struct{})
	var got uint64
	var gotErr error

	go func() {
		defer close(done)
		got, gotErr = c.doReadIndex(ctx)
	}()

	deadline := time.Now().Add(time.Second)
	var key string
	for time.Now().Before(deadline) {
		c.readMu.Lock()
		for k := range c.readWaiters {
			key = k
			break
		}
		c.readMu.Unlock()
		if key != "" {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	if key == "" {
		t.Fatalf("waiter was not registered")
	}

	c.handleReadStates([]etcdraft.ReadState{
		{RequestCtx: []byte(key), Index: 42},
	})

	<-done

	if gotErr != nil {
		t.Fatalf("unexpected err: %v", gotErr)
	}
	if got != 42 {
		t.Fatalf("expected idx=42, got %d", got)
	}
}
