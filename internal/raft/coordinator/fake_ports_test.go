package coordinator

import (
	"context"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"pulsardb/internal/raft/ports"
)

type mockWAL struct {
	SaveReadyCalled bool
	SaveReadyErr    error

	SaveConfStateCalled bool
	SaveConfStateErr    error

	CreateSnapshotCalled bool
	CreateSnapshotErr    error

	SaveSnapshotCalled bool
	SaveSnapshotErr    error

	CompactCalled bool
	CompactErr    error

	SnapIndex uint64
	SnapData  []byte

	CompactArg uint64
}

func (w *mockWAL) SaveReady(rd etcdraft.Ready) error {
	w.SaveReadyCalled = true
	return w.SaveReadyErr
}

func (w *mockWAL) SaveConfState(cs raftpb.ConfState) error {
	w.SaveConfStateCalled = true
	return w.SaveConfStateErr
}

func (w *mockWAL) CreateSnapshot(index uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	w.CreateSnapshotCalled = true
	if w.CreateSnapshotErr != nil {
		return raftpb.Snapshot{}, w.CreateSnapshotErr
	}
	return raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{Index: index, Term: 7},
		Data:     data,
	}, nil
}

func (w *mockWAL) SaveSnapshot(snap raftpb.Snapshot) error {
	w.SaveSnapshotCalled = true
	return w.SaveSnapshotErr
}

func (w *mockWAL) Compact(index uint64) error {
	w.CompactCalled = true
	w.CompactArg = index
	return w.CompactErr
}

func (w *mockWAL) SnapshotIndex() uint64 { return w.SnapIndex }
func (w *mockWAL) SnapshotData() []byte  { return w.SnapData }
func (w *mockWAL) EntriesAfter(uint64) ([]raftpb.Entry, error) {
	return nil, nil
}

type mockNode struct {
	id     uint64
	status etcdraft.Status
	wal    ports.WALStorage

	ProposeFn     func(context.Context, []byte) error
	ReadIndexFn   func(context.Context, []byte) error
	ProposeConfFn func(context.Context, raftpb.ConfChange) error
	StepFn        func(context.Context, raftpb.Message) error
	ApplyConfFn   func(raftpb.ConfChange) *raftpb.ConfState
	TickFn        func()
	AdvanceFn     func()
	StopFn        func()

	confState raftpb.ConfState
	isJoining bool

	AdvanceCalled bool
}

func (n *mockNode) Propose(ctx context.Context, data []byte) error {
	if n.ProposeFn != nil {
		return n.ProposeFn(ctx, data)
	}
	return nil
}

func (n *mockNode) ReadIndex(ctx context.Context, rctx []byte) error {
	if n.ReadIndexFn != nil {
		return n.ReadIndexFn(ctx, rctx)
	}
	return nil
}

func (n *mockNode) ProposeConfChange(ctx context.Context, cc raftpb.ConfChange) error {
	if n.ProposeConfFn != nil {
		return n.ProposeConfFn(ctx, cc)
	}
	return nil
}

func (n *mockNode) Status() etcdraft.Status { return n.status }

func (n *mockNode) Tick() {
	if n.TickFn != nil {
		n.TickFn()
	}
}

func (n *mockNode) Ready() <-chan etcdraft.Ready {
	ch := make(chan etcdraft.Ready)
	close(ch)
	return ch
}

func (n *mockNode) Step(ctx context.Context, msg raftpb.Message) error {
	if n.StepFn != nil {
		return n.StepFn(ctx, msg)
	}
	return nil
}

func (n *mockNode) Advance() {
	n.AdvanceCalled = true
	if n.AdvanceFn != nil {
		n.AdvanceFn()
	}
}

func (n *mockNode) TransferLeadership(context.Context, uint64, uint64) {}

func (n *mockNode) ApplyConfChange(cc raftpb.ConfChange) *raftpb.ConfState {
	if n.ApplyConfFn != nil {
		return n.ApplyConfFn(cc)
	}
	return nil
}

func (n *mockNode) Stop() {
	if n.StopFn != nil {
		n.StopFn()
	}
}

func (n *mockNode) ID() uint64 { return n.id }

func (n *mockNode) ConfState() raftpb.ConfState { return n.confState }
func (n *mockNode) SetConfState(cs raftpb.ConfState) {
	n.confState = cs
}

func (n *mockNode) RestoreFromConfState() {}
func (n *mockNode) IsJoining() bool       { return n.isJoining }
func (n *mockNode) Storage() ports.WALStorage {
	return n.wal
}

type mockTransport struct {
	raftByID   map[uint64]string
	clientByID map[uint64]string

	sent []raftpb.Message
}

func (t *mockTransport) AddPeer(nodeID uint64, raftAddr, clientAddr string) {
	if t.raftByID == nil {
		t.raftByID = map[uint64]string{}
	}
	if t.clientByID == nil {
		t.clientByID = map[uint64]string{}
	}
	t.raftByID[nodeID] = raftAddr
	t.clientByID[nodeID] = clientAddr
}

func (t *mockTransport) GetPeerAddrs(nodeID uint64) (raftAddr, clientAddr string) {
	return t.raftByID[nodeID], t.clientByID[nodeID]
}

func (t *mockTransport) AllPeers() (raftPeers, clientPeers map[uint64]string) {
	raftPeers = map[uint64]string{}
	clientPeers = map[uint64]string{}
	for k, v := range t.raftByID {
		raftPeers[k] = v
	}
	for k, v := range t.clientByID {
		clientPeers[k] = v
	}
	return raftPeers, clientPeers
}

func (t *mockTransport) Peers() map[uint64]string {
	out := map[uint64]string{}
	for k, v := range t.raftByID {
		out[k] = v
	}
	return out
}

func (t *mockTransport) InitPeerClient(uint64, string) error { return nil }
func (t *mockTransport) StartPeerSender(uint64, int)         {}
func (t *mockTransport) StopPeerSender(uint64)               {}
func (t *mockTransport) ClosePeerClient(uint64) error        { return nil }
func (t *mockTransport) RemovePeer(uint64)                   {}

func (t *mockTransport) SendMessages(msgs []raftpb.Message) { t.sent = append(t.sent, msgs...) }
func (t *mockTransport) DrainMessageQueues(time.Duration)   {}

func (t *mockTransport) GetLeaderClient(uint64) (ports.LeaderClient, bool) {
	return nil, false
}
