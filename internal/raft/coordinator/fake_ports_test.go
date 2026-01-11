package coordinator

import (
	"context"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"pulsardb/internal/raft/ports"
)

type fakeWAL struct {
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

func (w *fakeWAL) SaveReady(rd etcdraft.Ready) error {
	w.SaveReadyCalled = true
	return w.SaveReadyErr
}

func (w *fakeWAL) SaveConfState(cs raftpb.ConfState) error {
	w.SaveConfStateCalled = true
	return w.SaveConfStateErr
}

func (w *fakeWAL) CreateSnapshot(index uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	w.CreateSnapshotCalled = true
	if w.CreateSnapshotErr != nil {
		return raftpb.Snapshot{}, w.CreateSnapshotErr
	}
	return raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{Index: index, Term: 7},
		Data:     data,
	}, nil
}

func (w *fakeWAL) SaveSnapshot(snap raftpb.Snapshot) error {
	w.SaveSnapshotCalled = true
	return w.SaveSnapshotErr
}

func (w *fakeWAL) Compact(index uint64) error {
	w.CompactCalled = true
	w.CompactArg = index
	return w.CompactErr
}

func (w *fakeWAL) SnapshotIndex() uint64 { return w.SnapIndex }
func (w *fakeWAL) SnapshotData() []byte  { return w.SnapData }
func (w *fakeWAL) EntriesAfter(uint64) ([]raftpb.Entry, error) {
	return nil, nil
}

type fakeNode struct {
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

func (n *fakeNode) Propose(ctx context.Context, data []byte) error {
	if n.ProposeFn != nil {
		return n.ProposeFn(ctx, data)
	}
	return nil
}

func (n *fakeNode) ReadIndex(ctx context.Context, rctx []byte) error {
	if n.ReadIndexFn != nil {
		return n.ReadIndexFn(ctx, rctx)
	}
	return nil
}

func (n *fakeNode) ProposeConfChange(ctx context.Context, cc raftpb.ConfChange) error {
	if n.ProposeConfFn != nil {
		return n.ProposeConfFn(ctx, cc)
	}
	return nil
}

func (n *fakeNode) Status() etcdraft.Status { return n.status }

func (n *fakeNode) Tick() {
	if n.TickFn != nil {
		n.TickFn()
	}
}

func (n *fakeNode) Ready() <-chan etcdraft.Ready {
	ch := make(chan etcdraft.Ready)
	close(ch)
	return ch
}

func (n *fakeNode) Step(ctx context.Context, msg raftpb.Message) error {
	if n.StepFn != nil {
		return n.StepFn(ctx, msg)
	}
	return nil
}

func (n *fakeNode) Advance() {
	n.AdvanceCalled = true
	if n.AdvanceFn != nil {
		n.AdvanceFn()
	}
}

func (n *fakeNode) TransferLeadership(context.Context, uint64, uint64) {}

func (n *fakeNode) ApplyConfChange(cc raftpb.ConfChange) *raftpb.ConfState {
	if n.ApplyConfFn != nil {
		return n.ApplyConfFn(cc)
	}
	return nil
}

func (n *fakeNode) Stop() {
	if n.StopFn != nil {
		n.StopFn()
	}
}

func (n *fakeNode) ID() uint64 { return n.id }

func (n *fakeNode) ConfState() raftpb.ConfState { return n.confState }
func (n *fakeNode) SetConfState(cs raftpb.ConfState) {
	n.confState = cs
}

func (n *fakeNode) RestoreFromConfState() {}
func (n *fakeNode) IsJoining() bool       { return n.isJoining }
func (n *fakeNode) Storage() ports.WALStorage {
	return n.wal
}

type fakeTransport struct {
	raftByID   map[uint64]string
	clientByID map[uint64]string

	sent []raftpb.Message
}

func (t *fakeTransport) AddPeer(nodeID uint64, raftAddr, clientAddr string) {
	if t.raftByID == nil {
		t.raftByID = map[uint64]string{}
	}
	if t.clientByID == nil {
		t.clientByID = map[uint64]string{}
	}
	t.raftByID[nodeID] = raftAddr
	t.clientByID[nodeID] = clientAddr
}

func (t *fakeTransport) GetPeerAddrs(nodeID uint64) (raftAddr, clientAddr string) {
	return t.raftByID[nodeID], t.clientByID[nodeID]
}

func (t *fakeTransport) AllPeers() (raftPeers, clientPeers map[uint64]string) {
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

func (t *fakeTransport) Peers() map[uint64]string {
	out := map[uint64]string{}
	for k, v := range t.raftByID {
		out[k] = v
	}
	return out
}

func (t *fakeTransport) InitPeerClient(uint64, string) error { return nil }
func (t *fakeTransport) StartPeerSender(uint64, int)         {}
func (t *fakeTransport) StopPeerSender(uint64)               {}
func (t *fakeTransport) ClosePeerClient(uint64) error        { return nil }
func (t *fakeTransport) RemovePeer(uint64)                   {}

func (t *fakeTransport) SendMessages(msgs []raftpb.Message) { t.sent = append(t.sent, msgs...) }
func (t *fakeTransport) DrainMessageQueues(time.Duration)   {}

func (t *fakeTransport) GetLeaderClient(uint64) (ports.LeaderClient, bool) {
	return nil, false
}
