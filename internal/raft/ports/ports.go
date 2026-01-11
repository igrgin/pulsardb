package ports

import (
	"context"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type ClusterMembership interface {
	ProposeAddNode(ctx context.Context, nodeID uint64, raftAddr, clientAddr string) error
	ProposeAddLearnerNode(ctx context.Context, nodeID uint64, raftAddr, clientAddr string) error
	ProposeRemoveNode(ctx context.Context, nodeID uint64) error
}

type WALStorage interface {
	SaveReady(rd etcdraft.Ready) error
	SaveConfState(cs raftpb.ConfState) error
	CreateSnapshot(index uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error)
	SaveSnapshot(snap raftpb.Snapshot) error
	Compact(index uint64) error
	SnapshotIndex() uint64
	SnapshotData() []byte
	EntriesAfter(index uint64) ([]raftpb.Entry, error)
}

type RaftNode interface {
	Propose(ctx context.Context, data []byte) error
	ReadIndex(ctx context.Context, rctx []byte) error
	ProposeConfChange(ctx context.Context, cc raftpb.ConfChange) error
	Status() etcdraft.Status
	Tick()
	Ready() <-chan etcdraft.Ready
	Step(ctx context.Context, msg raftpb.Message) error
	Advance()
	TransferLeadership(ctx context.Context, lead, transferee uint64)
	ApplyConfChange(cc raftpb.ConfChange) *raftpb.ConfState
	Stop()

	ID() uint64
	ConfState() raftpb.ConfState
	SetConfState(cs raftpb.ConfState)
	RestoreFromConfState()
	IsJoining() bool

	Storage() WALStorage
}

type Transport interface {
	AddPeer(nodeID uint64, raftAddr, clientAddr string)
	GetPeerAddrs(nodeID uint64) (raftAddr, clientAddr string)
	AllPeers() (raftPeers, clientPeers map[uint64]string)
	Peers() map[uint64]string
	InitPeerClient(nodeID uint64, raftAddr string) error
	StartPeerSender(nodeID uint64, queueSize int)
	StopPeerSender(nodeID uint64)
	ClosePeerClient(nodeID uint64) error
	RemovePeer(nodeID uint64)
	SendMessages(msgs []raftpb.Message)
	DrainMessageQueues(timeout time.Duration)
	GetLeaderClient(nodeID uint64) (LeaderClient, bool)
}

type LeaderClient interface {
	GetReadIndex(ctx context.Context, fromNode uint64) (uint64, error)
	RequestJoinCluster(ctx context.Context, nodeID uint64, raftAddr, clientAddr string) (accepted bool, message string, err error)
}
