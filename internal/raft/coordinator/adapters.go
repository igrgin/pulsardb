package coordinator

import (
	"context"
	"time"

	"pulsardb/internal/raft"
	"pulsardb/internal/raft/ports"
	rafttransportpb "pulsardb/internal/transport/gen/raft"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type RaftNodeAdapter struct {
	node    *raft.Node
	storage *WALStorageAdapter
}

func NewRaftNodeAdapter(node *raft.Node) *RaftNodeAdapter {
	return &RaftNodeAdapter{
		node:    node,
		storage: &WALStorageAdapter{storage: node.Storage()},
	}
}

func (a *RaftNodeAdapter) Propose(ctx context.Context, data []byte) error {
	return a.node.Propose(ctx, data)
}

func (a *RaftNodeAdapter) ReadIndex(ctx context.Context, rctx []byte) error {
	return a.node.ReadIndex(ctx, rctx)
}

func (a *RaftNodeAdapter) ProposeConfChange(ctx context.Context, cc raftpb.ConfChange) error {
	return a.node.RaftNode().ProposeConfChange(ctx, cc)
}

func (a *RaftNodeAdapter) Status() etcdraft.Status {
	return a.node.Status()
}

func (a *RaftNodeAdapter) Tick() {
	a.node.RaftNode().Tick()
}

func (a *RaftNodeAdapter) Ready() <-chan etcdraft.Ready {
	return a.node.RaftNode().Ready()
}

func (a *RaftNodeAdapter) Step(ctx context.Context, msg raftpb.Message) error {
	return a.node.RaftNode().Step(ctx, msg)
}

func (a *RaftNodeAdapter) Advance() {
	a.node.RaftNode().Advance()
}

func (a *RaftNodeAdapter) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	a.node.RaftNode().TransferLeadership(ctx, lead, transferee)
}

func (a *RaftNodeAdapter) ApplyConfChange(cc raftpb.ConfChange) *raftpb.ConfState {
	return a.node.ApplyConfChange(cc)
}

func (a *RaftNodeAdapter) Stop() {
	a.node.Stop()
}

func (a *RaftNodeAdapter) ID() uint64 {
	return a.node.Id
}

func (a *RaftNodeAdapter) ConfState() raftpb.ConfState {
	return a.node.ConfState()
}

func (a *RaftNodeAdapter) SetConfState(cs raftpb.ConfState) {
	a.node.SetConfState(cs)
}

func (a *RaftNodeAdapter) RestoreFromConfState() {
	a.node.RestoreFromConfState()
}

func (a *RaftNodeAdapter) IsJoining() bool {
	return a.node.IsJoining
}

func (a *RaftNodeAdapter) Storage() ports.WALStorage {
	return a.storage
}

type TransportAdapter struct {
	node *raft.Node
}

func NewTransportAdapter(node *raft.Node) *TransportAdapter {
	return &TransportAdapter{node: node}
}

func (a *TransportAdapter) AddPeer(nodeID uint64, raftAddr, clientAddr string) {
	a.node.AddPeer(nodeID, raftAddr, clientAddr)
}

func (a *TransportAdapter) GetPeerAddrs(nodeID uint64) (raftAddr, clientAddr string) {
	return a.node.GetPeerAddrs(nodeID)
}

func (a *TransportAdapter) AllPeers() (raftPeers, clientPeers map[uint64]string) {
	return a.node.AllPeers()
}

func (a *TransportAdapter) Peers() map[uint64]string {
	return a.node.Peers()
}

func (a *TransportAdapter) InitPeerClient(nodeID uint64, raftAddr string) error {
	return a.node.InitPeerClient(nodeID, raftAddr)
}

func (a *TransportAdapter) StartPeerSender(nodeID uint64, queueSize int) {
	a.node.StartPeerSender(nodeID, queueSize)
}

func (a *TransportAdapter) StopPeerSender(nodeID uint64) {
	a.node.StopPeerSender(nodeID)
}

func (a *TransportAdapter) ClosePeerClient(nodeID uint64) error {
	return a.node.ClosePeerClient(nodeID)
}

func (a *TransportAdapter) RemovePeer(nodeID uint64) {
	a.node.RemovePeer(nodeID)
}

func (a *TransportAdapter) SendMessages(msgs []raftpb.Message) {
	a.node.SendMessages(msgs)
}

func (a *TransportAdapter) DrainMessageQueues(timeout time.Duration) {
	a.node.DrainQueues(timeout)
}

func (a *TransportAdapter) GetLeaderClient(nodeID uint64) (ports.LeaderClient, bool) {
	client, ok := a.node.GetLeaderClient(nodeID)
	if !ok {
		return nil, false
	}
	return &LeaderClientAdapter{client: client}, true
}

type WALStorageAdapter struct {
	storage *raft.Storage
}

func (s *WALStorageAdapter) SaveReady(rd etcdraft.Ready) error {
	return s.storage.SaveReady(rd)
}

func (s *WALStorageAdapter) SaveConfState(cs raftpb.ConfState) error {
	return s.storage.SaveConfState(cs)
}

func (s *WALStorageAdapter) CreateSnapshot(index uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	return s.storage.CreateSnapshot(index, cs, data)
}

func (s *WALStorageAdapter) SaveSnapshot(snap raftpb.Snapshot) error {
	return s.storage.SaveSnapshot(snap)
}

func (s *WALStorageAdapter) Compact(index uint64) error {
	return s.storage.Compact(index)
}

func (s *WALStorageAdapter) SnapshotIndex() uint64 {
	return s.storage.SnapshotIndex()
}

func (s *WALStorageAdapter) SnapshotData() []byte {
	return s.storage.SnapshotData()
}

func (s *WALStorageAdapter) EntriesAfter(index uint64) ([]raftpb.Entry, error) {
	return s.storage.EntriesAfter(index)
}

type LeaderClientAdapter struct {
	client raft.Client
}

func (c *LeaderClientAdapter) GetReadIndex(ctx context.Context, fromNode uint64) (uint64, error) {
	resp, err := c.client.GetReadIndex(ctx, &rafttransportpb.GetReadIndexRequest{FromNode: fromNode})
	if err != nil {
		return 0, err
	}
	return resp.ReadIndex, nil
}

func (c *LeaderClientAdapter) RequestJoinCluster(ctx context.Context, nodeID uint64, raftAddr, clientAddr string) (accepted bool, message string, err error) {
	resp, err := c.client.RequestJoinCluster(ctx, &rafttransportpb.JoinRequest{
		NodeId:     nodeID,
		RaftAddr:   raftAddr,
		ClientAddr: clientAddr,
	})
	if err != nil {
		return false, "", err
	}
	return resp.Accepted, resp.Message, nil
}
