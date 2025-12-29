package raft

import (
	"context"
	"log/slog"
	"pulsardb/internal/configuration"
	rafttransportpb "pulsardb/internal/transport/gen/raft"
	"sync"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	Id          uint64
	raftNode    etcdraft.Node
	storage     *Storage
	raftPeers   map[uint64]string
	clientPeers map[uint64]string
	clients     map[uint64]Client
	confState   raftpb.ConfState
	mu          sync.RWMutex
	Timeout     uint64
	peerQueues  map[uint64]chan raftpb.Message
	peerQueueMu sync.RWMutex
	sendStopCh  chan struct{}
	sendWg      sync.WaitGroup
}

func NewNode(rc *configuration.RaftConfigurationProperties, localAddr string) (*Node, error) {
	slog.Info("creating raft node", "node_id", rc.NodeID, "addr", localAddr)

	cfg, err := newNodeConfig(rc, localAddr)
	if err != nil {
		return nil, err
	}

	n := &Node{
		Id:          rc.NodeID,
		storage:     cfg.storage,
		raftPeers:   rc.RaftPeers,
		clientPeers: rc.ClientPeers,
		clients:     make(map[uint64]Client),
		confState:   cfg.storage.ConfState(),
		Timeout:     rc.Timeout,
		raftNode:    cfg.raftNode,
	}

	if err := n.InitAllPeerClients(); err != nil {
		slog.Error("failed to init peer clients", "error", err)
		return nil, err
	}

	n.initSendPool(rc.SendQueueSize)

	slog.Info("raft node created",
		"id", rc.NodeID,
		"peers", len(rc.RaftPeers),
		"timeout", rc.Timeout,
	)
	return n, nil
}

func (n *Node) RawNode() *etcdraft.Node {
	return &n.raftNode
}

func (n *Node) Status() etcdraft.Status {
	return n.raftNode.Status()
}

func (n *Node) Storage() *Storage {
	return n.storage
}

func (n *Node) ConfState() raftpb.ConfState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.confState
}

func (n *Node) SetConfState(cs raftpb.ConfState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	slog.Debug("updating confState", "node_id", n.Id, "voters", cs.Voters, "learners", cs.Learners)
	n.confState = cs
}

func (n *Node) initSendPool(queueSize int) {
	n.sendStopCh = make(chan struct{})
	n.peerQueues = make(map[uint64]chan raftpb.Message)

	count := 0
	for peerID := range n.raftPeers {
		if peerID == n.Id {
			continue
		}
		n.startPeerSender(peerID, queueSize)
		count++
	}
	slog.Debug("send pool initialized", "node_id", n.Id, "peers", count, "queueSize", queueSize)
}

func (n *Node) startPeerSender(peerID uint64, queueSize int) {
	ch := make(chan raftpb.Message, queueSize)

	n.peerQueueMu.Lock()
	n.peerQueues[peerID] = ch
	n.peerQueueMu.Unlock()

	n.sendWg.Add(1)
	go func() {
		defer n.sendWg.Done()
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				n.sendMessage(msg)
			case <-n.sendStopCh:
				return
			}
		}
	}()
	slog.Debug("peer sender started", "node_id", n.Id, "peer_id", peerID)
}

func (n *Node) stopSendPool() {
	if n.sendStopCh == nil {
		return
	}
	slog.Debug("stopping send pool", "node_id", n.Id)
	close(n.sendStopCh)

	n.peerQueueMu.Lock()
	for _, ch := range n.peerQueues {
		close(ch)
	}
	n.peerQueues = nil
	n.peerQueueMu.Unlock()

	n.sendWg.Wait()
	slog.Debug("send pool stopped", "node_id", n.Id)
}

func (n *Node) stopPeerSender(peerID uint64) {
	n.peerQueueMu.Lock()
	ch, ok := n.peerQueues[peerID]
	if ok {
		delete(n.peerQueues, peerID)
	}
	n.peerQueueMu.Unlock()

	if ok && ch != nil {
		close(ch)
		slog.Debug("peer sender stopped", "node_id", n.Id, "peer_id", peerID)
	}
}

func (n *Node) Peers() map[uint64]string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	peersCopy := make(map[uint64]string, len(n.raftPeers))
	for k, v := range n.raftPeers {
		peersCopy[k] = v
	}
	return peersCopy
}

func (n *Node) GetTimeout() uint64 {
	return n.Timeout
}

func (n *Node) Propose(ctx context.Context, data []byte) error {
	slog.Debug("proposing data", "node_id", n.Id, "bytes", len(data))
	return n.raftNode.Propose(ctx, data)
}

func (n *Node) ReadIndex(ctx context.Context, rctx []byte) error {
	slog.Debug("requesting read index", "node_id", n.Id)
	return n.raftNode.ReadIndex(ctx, rctx)
}

func (n *Node) ApplyConfChange(cc raftpb.ConfChange) *raftpb.ConfState {
	slog.Debug("applying conf change", "node_id", n.Id, "type", cc.Type, "target", cc.NodeID)
	return n.raftNode.ApplyConfChange(cc)
}

func (n *Node) ReportUnreachable(id uint64) {
	slog.Debug("reporting peer unreachable", "node_id", n.Id, "peer_id", id)
	n.raftNode.ReportUnreachable(id)
}

func (n *Node) ReportSnapshot(id uint64, status etcdraft.SnapshotStatus) {
	slog.Debug("reporting snapshot status", "node_id", n.Id, "peer_id", id, "status", status)
	n.raftNode.ReportSnapshot(id, status)
}

func (n *Node) restoreFromConfState() {
	slog.Debug("restoring peer connections from confState",
		"node_id", n.Id,
		"voters", n.confState.Voters,
		"learners", n.confState.Learners,
	)

	if len(n.confState.Voters) == 0 {
		slog.Warn("confState empty, probably new node",
			"node_id", n.Id,
			"raftPeers", n.raftPeers,
		)
		return
	}

	for _, voterID := range n.confState.Voters {
		if voterID == n.Id {
			continue
		}

		addr, ok := n.raftPeers[voterID]
		if !ok {
			slog.Warn("no address for voter",
				"node_id", n.Id,
				"voter_id", voterID,
			)
			continue
		}

		n.mu.RLock()
		_, exists := n.clients[voterID]
		n.mu.RUnlock()

		if !exists {
			if err := n.initPeerClient(voterID, addr); err != nil {
				slog.Warn("failed to init peer client",
					"peer_id", voterID,
					"addr", addr,
					"error", err,
				)
			} else {
				slog.Debug("restored peer client",
					"node_id", n.Id,
					"peer_id", voterID,
				)
			}
		}
	}
}

func (n *Node) InitAllPeerClients() error {
	slog.Debug("initializing all peer clients", "node_id", n.Id, "peers", len(n.raftPeers))
	for id, raftAddr := range n.raftPeers {
		if id == n.Id {
			continue
		}

		if err := n.initPeerClient(id, raftAddr); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) initPeerClient(id uint64, raftAddr string) error {
	clientAddr := n.clientPeers[id]

	slog.Debug("initializing peer client", "peer_id", id, "raftAddr", raftAddr, "clientAddr", clientAddr)

	raftConn, err := grpc.NewClient(raftAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		slog.Warn("failed to create raft connection", "peer_id", id, "addr", raftAddr, "error", err)
		return err
	}

	clientConn, err := grpc.NewClient(clientAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		_ = raftConn.Close()
		slog.Warn("failed to create client connection", "peer_id", id, "addr", clientAddr, "error", err)
		return err
	}

	client := newCombinedClient(raftConn, clientConn)

	n.mu.Lock()
	n.clients[id] = client
	n.mu.Unlock()

	slog.Debug("peer client initialized", "peer_id", id, "raftAddr", raftAddr, "clientAddr", clientAddr)
	return nil
}

func (n *Node) sendMessages(msgs []raftpb.Message) {
	if len(msgs) == 0 {
		return
	}

	for _, msg := range msgs {
		if msg.To == 0 || msg.To == n.Id {
			continue
		}

		n.peerQueueMu.RLock()
		ch, ok := n.peerQueues[msg.To]
		n.peerQueueMu.RUnlock()

		if !ok {
			slog.Warn("no send queue for peer", "node_id", n.Id, "to", msg.To, "type", msg.Type)
			n.raftNode.ReportUnreachable(msg.To)
			continue
		}

		select {
		case ch <- msg:
			slog.Debug("message queued", "to", msg.To, "type", msg.Type)
		default:
			slog.Debug("queue full, dropping message", "to", msg.To, "type", msg.Type)
			n.raftNode.ReportUnreachable(msg.To)
		}
	}
}

func (n *Node) sendMessage(msg raftpb.Message) {
	n.mu.RLock()
	client, ok := n.clients[msg.To]
	n.mu.RUnlock()

	if !ok {
		slog.Warn("no client for peer",
			"node_id", n.Id,
			"to", msg.To,
			"type", msg.Type,
		)
		n.raftNode.ReportUnreachable(msg.To)
		return
	}

	data, err := msg.Marshal()
	if err != nil {
		slog.Error("failed to marshal raft message",
			"to", msg.To,
			"type", msg.Type,
			"error", err,
		)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n.Timeout)*time.Second)
	defer cancel()

	if _, err := client.SendRaftMessage(ctx, &rafttransportpb.RaftMessage{Data: data}); err != nil {
		slog.Debug("failed to send raft message",
			"to", msg.To,
			"type", msg.Type,
			"error", err,
		)
		n.raftNode.ReportUnreachable(msg.To)
	}
}

func (n *Node) GetLeaderClient(leaderID uint64) (Client, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	client, ok := n.clients[leaderID]
	return client, ok
}

func (n *Node) StopClients() {
	slog.Debug("stopping all peer clients", "node_id", n.Id)
	n.mu.Lock()
	clients := n.clients
	n.clients = make(map[uint64]Client)
	n.mu.Unlock()

	for id, c := range clients {
		if c == nil {
			continue
		}
		if err := c.Close(); err != nil {
			slog.Warn("failed to close peer client", "peer_id", id, "error", err)
		}
	}
	slog.Debug("all peer clients stopped", "node_id", n.Id)
}

func (n *Node) Stop() {
	slog.Info("stopping raft node", "id", n.Id)
	n.stopSendPool()
	n.StopClients()
	n.raftNode.Stop()
	slog.Info("raft node stopped", "id", n.Id)
}

func (n *Node) AddPeer(id uint64, raftAddr, clientAddr string) {
	n.mu.Lock()
	n.raftPeers[id] = raftAddr
	n.clientPeers[id] = clientAddr
	n.mu.Unlock()
	slog.Info("peer added", "node_id", n.Id, "peer_id", id, "raftAddr", raftAddr, "clientAddr", clientAddr)
}
