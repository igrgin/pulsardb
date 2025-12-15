package raft

import (
	"context"
	"log/slog"
	rafttransportpb "pulsardb/internal/transport/gen/raft"
	"runtime"
	"sync"
	"time"

	"pulsardb/internal/configuration/properties"

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
	sendQueues  []chan raftpb.Message
	sendStopCh  chan struct{}
	sendWg      sync.WaitGroup
}

func NewNode(rc *properties.RaftConfigProperties, localAddr string) (*Node, error) {
	cfg, err := newNodeConfig(rc, localAddr)
	if err != nil {
		return nil, err
	}

	n := &Node{
		Id:          rc.NodeId,
		storage:     cfg.storage,
		raftPeers:   rc.RaftPeers,
		clientPeers: rc.ClientPeers,
		clients:     make(map[uint64]Client),
		confState:   cfg.storage.ConfState(),
		Timeout:     rc.Timeout,
		raftNode:    cfg.raftNode,
	}

	if err := n.InitAllPeerClients(); err != nil {
		return nil, err
	}

	workers := min(runtime.GOMAXPROCS(0), 8)
	queueSize := 1024
	n.initSendPool(workers, queueSize)

	slog.Info("raft Node created", "id", rc.NodeId)
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
	n.confState = cs
}

func (n *Node) initSendPool(workers, queueSize int) {
	if workers <= 0 {
		workers = 1
	}
	if queueSize <= 0 {
		queueSize = 1
	}
	n.sendStopCh = make(chan struct{})
	n.sendQueues = make([]chan raftpb.Message, workers)

	for i := 0; i < workers; i++ {
		ch := make(chan raftpb.Message, queueSize)
		n.sendQueues[i] = ch

		n.sendWg.Add(1)
		go func(in <-chan raftpb.Message) {
			defer n.sendWg.Done()
			for {
				select {
				case msg, ok := <-in:
					if !ok {
						return
					}
					n.sendMessage(msg)
				case <-n.sendStopCh:
					return
				}
			}
		}(ch)
	}
}

func (n *Node) stopSendPool() {
	if n.sendStopCh == nil {
		return
	}
	close(n.sendStopCh)
	for _, ch := range n.sendQueues {
		close(ch)
	}
	n.sendWg.Wait()
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
	return n.raftNode.Propose(ctx, data)
}

func (n *Node) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.raftNode.ReadIndex(ctx, rctx)
}

func (n *Node) ApplyConfChange(cc raftpb.ConfChange) *raftpb.ConfState {
	return n.raftNode.ApplyConfChange(cc)
}

func (n *Node) ReportUnreachable(id uint64) {
	n.raftNode.ReportUnreachable(id)
}

func (n *Node) ReportSnapshot(id uint64, status etcdraft.SnapshotStatus) {
	n.raftNode.ReportSnapshot(id, status)
}

func (n *Node) restoreFromConfState() {
	slog.Debug("restoring peer connections from confState",
		"node_id", n.Id,
		"voters", n.confState.Voters,
		"learners", n.confState.Learners,
	)

	if len(n.confState.Voters) == 0 {
		slog.Debug("confState empty, using configured raftPeers",
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
			}

			slog.Debug("restored peer client",
				"node_id", n.Id,
				"peer_id", voterID,
			)
		}
	}
}

func (n *Node) InitAllPeerClients() error {
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

	raftConn, err := grpc.NewClient(raftAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}

	clientConn, err := grpc.NewClient(clientAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		_ = raftConn.Close()
		return err
	}

	client := newCombinedClient(raftConn, clientConn)

	n.mu.Lock()
	n.clients[id] = client
	n.mu.Unlock()

	slog.Debug("initialized peer client", "peer_id", id, "raftAddr", raftAddr, "clientAddr", clientAddr)
	return nil
}

func (n *Node) sendMessages(msgs []raftpb.Message) {
	if len(msgs) == 0 {
		return
	}
	if len(n.sendQueues) == 0 {

		for _, msg := range msgs {
			if msg.To != 0 {
				n.sendMessage(msg)
			}
		}
		return
	}

	for _, msg := range msgs {
		if msg.To == 0 {
			continue
		}
		idx := int(msg.To % uint64(len(n.sendQueues)))
		q := n.sendQueues[idx]

		select {
		case q <- msg:
		default:
			slog.Debug("raft send queue full; dropping message",
				"to", msg.To,
				"type", msg.Type,
			)
		}
	}
}

func (n *Node) sendMessage(msg raftpb.Message) {
	n.mu.RLock()
	client, ok := n.clients[msg.To]
	n.mu.RUnlock()

	if !ok {
		slog.Warn("no client for peer",
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
}

func (n *Node) Stop() {
	n.stopSendPool()
	n.StopClients()
	n.raftNode.Stop()
	slog.Info("raft Node stopped", "id", n.Id)
}
