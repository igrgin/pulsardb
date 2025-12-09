package raft

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	raftevents "pulsardb/internal/raft/gen"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const sendTimeout = 5 * time.Second

// sendMessages sends raft messages to peers concurrently.
func (n *Node) sendMessages(msgs []raftpb.Message) {
	for _, msg := range msgs {
		if msg.To == 0 {
			continue
		}
		go n.sendMessage(msg)
	}
}

// sendMessage sends a single raft message to a peer.
func (n *Node) sendMessage(msg raftpb.Message) {
	n.mu.RLock()
	client, ok := n.clients[msg.To]
	n.mu.RUnlock()

	if !ok {
		slog.Warn("no client for peer",
			"to", msg.To,
			"type", msg.Type.String(),
		)
		n.raftNode.ReportUnreachable(msg.To)
		return
	}

	data, err := json.Marshal(msg)
	if err != nil {
		slog.Error("failed to marshal raft message",
			"to", msg.To,
			"type", msg.Type.String(),
			"error", err,
		)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()

	if _, err := client.SendRaftMessage(ctx, &raftevents.RaftMessage{Data: data}); err != nil {
		slog.Debug("failed to send raft message",
			"to", msg.To,
			"type", msg.Type.String(),
			"error", err,
		)
		n.raftNode.ReportUnreachable(msg.To)
	}
}

// initAllPeerClients initializes gRPC clients for all configured peers.
func (n *Node) initAllPeerClients() error {
	for id, addr := range n.peers {
		if id == n.Id {
			continue
		}
		if err := n.initPeerClient(id, addr); err != nil {
			return err
		}
	}
	return nil
}

// initPeerClient creates a gRPC client for a single peer.
func (n *Node) initPeerClient(id uint64, addr string) error {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}

	client := newCombinedClient(conn)

	n.mu.Lock()
	n.clients[id] = client
	n.mu.Unlock()

	slog.Debug("initialized peer client", "peer_id", id, "addr", addr)
	return nil
}

// getLeaderClient returns the gRPC client for the current leader.
func (n *Node) getLeaderClient(leaderID uint64) (Client, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	client, ok := n.clients[leaderID]
	return client, ok
}
