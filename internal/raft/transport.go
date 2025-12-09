package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	raftevents "pulsardb/internal/raft/gen"
	commandevents "pulsardb/internal/transport/gen"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// initPeerClients dials all peers and creates gRPC clients.
func (n *Node) initPeerClients() error {
	for id, addr := range n.peers {
		if id == n.Id {
			continue
		}
		conn, err := dialRaftPeer(addr)
		if err != nil {
			return fmt.Errorf("failed to dial peer %d at %s: %w", id, addr, err)
		}

		raftClient := raftevents.NewRaftTransportServiceClient(conn)
		cmdClient := commandevents.NewCommandEventServiceClient(conn)

		n.clients[id] = &combinedClient{
			raftClient:    raftClient,
			commandClient: cmdClient,
		}
	}
	return nil
}

// sendMessages delivers raftpb.Message slice to peers via gRPC.
func (n *Node) sendMessages(msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == 0 || m.To == n.Id {
			continue
		}

		client, ok := n.clients[m.To]
		if !ok {
			slog.Error("missing raft client for peer", "to", m.To)
			continue
		}

		data, err := json.Marshal(&m)
		if err != nil {
			slog.Error("failed to marshal raft message", "error", err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.SendRaftMessage(ctx, &raftevents.RaftMessage{Data: data})
		cancel()
		if err != nil {
			slog.Error("failed to send raft message", "to", m.To, "error", err)
		}
	}
}

func dialRaftPeer(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             5 * time.Second,
		PermitWithoutStream: true,
	}))
}
