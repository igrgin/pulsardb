package raft

import (
	"context"
	"log/slog"
	rafttransportpb "pulsardb/internal/transport/gen/raft"

	"google.golang.org/grpc"
)

type Client struct {
	raftConn   *grpc.ClientConn
	raftClient rafttransportpb.RaftTransportServiceClient
}

func newClient(raftConn *grpc.ClientConn) Client {
	slog.Debug("creating combined client",
		"raftTarget", raftConn.Target(),
	)
	return Client{
		raftConn:   raftConn,
		raftClient: rafttransportpb.NewRaftTransportServiceClient(raftConn),
	}
}

func (c *Client) Close() error {
	slog.Debug("closing Raft client",
		"raftTarget", c.raftConn.Target(),
	)
	var err error
	if c.raftConn != nil {
		err = c.raftConn.Close()
		if err != nil {
			slog.Warn("failed to close raft connection", "error", err)
		}
	}

	return err
}

func (c *Client) SendRaftMessage(
	ctx context.Context,
	in *rafttransportpb.RaftMessage,
	opts ...grpc.CallOption,
) (*rafttransportpb.RaftMessageResponse, error) {
	return c.raftClient.SendRaftMessage(ctx, in, opts...)
}

func (c *Client) GetReadIndex(ctx context.Context, req *rafttransportpb.GetReadIndexRequest, opts ...grpc.CallOption) (*rafttransportpb.GetReadIndexResponse, error) {
	slog.Debug("requesting read index from leader", "from_node", req.FromNode)
	resp, err := c.raftClient.GetReadIndex(ctx, req, opts...)
	if err != nil {
		slog.Debug("get read index failed", "error", err)
	}
	return resp, err
}

func (c *Client) RequestJoinCluster(
	ctx context.Context,
	in *rafttransportpb.JoinRequest,
	opts ...grpc.CallOption,
) (*rafttransportpb.JoinResponse, error) {
	slog.Debug("requesting to join cluster", "node_id", in.NodeId)
	return c.raftClient.RequestJoinCluster(ctx, in, opts...)
}
