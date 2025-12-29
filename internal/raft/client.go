package raft

import (
	"context"
	"log/slog"
	"pulsardb/internal/transport/gen/commandevents"
	rafttransportpb "pulsardb/internal/transport/gen/raft"

	"google.golang.org/grpc"
)

type Client interface {
	SendRaftMessage(ctx context.Context, in *rafttransportpb.RaftMessage, opts ...grpc.CallOption) (*rafttransportpb.RaftMessageResponse, error)
	GetReadIndex(ctx *context.Context, in *rafttransportpb.GetReadIndexRequest, opts ...grpc.CallOption) (*rafttransportpb.GetReadIndexResponse, error)
	ProcessCommandEvent(ctx context.Context, in *commandeventspb.CommandEventRequest, opts ...grpc.CallOption) (*commandeventspb.CommandEventResponse, error)

	Close() error
}

type combinedClient struct {
	raftConn   *grpc.ClientConn
	cmdConn    *grpc.ClientConn
	raftClient rafttransportpb.RaftTransportServiceClient
	cmdClient  commandeventspb.CommandEventClientServiceClient
}

func newCombinedClient(raftConn, cmdConn *grpc.ClientConn) *combinedClient {
	slog.Debug("creating combined client",
		"raftTarget", raftConn.Target(),
		"cmdTarget", cmdConn.Target(),
	)
	return &combinedClient{
		raftConn:   raftConn,
		cmdConn:    cmdConn,
		raftClient: rafttransportpb.NewRaftTransportServiceClient(raftConn),
		cmdClient:  commandeventspb.NewCommandEventClientServiceClient(cmdConn),
	}
}

func (c *combinedClient) Close() error {
	slog.Debug("closing combined client",
		"raftTarget", c.raftConn.Target(),
		"cmdTarget", c.cmdConn.Target(),
	)
	var err1, err2 error
	if c.raftConn != nil {
		err1 = c.raftConn.Close()
		if err1 != nil {
			slog.Warn("failed to close raft connection", "error", err1)
		}
	}
	if c.cmdConn != nil {
		err2 = c.cmdConn.Close()
		if err2 != nil {
			slog.Warn("failed to close command connection", "error", err2)
		}
	}
	if err1 != nil {
		return err1
	}
	return err2
}

func (c *combinedClient) SendRaftMessage(
	ctx context.Context,
	in *rafttransportpb.RaftMessage,
	opts ...grpc.CallOption,
) (*rafttransportpb.RaftMessageResponse, error) {
	return c.raftClient.SendRaftMessage(ctx, in, opts...)
}

func (c *combinedClient) ProcessCommandEvent(
	ctx context.Context,
	in *commandeventspb.CommandEventRequest,
	opts ...grpc.CallOption,
) (*commandeventspb.CommandEventResponse, error) {
	slog.Debug("forwarding command to leader", "event_id", in.EventId, "type", in.Type)
	resp, err := c.cmdClient.ProcessCommandEvent(ctx, in, opts...)
	if err != nil {
		slog.Debug("forward command failed", "event_id", in.EventId, "error", err)
	}
	return resp, err
}

func (c *combinedClient) GetReadIndex(ctx *context.Context, req *rafttransportpb.GetReadIndexRequest, opts ...grpc.CallOption) (*rafttransportpb.GetReadIndexResponse, error) {
	slog.Debug("requesting read index from leader", "from_node", req.FromNode)
	resp, err := c.raftClient.GetReadIndex(*ctx, req, opts...)
	if err != nil {
		slog.Debug("get read index failed", "error", err)
	}
	return resp, err
}
