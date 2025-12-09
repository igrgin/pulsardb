package raft

import (
	"context"

	raftevents "pulsardb/internal/raft/gen"
	commandevents "pulsardb/internal/transport/gen"

	"google.golang.org/grpc"
)

// Client defines the interface for peer communication.
type Client interface {
	SendRaftMessage(ctx context.Context, in *raftevents.RaftMessage, opts ...grpc.CallOption) (*raftevents.RaftMessageResponse, error)
	ProcessCommandEvent(ctx context.Context, in *commandevents.CommandEventRequest, opts ...grpc.CallOption) (*commandevents.CommandEventResponse, error)
}

// combinedClient implements Client by delegating to underlying gRPC clients.
type combinedClient struct {
	raftClient    raftevents.RaftTransportServiceClient
	commandClient commandevents.CommandEventServiceClient
}

// newCombinedClient creates a new combined client from a gRPC connection.
func newCombinedClient(conn grpc.ClientConnInterface) *combinedClient {
	return &combinedClient{
		raftClient:    raftevents.NewRaftTransportServiceClient(conn),
		commandClient: commandevents.NewCommandEventServiceClient(conn),
	}
}

func (c *combinedClient) SendRaftMessage(
	ctx context.Context,
	in *raftevents.RaftMessage,
	opts ...grpc.CallOption,
) (*raftevents.RaftMessageResponse, error) {
	return c.raftClient.SendRaftMessage(ctx, in, opts...)
}

func (c *combinedClient) ProcessCommandEvent(
	ctx context.Context,
	in *commandevents.CommandEventRequest,
	opts ...grpc.CallOption,
) (*commandevents.CommandEventResponse, error) {
	return c.commandClient.ProcessCommandEvent(ctx, in, opts...)
}
