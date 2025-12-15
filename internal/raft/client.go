package raft

import (
	"context"
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
	return &combinedClient{
		raftConn:   raftConn,
		cmdConn:    cmdConn,
		raftClient: rafttransportpb.NewRaftTransportServiceClient(raftConn),
		cmdClient:  commandeventspb.NewCommandEventClientServiceClient(cmdConn),
	}
}

func (c *combinedClient) Close() error {
	var err1, err2 error
	if c.raftConn != nil {
		err1 = c.raftConn.Close()
	}
	if c.cmdConn != nil {
		err2 = c.cmdConn.Close()
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
	return c.cmdClient.ProcessCommandEvent(ctx, in, opts...)
}

func (c *combinedClient) GetReadIndex(ctx *context.Context, req *rafttransportpb.GetReadIndexRequest, opts ...grpc.CallOption) (*rafttransportpb.GetReadIndexResponse, error) {
	return c.raftClient.GetReadIndex(*ctx, req, opts...)
}
