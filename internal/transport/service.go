package transport

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"pulsardb/internal/command"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/raft"
	raftevents "pulsardb/internal/raft/gen"
	"pulsardb/internal/transport/endpoint"
	"pulsardb/internal/transport/gen"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Service struct {
	network        string
	address        string
	clientPort     string
	raftPort       string
	timeout        int
	commandService *command.Service
	raftNode       *raft.Node
	Server         *grpc.Server
}

func NewTransportService(transportConfig *properties.TransportConfigProperties, commandService *command.Service, raftNode *raft.Node) *Service {
	return &Service{
		network:        transportConfig.Network,
		address:        transportConfig.Address,
		clientPort:     transportConfig.ClientPort,
		raftPort:       transportConfig.RaftPort,
		timeout:        transportConfig.Timeout,
		commandService: commandService,
		raftNode:       raftNode,
	}
}

func (ts *Service) StartServer() (net.Listener, net.Listener, error) {
	clientLis, err := net.Listen(ts.network, net.JoinHostPort(ts.address, ts.clientPort))
	if err != nil {
		return nil, nil, err
	}

	raftLis, err := net.Listen(ts.network, net.JoinHostPort(ts.address, ts.raftPort))
	if err != nil {
		clientLis.Close()
		return nil, nil, err
	}

	var opts []grpc.ServerOption
	const minTimeout = 60

	if ts.timeout >= minTimeout {
		slog.Info(fmt.Sprintf("Setting transport timeout to %d seconds", ts.timeout))
		opts = append(opts,
			grpc.UnaryInterceptor(timeoutInterceptor(time.Duration(ts.timeout)*time.Second)))
	} else {
		slog.Warn(fmt.Sprintf("Timeout can't be less than raft timeout. Setting transport timeout to %d seconds.", minTimeout))
		opts = append(opts,
			grpc.UnaryInterceptor(timeoutInterceptor(time.Duration(minTimeout)*time.Second)))
	}

	ts.Server = grpc.NewServer(opts...)

	// Command service
	command_events.RegisterCommandEventServiceServer(ts.Server, &endpoint.GRPCServer{RaftNode: ts.raftNode})

	// Raft transport service
	if ts.raftNode != nil {
		raftevents.RegisterRaftTransportServiceServer(ts.Server, endpoint.NewRaftTransportServer(ts.raftNode))
	}

	reflection.Register(ts.Server)
	slog.Info(fmt.Sprintf("transport listening at %s for client and %s for raft", clientLis.Addr().String(), raftLis.Addr().String()))

	go func() {
		if err := ts.Server.Serve(clientLis); err != nil {
			slog.Error("failed to serve listener", "Error", err)
		}
	}()

	go func() {
		if err := ts.Server.Serve(raftLis); err != nil {
			slog.Error("failed to serve listener", "Error", err)
		}
	}()

	return clientLis, raftLis, nil
}

func timeoutInterceptor(timeout time.Duration) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return handler(ctx, req)
	}
}
