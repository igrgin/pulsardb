package transport

import (
	"log/slog"
	"net"
	"pulsardb/internal/command"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/raft"
	"pulsardb/internal/transport/endpoint"
	"pulsardb/internal/transport/gen/commandevents"
	"pulsardb/internal/transport/gen/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Service struct {
	network              string
	address              string
	clientPort           string
	raftPort             string
	timeout              uint64
	commandService       *command.Service
	raftService          *raft.Service
	RaftServer           *grpc.Server
	ClientServer         *grpc.Server
	maxConcurrentStreams uint32
}

func NewTransportService(transportConfig *properties.TransportConfigProperties, commandService *command.Service, raftService *raft.Service) *Service {
	return &Service{
		network:              transportConfig.Network,
		address:              transportConfig.Address,
		clientPort:           transportConfig.ClientPort,
		raftPort:             transportConfig.RaftPort,
		timeout:              transportConfig.Timeout,
		commandService:       commandService,
		raftService:          raftService,
		maxConcurrentStreams: transportConfig.MaxConcurrentStreams,
	}
}

func (ts *Service) StartRaftServer() (net.Listener, error) {
	raftLis, err := net.Listen(ts.network, net.JoinHostPort(ts.address, ts.raftPort))
	if err != nil {
		return nil, err
	}

	var raftOpts []grpc.ServerOption
	raftOpts = append(raftOpts, grpc.MaxConcurrentStreams(ts.maxConcurrentStreams))

	ts.RaftServer = grpc.NewServer(raftOpts...)

	rafttransportpb.RegisterRaftTransportServiceServer(
		ts.RaftServer,
		endpoint.NewRaftTransportServer(ts.raftService),
	)

	reflection.Register(ts.RaftServer)
	slog.Info("transport listening for raft", "Raft_Addr", raftLis.Addr())

	go func() {
		if err := ts.RaftServer.Serve(raftLis); err != nil {
			slog.Error("failed to serve raft listener", "Error", err)
		}
	}()

	return raftLis, nil
}

func (ts *Service) StartClientServer() (net.Listener, error) {
	clientLis, err := net.Listen(ts.network, net.JoinHostPort(ts.address, ts.clientPort))
	if err != nil {
		return nil, err
	}

	var clientOpts []grpc.ServerOption
	clientOpts = append(clientOpts, grpc.MaxConcurrentStreams(ts.maxConcurrentStreams))

	ts.ClientServer = grpc.NewServer(clientOpts...)

	commandeventspb.RegisterCommandEventClientServiceServer(
		ts.ClientServer,
		&endpoint.GRPCServer{CommandService: ts.commandService},
	)

	reflection.Register(ts.ClientServer)
	slog.Info("transport listening for client", "Client_Addr", clientLis.Addr())

	go func() {
		if err := ts.ClientServer.Serve(clientLis); err != nil {
			slog.Error("failed to serve client listener", "Error", err)
		}
	}()

	return clientLis, nil
}
