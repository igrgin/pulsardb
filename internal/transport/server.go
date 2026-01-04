package transport

import (
	"fmt"
	"log/slog"
	"net"
	"pulsardb/internal/configuration"
	"pulsardb/internal/metrics"

	"pulsardb/internal/transport/gen/command"
	rafttransportpb "pulsardb/internal/transport/gen/raft"
	"pulsardb/internal/transport/handler"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type ServerConfig struct {
	Network    string
	Address    string
	ClientPort string
	RaftPort   string
}

type Server struct {
	cfg          ServerConfig
	clientServer *grpc.Server
	raftServer   *grpc.Server
	clientLis    net.Listener
	raftLis      net.Listener
}

func NewServer(
	cfg *configuration.TransportConfigurationProperties,
	cmdProcessor handler.CommandProcessor,
	raftHandler handler.RaftHandler,
) *Server {
	s := &Server{cfg: ServerConfig{Network: cfg.Network, Address: cfg.Address, ClientPort: cfg.ClientPort, RaftPort: cfg.RaftPort}}

	RaftOpts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(cfg.RaftTransportConfig.MaxConcurrentStreams),
		grpc.NumStreamWorkers(cfg.RaftTransportConfig.NumStreamWorkers),
		grpc.ConnectionTimeout(cfg.RaftTransportConfig.Timeout),
		grpc.ChainUnaryInterceptor(metrics.UnaryServerInterceptor()),
	}
	ClientOpts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(cfg.ClientTransportConfig.MaxConcurrentStreams),
		grpc.NumStreamWorkers(cfg.ClientTransportConfig.NumStreamWorkers),
		grpc.ConnectionTimeout(cfg.ClientTransportConfig.Timeout),
		grpc.ChainUnaryInterceptor(metrics.UnaryServerInterceptor()),
	}

	s.clientServer = grpc.NewServer(ClientOpts...)
	commandeventspb.RegisterCommandEventClientServiceServer(
		s.clientServer,
		handler.NewCommandHandler(cmdProcessor),
	)
	reflection.Register(s.clientServer)

	s.raftServer = grpc.NewServer(RaftOpts...)
	rafttransportpb.RegisterRaftTransportServiceServer(
		s.raftServer,
		handler.NewRaftTransportHandler(raftHandler),
	)
	reflection.Register(s.raftServer)

	return s
}

func (s *Server) StartRaft() error {
	network := s.cfg.Network
	if network == "" {
		network = "tcp"
	}

	addr := net.JoinHostPort(s.cfg.Address, s.cfg.RaftPort)
	lis, err := net.Listen(network, addr)
	if err != nil {
		return fmt.Errorf("listen raft: %w", err)
	}
	s.raftLis = lis

	slog.Info("raft server listening", "addr", addr)

	go func() {
		if err := s.raftServer.Serve(lis); err != nil {
			slog.Error("raft server error", "error", err)
		}
	}()

	return nil
}

func (s *Server) StartClient() error {
	network := s.cfg.Network
	if network == "" {
		network = "tcp"
	}

	addr := net.JoinHostPort(s.cfg.Address, s.cfg.ClientPort)
	lis, err := net.Listen(network, addr)
	if err != nil {
		return fmt.Errorf("listen client: %w", err)
	}
	s.clientLis = lis

	slog.Info("client server listening", "addr", addr)

	go func() {
		if err := s.clientServer.Serve(lis); err != nil {
			slog.Error("client server error", "error", err)
		}
	}()

	return nil
}

func (s *Server) Stop() {
	if s.clientServer != nil {
		s.clientServer.GracefulStop()
	}
	if s.raftServer != nil {
		s.raftServer.GracefulStop()
	}
}

func (s *Server) RaftAddr() string {
	if s.raftLis != nil {
		return s.raftLis.Addr().String()
	}
	return ""
}

func (s *Server) ClientAddr() string {
	if s.clientLis != nil {
		return s.clientLis.Addr().String()
	}
	return ""
}
