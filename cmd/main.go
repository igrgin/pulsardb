package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"pulsardb/internal/configuration"
	"pulsardb/internal/logging"
	"pulsardb/internal/raft"
	"pulsardb/internal/transport"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	config, raftConfig, transportConfig, err := configuration.LoadConfig()
	if err != nil {
		slog.Error("Failed to initialize application context", "Error", err)
		return
	}

	logging.Init(config.Application.LogLevel)
	slog.Info("Starting database...")

	raftAddr := net.JoinHostPort(transportConfig.Address, transportConfig.RaftPort)
	raftNode, err := raft.NewNode(raftConfig, raftAddr)
	if err != nil {
		slog.Error("Failed to create raft node", "error", err)
		return
	}

	services, _ := NewServices(raftNode, raftConfig)

	transportService := transport.NewTransportService(
		transportConfig,
		services.Command,
		services.Raft,
	)

	_, err = transportService.StartRaftServer()
	if err != nil {
		slog.Error("Failed to start Raft transport server", "error", err)
		raftNode.Stop()
		return
	}

	services.Raft.Start()

	qctx, qcancel := context.WithTimeout(ctx, time.Duration(config.Application.QuorumWaitTime)*time.Second)
	defer qcancel()

	leaderID, readIndex, err := services.Raft.WaitForQuorum(qctx)
	if err != nil {
		slog.Error("Failed to achieve Raft quorum", "error", err)
		raftNode.Stop()
		return
	}

	_, err = transportService.StartClientServer()
	if err != nil {
		slog.Error("Failed to start client transport server", "error", err)
		raftNode.Stop()
		return
	}

	slog.Info("Database Ready",
		"node_id", raftNode.Id,
		"leader_id", leaderID,
		"read_index", readIndex,
	)

	<-ctx.Done()

	slog.Info("Shutting down database...")
	shutdown(transportService, services.Raft)
	slog.Info("Database shutdown complete")
}

func shutdown(transportService *transport.Service, raftService *raft.Service) {
	transportService.ClientServer.GracefulStop()
	transportService.RaftServer.GracefulStop()
	raftService.Stop()
}
