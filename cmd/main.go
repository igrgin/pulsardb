package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"pulsardb/internal/command"
	"pulsardb/internal/configuration"
	"pulsardb/internal/logging"
	"pulsardb/internal/raft"
	"pulsardb/internal/statemachine"
	"pulsardb/internal/store"
	"pulsardb/internal/transport"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := configuration.Load(func(o *configuration.LoadOptions) {
		o.BaseDir = "internal/static"
	})
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logging.Init(cfg.App.LogLevel)
	slog.Info("Starting database...")

	configProvider := configuration.NewProvider(cfg)

	slog.Info("loaded config",
		"profile", cfg,
		"node_id", cfg.Raft.NodeID,
		"raft_addr", cfg.Transport.RaftAddr(),
	)

	// 1. Storage (no dependencies)
	storeService := store.NewService()

	// 2. State machine (depends on store)
	stateMachine := statemachine.New(storeService)

	// 3. Raft node (depends on state machine)
	raftNode, err := raft.NewNode(configProvider.GetRaft(), net.JoinHostPort(configProvider.GetTransport().Address, configProvider.GetTransport().RaftPort))
	if err != nil {
		slog.Error("failed to create raft node", "error", err)
		os.Exit(1)
	}

	raftService := raft.NewService(raftNode, storeService, stateMachine, configProvider.GetRaft())

	// 4. Command service (depends on raft as proposer, store as reader)
	commandService := command.NewService(storeService, raftService, command.BatchConfig{
		MaxSize: cfg.Raft.BatchSize,
		MaxWait: cfg.Raft.BatchMaxWait,
	})

	stateMachine.OnApply(commandService.HandleApplied)

	transportServer := transport.NewServer(configProvider.GetTransport(), commandService, raftService)

	transportServer.StartRaft()
	raftService.Start()

	qctx, qcancel := context.WithTimeout(ctx, time.Duration(configProvider.GetApplication().QuorumWaitTime)*time.Second)
	defer qcancel()

	leaderID, readIndex, err := raftService.WaitForQuorum(qctx)
	if err != nil {
		slog.Error("Failed to achieve Raft quorum", "error", err)
		shutdown(transportServer, raftService)
		return
	}

	go raftService.ReconcileConfiguredPeers()

	transportServer.StartClient()

	slog.Info("Database Ready",
		"node_id", raftNode.Id,
		"leader_id", leaderID,
		"read_index", readIndex,
	)

	<-ctx.Done()

	shutdown(transportServer, raftService)
}

func shutdown(transportServer *transport.Server, raftService *raft.Service) {
	transportServer.Stop()
	raftService.Stop()
}
