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
	"pulsardb/internal/storage"
	"pulsardb/internal/transport"
	"pulsardb/internal/transport/gen"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	config, commandConfig, raftConfig, transportConfig, err := configuration.LoadConfig()
	if err != nil {
		slog.Error("Failed to initialize application context", "Error", err)
		return
	}

	logging.Init(config.Application.LogLevel)
	slog.Info("Starting database...")

	// Initialize services
	storageService := storage.NewStorageService()
	cmdService := command.NewCommandService(commandConfig)

	// Create command handlers
	setHandler := command.NewSetHandler(storageService)
	getHandler := command.NewGetHandler(storageService)
	deleteHandler := command.NewDeleteHandler(storageService)

	// Create raft node (renamed from NewRaftNode to NewNode)
	raftAddr := net.JoinHostPort(transportConfig.Address, transportConfig.RaftPort)
	raftNode, err := raft.NewNode(raftConfig, storageService, cmdService, raftAddr)
	if err != nil {
		slog.Error("Failed to create raft node", "error", err)
		return
	}

	// Log initial status (access underlying raft node via RawNode)
	rawNode := raftNode.RawNode()
	status := (*rawNode).Status()
	slog.Info("raft status on startup",
		"id", raftNode.Id,
		"state", status.RaftState.String(),
		"term", status.Term,
		"lead", status.Lead,
		"voters", status.Config.Voters,
	)

	raftNode.Start()

	// Register command handlers
	commandHandlers := map[command_events.CommandEventType]command.Handler{
		command_events.CommandEventType_SET:    setHandler,
		command_events.CommandEventType_GET:    getHandler,
		command_events.CommandEventType_DELETE: deleteHandler,
	}

	// Start task executor
	taskExecutor := command.NewTaskExecutor(cmdService, commandHandlers)
	go taskExecutor.Execute()

	// Start transport server
	transportService := transport.NewTransportService(transportConfig, cmdService, raftNode)
	_, _, err = transportService.StartServer()
	if err != nil {
		slog.Error("Failed to start transport server", "error", err)
		raftNode.Stop()
		return
	}

	slog.Info("Database Ready")

	// Wait for shutdown signal
	<-ctx.Done()

	// Graceful shutdown
	slog.Info("Shutting down database...")

	// Stop in reverse order of startup
	transportService.Server.GracefulStop()
	taskExecutor.Stop()
	raftNode.Stop() // renamed from StopLoop to Stop

	slog.Info("Database shutdown complete")
}
