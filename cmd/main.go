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

	storageService := storage.NewStorageService()
	cmdService := command.NewCommandService(commandConfig)

	setHandler := command.NewSetHandler(storageService)
	getHandler := command.NewGetHandler(storageService)
	deleteHandler := command.NewDeleteHandler(storageService)

	raftNode, err := raft.NewRaftNode(raftConfig, storageService, cmdService, net.JoinHostPort(transportConfig.Address, transportConfig.RaftPort))
	if err != nil {
		slog.Error("Failed to start node with error", "ERROR", err)
		return
	}

	status := raftNode.Node.Status()
	slog.Info("raft status on startup",
		"id", raftNode.Id,
		"state", status.RaftState.String(),
		"term", status.Term,
		"lead", status.Lead,
		"voters", status.Config.Voters,
	)
	raftNode.StartLoop()

	commandHandlers := map[command_events.CommandEventType]command.Handler{
		command_events.CommandEventType_SET:    setHandler,
		command_events.CommandEventType_GET:    getHandler,
		command_events.CommandEventType_DELETE: deleteHandler,
	}

	taskExecutor := command.NewTaskExecutor(cmdService, commandHandlers)
	go taskExecutor.Execute()

	transportService := transport.NewTransportService(transportConfig, cmdService, raftNode)

	_, _, err = transportService.StartServer()
	if err != nil {
		slog.Error("Failed to start transport server", "Error", err)
		return
	}

	slog.Info("Database Ready")
	<-ctx.Done()

	transportService.Server.GracefulStop()
	taskExecutor.Stop()
	slog.Info("Shutting down database...")
}
