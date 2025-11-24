package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"pulsardb/internal/configuration"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/core/queue"
	"pulsardb/internal/transport"
	"syscall"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	slog.Info("Starting database...")
	config, err := configuration.Load()
	if err != nil {
		slog.Error("Failed to initialize application context", "Error", err)
		return
	}

	cfgProvider := properties.NewProvider(config)

	commandConfig := cfgProvider.GetCommand()
	TaskQueue, err := queue.CreateTaskQueue(&commandConfig)
	if err != nil {
		slog.Error("Failed to create DB Queue", "Error", err)
		return
	}

	serverConfig := cfgProvider.GetTransport()
	_, grpcServer, err := transport.Start(&serverConfig, TaskQueue)
	if err != nil {
		slog.Error("Failed to start DB transport", "Error", err)
		return
	}

	TaskQueue.Start()

	slog.Info("Database Ready")
	<-ctx.Done()

	slog.Info("Shutting down database...")
	TaskQueue.Close()
	grpcServer.GracefulStop()
}
