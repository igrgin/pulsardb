package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"pulsardb/config/initializer"
	"pulsardb/database/event_queue"
	"pulsardb/server"
	"syscall"
)

func main() {
	AppConfig, err := initializer.Initialize("config/base",
		"config/profiles", "info")

	if err != nil {
		slog.Error(fmt.Sprintf("failed to initialize configuration: %v", err))
		os.Exit(1)
	}

	slog.Info("starting application...")

	dbQueue, err := event_queue.CreateDBQueue(AppConfig.Database.QueueSize)

	if err != nil {
		slog.Error(fmt.Sprintf("failed to create DB Queue: %v", err))
		os.Exit(1)
	}

	lis, s, err := server.Start(AppConfig.Server.Network, ":"+AppConfig.Server.Port, AppConfig.Server.Timeout, dbQueue)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to start DB server: %v", err))
		os.Exit(1)
	}

	dbQueue.Start()

	quit := make(chan os.Signal, 1)
	slog.Info("application started")
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	<-quit

	s.GracefulStop()
	lis.Close()
	slog.Info("shutting down database...GOODBYE")
}
