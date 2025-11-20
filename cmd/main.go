package main

import (
	"log/slog"
	"os"
	"os/signal"
	"pulsardb/config/initializer"
	"pulsardb/server"
	"syscall"
)

func main() {
	AppConfig, err := initializer.Initialize("config/base",
		"config/profiles", "info")

	if err != nil {
		slog.Error("failed to initialize configuration", "error", err)
		os.Exit(1)
	}

	slog.Info("starting application...")

	lis, s, err := server.Start(AppConfig.Server.Network, ":"+AppConfig.Server.Port)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	slog.Info("application started")
	<-quit

	slog.Info("shutting down server...")
	s.GracefulStop()
	lis.Close()
}
