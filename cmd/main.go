package main

import (
	"log/slog"
	"pulsardb/config/application"
	"pulsardb/server"
	"pulsardb/utility/logging"
)

var AppConfig = applicationConfig.Initialize("application", "config/application/base",
	"config/application/profiles")

func main() {
	logger := logging.NewLogger(AppConfig.Meta.LogLevel)
	slog.SetDefault(logger)
	slog.Info("starting application")
	server.Start(AppConfig.Server.Network, ":"+AppConfig.Server.Port)
}
