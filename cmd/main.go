package main

import (
	"log/slog"
	"pulsardb/config/application"
	"pulsardb/server"
)

var AppConfig, _ = applicationConfig.Initialize("config/application/base",
	"config/application/profiles", "info")

func main() {
	slog.Info("starting application")
	server.Start(AppConfig.Server.Network, ":"+AppConfig.Server.Port)
}
