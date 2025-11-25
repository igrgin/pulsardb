package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"pulsardb/internal/command"
	"pulsardb/internal/configuration"
	"pulsardb/internal/configuration/properties"
	logging "pulsardb/internal/logging"
	"pulsardb/internal/storage"
	"pulsardb/internal/transport"
	"pulsardb/internal/transport/gen"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	config, err := configuration.Load()
	if err != nil {
		slog.Error("Failed to initialize application context", "Error", err)
		return
	}

	logging.Init(config.Application.LogLevel)
	slog.Info("Starting database...")

	cfgProvider := properties.NewProvider(config)

	storageService := storage.NewStorageService()

	commandConfig := cfgProvider.GetCommand()
	cmdService := command.NewCommandService(commandConfig)

	setHandler := command.NewSetHandler(storageService)
	getHandler := command.NewGetHandler(storageService)
	deleteHandler := command.NewDeleteHandler(storageService)

	commandHandlers := map[command_events.CommandEventType]command.Handler{
		command_events.CommandEventType_SET:    setHandler,
		command_events.CommandEventType_GET:    getHandler,
		command_events.CommandEventType_DELETE: deleteHandler,
	}

	taskExecutor := command.NewTaskExecutor(cmdService, commandHandlers)
	go taskExecutor.Execute()

	serverConfig := cfgProvider.GetTransport()
	transportService := transport.NewTransportService(serverConfig, cmdService)

	_, err = transportService.StartServer()
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
