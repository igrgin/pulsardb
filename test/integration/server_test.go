package integration

import (
	"context"
	"net"
	"pulsardb/internal/command"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/storage"
	"pulsardb/internal/transport"
	"pulsardb/internal/transport/gen"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startGRPCServer(t *testing.T) (net.Listener, *transport.Service) {
	t.Helper()

	const (
		network   = "tcp"
		addr      = "0"
		port      = "0"
		timeout   = 10
		queueSize = 10
	)

	// Command service
	commandConfig := &properties.CommandConfigProperties{QueueSize: queueSize}
	commandService := command.NewCommandService(commandConfig)

	// Storage service
	storageService := storage.NewStorageService()

	// Handlers
	setHandler := command.NewSetHandler(storageService)
	getHandler := command.NewGetHandler(storageService)
	deleteHandler := command.NewDeleteHandler(storageService)

	commandHandlers := map[command_events.CommandEventType]command.Handler{
		command_events.CommandEventType_SET:    setHandler,
		command_events.CommandEventType_GET:    getHandler,
		command_events.CommandEventType_DELETE: deleteHandler,
	}

	// Task executor
	taskExecutor := command.NewTaskExecutor(commandService, commandHandlers)
	go func() {
		taskExecutor.Execute()
	}()

	// Transport service
	transportConfig := &properties.TransportConfigProperties{
		Network: network,
		Address: addr,
		Port:    port,
		Timeout: timeout,
	}

	transService := transport.NewTransportService(transportConfig, commandService)
	require.NotNil(t, transService, "grpc transport must not be nil")

	lis, err := transService.StartServer()
	require.NoError(t, err, "Error starting server")
	require.NotNil(t, lis, "listener must not be nil")

	t.Cleanup(func() {
		transService.Server.GracefulStop()
		_ = lis.Close()
	})

	return lis, transService
}

func dialConn(t *testing.T, lis net.Listener) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.Dial(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err, "failed to connect to gRPC transport")

	t.Cleanup(func() {
		_ = conn.Close()
	})

	return conn
}

func TestStart_ListenerIsActive(t *testing.T) {
	lis, _ := startGRPCServer(t)
	time.Sleep(100 * time.Millisecond)

	conn := dialConn(t, lis)
	assert.NotNil(t, conn, "connection should not be nil")
}

func TestStart_HandleEventSuccess(t *testing.T) {
	lis, _ := startGRPCServer(t)
	time.Sleep(100 * time.Millisecond)

	conn := dialConn(t, lis)
	client := command_events.NewCommandEventServiceClient(conn)

	req := &command_events.CommandEventRequest{
		Type: command_events.CommandEventType_SET,
		Key:  "mykey",
		CmdValue: &command_events.CommandEventValue{
			Value: &command_events.CommandEventValue_IntValue{
				IntValue: 1,
			},
		},
		ExtraAttributes: map[string]string{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.ProcessCommandEvent(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.Empty(t, resp.ErrorMessage)
}
