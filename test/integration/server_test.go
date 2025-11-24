package integration

import (
	"context"
	"net"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/core/queue"
	"pulsardb/internal/transport"
	command_event "pulsardb/internal/transport/gen"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startGRPCServer(t *testing.T) net.Listener {
	t.Helper()

	const (
		network   = "tcp"
		addr      = ":0"
		timeout   = 10
		queueSize = 10
	)

	commandConfig := properties.CommandConfigProperties{queueSize}

	dbQueue, err := queue.CreateTaskQueue(&commandConfig)
	require.NoError(t, err, "Failed to create TaskQueue")

	transportConfig := properties.TransportConfigProperties{Network: network, Address: addr, Timeout: timeout}
	lis, s, err := transport.Start(&transportConfig, dbQueue)
	require.NoError(t, err, "Transport failed to start")
	require.NotNil(t, lis, "listener must not be nil")
	require.NotNil(t, s, "grpc transport must not be nil")

	t.Cleanup(func() {
		s.GracefulStop()
		_ = lis.Close()
	})

	return lis
}

func TestStart_ListenerIsActive(t *testing.T) {
	lis := startGRPCServer(t)
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "expected transport to be listening at %s, but dial failed", lis.Addr().String())
	t.Cleanup(func() {
		_ = conn.Close()
	})

	assert.NotNil(t, conn, "connection should not be nil")
}

func TestStart_HandleEventSuccess(t *testing.T) {
	lis := startGRPCServer(t)

	// Give the server a brief moment to start accepting connections.
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "failed to connect to gRPC transport")
	t.Cleanup(func() {
		_ = conn.Close()
	})

	// Service name changed on the server side to CommandEventService,
	// so the client constructor must match that.
	client := command_event.NewCommandEventServiceClient(conn)

	req := &command_event.CommandEventRequest{
		Type: command_event.CommandEventType_SET,
		Key:  "mykey",
		Value: &command_event.CommandEventRequest_StringValue{
			StringValue: "myvalue",
		},
		OnlyIfAbsent:  false,
		OnlyIfPresent: false,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.ProcessCommandEvent(ctx, req)
	require.NoError(t, err, "ProcessCommandEvent failed")
	assert.NotNil(t, resp, "expected response, got nil")
}
