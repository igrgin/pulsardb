package integration

import (
	"context"
	"net"
	"pulsardb/server"
	"pulsardb/server/gen"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startGRPCServer(t *testing.T) net.Listener {
	t.Helper()
	network := "tcp"
	addr := ":0"

	lis, s, err := server.Start(network, addr)
	require.NoError(t, err, "Server failed to start")
	require.NotNil(t, lis, "listener must not be nil")
	require.NotNil(t, s, "grpc server must not be nil")

	t.Cleanup(func() {
		s.GracefulStop()
		lis.Close()
	})

	return lis
}

func TestStart_ListenerIsActive(t *testing.T) {
	lis := startGRPCServer(t)

	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "expected server to be listening at %s, but dial failed: %v", lis.Addr().String(), err)
	defer conn.Close()

	assert.NotNil(t, conn, "connection should not be nil")
}

func TestStart_HandleEventSuccess(t *testing.T) {
	lis := startGRPCServer(t)

	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "failed to connect to gRPC server: %v", err)
	defer conn.Close()

	client := db_events.NewDBEventServiceClient(conn)

	req := &db_events.DBEventRequest{
		Type:          db_events.DBEventType_SET,
		Key:           []byte("mykey"),
		Value:         []byte("myvalue"),
		OnlyIfAbsent:  false,
		OnlyIfPresent: false,
	}

	ctx := context.Background()
	resp, err := client.HandleEvent(ctx, req)

	require.NoError(t, err, "HandleEvent failed: %v", err)
	assert.NotNil(t, resp, "expected response, got nil")
}
