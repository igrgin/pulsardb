package integration

import (
	"context"
	"net"
	"pulsardb/server"
	dbevents "pulsardb/server/gen"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startGRPCServer(t *testing.T) net.Listener {
	t.Helper()
	network := "tcp"
	addr := ":0"

	lis, s, err := server.Start(network, addr)

	if err != nil {
		t.Fatalf("Server failed to start with error: %v", err)
	}

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
	if err != nil {
		t.Fatalf("expected server to be listening at %s, but dial failed: %v", lis.Addr().String(), err)
	}

	conn.Close()
}

func TestStart_HandleEventSuccess(t *testing.T) {
	lis := startGRPCServer(t)

	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.NewClient("dns:///"+lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to connect to gRPC server: %v", err)
	}

	client := dbevents.NewDBEventServiceClient(conn)

	req := &dbevents.DBEventRequest{
		Type:          dbevents.DBEventType_SET,
		Key:           []byte("mykey"),
		Value:         []byte("myvalue"),
		OnlyIfAbsent:  false,
		OnlyIfPresent: false,
	}

	ctx := context.Background()
	resp, err := client.HandleEvent(ctx, req)
	if err != nil {
		t.Fatalf("HandleEvent failed: %v", err)
	}

	if resp == nil {
		t.Error("expected response, got nil")
	}
}
