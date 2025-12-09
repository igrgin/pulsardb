package integration

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"pulsardb/internal/command"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/raft"
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
		timeout   = 60
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
	go taskExecutor.Execute()

	t.Cleanup(func() {
		taskExecutor.Stop()
	})

	// Transport service
	transportConfig := &properties.TransportConfigProperties{
		Network:    network,
		Address:    addr,
		RaftPort:   port,
		ClientPort: port,
		Timeout:    timeout,
	}

	raftNode := getRaftNode()

	transService := transport.NewTransportService(transportConfig, commandService, raftNode)
	require.NotNil(t, transService, "grpc transport must not be nil")

	clientLis, raftLis, err := transService.StartServer()
	require.NoError(t, err, "Error starting server")
	require.NotNil(t, clientLis, "client listener must not be nil")
	require.NotNil(t, raftLis, "raft listener must not be nil")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = raftNode.WaitForLeader(ctx, 50*time.Millisecond)
	require.NoError(t, err, "raft node did not become ready in time")

	t.Cleanup(func() {
		transService.Server.GracefulStop()
		_ = clientLis.Close()
	})

	return clientLis, transService
}

func dialConn(t *testing.T, lis net.Listener) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "failed to create gRPC client")

	// Start connecting
	conn.Connect()

	// Wait until the connection becomes READY (replacement for WithBlock)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Wait for a state change from Idle/Connecting → Ready
	if !conn.WaitForStateChange(ctx, conn.GetState()) {
		t.Fatalf("failed to connect to gRPC transport: timeout")
	}

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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	slog.Info("sending event")
	resp, err := client.ProcessCommandEvent(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.Empty(t, resp.ErrorMessage)
}

func getRaftNode() *raft.Node {
	// Minimal Raft config for a single-node test cluster.
	tmpDir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		panic(err)
	}

	raftCfg := &properties.RaftConfigProperties{
		NodeId:         1,
		Peers:          map[uint64]string{}, // single-node, no peers
		StorageBaseDir: filepath.Join(tmpDir, "storage"),
	}

	storageService := storage.NewStorageService()

	// Use a tiny command queue for the test.
	cmdCfg := &properties.CommandConfigProperties{QueueSize: 10}
	cmdService := command.NewCommandService(cmdCfg)

	// Local raft address can be anything; tests do not dial it directly.
	localAddr := "127.0.0.1:0"

	rn, err := raft.NewRaftNode(raftCfg, storageService, cmdService, localAddr)
	if err != nil {
		panic(err)
	}
	rn.StartLoop()

	return rn
}
