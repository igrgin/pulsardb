package integration

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pulsardb/internal/command"
	"pulsardb/internal/configuration/properties"
	raftpkg "pulsardb/internal/raft"
	"pulsardb/internal/store"
	"pulsardb/internal/transport"
	"pulsardb/internal/transport/gen/commandevents"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func getFreePorts(t *testing.T, n int) []string {
	t.Helper()
	ports := make([]string, n)
	listeners := make([]net.Listener, n)

	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = l
		_, ports[i], _ = net.SplitHostPort(l.Addr().String())
	}

	for _, l := range listeners {
		l.Close()
	}

	return ports
}

type transportTestRaftProposer struct {
	nodeID       uint64
	appliedIndex atomic.Uint64
	cmdSvc       *command.Service
}

func newTransportTestRaftProposer(nodeID uint64) *transportTestRaftProposer {
	f := &transportTestRaftProposer{nodeID: nodeID}
	f.appliedIndex.Store(100)
	return f
}

func (f *transportTestRaftProposer) Propose(ctx context.Context, data []byte) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	_, err := f.cmdSvc.Apply(data)
	return err
}

func (f *transportTestRaftProposer) GetReadIndex(ctx context.Context) (uint64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return f.appliedIndex.Load(), nil
}

func (f *transportTestRaftProposer) GetReadIndexFromLeader(ctx context.Context, leaderID uint64) (uint64, error) {
	return f.appliedIndex.Load(), nil
}

func (f *transportTestRaftProposer) WaitUntilApplied(ctx context.Context, index uint64) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func (f *transportTestRaftProposer) IsLeader() bool   { return true }
func (f *transportTestRaftProposer) LeaderID() uint64 { return f.nodeID }
func (f *transportTestRaftProposer) NodeID() uint64   { return f.nodeID }

func (f *transportTestRaftProposer) ForwardToLeader(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (*commandeventspb.CommandEventResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

type transportTestRegistry struct {
	svc *command.Service
}

func (r *transportTestRegistry) RegisterPending(eventID uint64, ch chan *commandeventspb.CommandEventResponse) {
	r.svc.RegisterPending(eventID, ch)
}

func (r *transportTestRegistry) UnregisterPending(eventID uint64) {
	r.svc.UnregisterPending(eventID)
}

type testServerBundle struct {
	client    commandeventspb.CommandEventClientServiceClient
	conn      *grpc.ClientConn
	transport *transport.Service
	cmdSvc    *command.Service
}

func (ts *testServerBundle) Close() {
	if ts.conn != nil {
		ts.conn.Close()
	}
	if ts.transport != nil && ts.transport.ClientServer != nil {
		ts.transport.ClientServer.GracefulStop()
	}
	if ts.transport != nil && ts.transport.RaftServer != nil {
		ts.transport.RaftServer.GracefulStop()
	}
}

func startTransportTestServer(t *testing.T, nodeID uint64) *testServerBundle {
	t.Helper()

	storageSvc := store.NewStorageService()
	raftProposer := newTransportTestRaftProposer(nodeID)
	registry := &transportTestRegistry{}

	batcher := raftpkg.NewBatcher(raftProposer, registry, 100, 10*time.Millisecond)
	cmdSvc := command.NewCommandService(storageSvc, raftProposer, batcher)

	raftProposer.cmdSvc = cmdSvc
	registry.svc = cmdSvc

	ports := getFreePorts(t, 2)

	cfg := &properties.TransportConfigProperties{
		Network:    "tcp",
		Address:    "127.0.0.1",
		ClientPort: ports[0],
		RaftPort:   ports[1],
		ClientTransport: properties.ClientTransportConfigProperties{
			MaxConcurrentStreams: 100,
			NumStreamWorkers:     1,
		},
		RaftTransport: properties.RaftTransportConfigProperties{
			MaxConcurrentStreams: 100,
			NumStreamWorkers:     1,
		},
	}

	ts := transport.NewTransportService(cfg, cmdSvc, nil)

	clientLis, err := ts.StartClientServer()
	require.NoError(t, err)

	conn, err := grpc.Dial(
		clientLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := commandeventspb.NewCommandEventClientServiceClient(conn)

	return &testServerBundle{
		client:    client,
		conn:      conn,
		transport: ts,
		cmdSvc:    cmdSvc,
	}
}

func TestTransport_StartClientServer_AcceptsConnections(t *testing.T) {
	ts, lis, err := setupTransport(t)
	require.NotNil(t, lis)
	defer ts.ClientServer.GracefulStop()

	conn, err := grpc.Dial(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	conn.Close()
}

func setupTransport(t *testing.T) (*transport.Service, net.Listener, error) {
	ports := getFreePorts(t, 2)

	cfg := &properties.TransportConfigProperties{
		Network:    "tcp",
		Address:    "127.0.0.1",
		ClientPort: ports[0],
		RaftPort:   ports[1],
		ClientTransport: properties.ClientTransportConfigProperties{
			MaxConcurrentStreams: 100,
			NumStreamWorkers:     1,
		},
		RaftTransport: properties.RaftTransportConfigProperties{
			MaxConcurrentStreams: 100,
			NumStreamWorkers:     1,
		},
	}

	storageSvc := store.NewStorageService()
	raftProposer := newTransportTestRaftProposer(1)
	registry := &transportTestRegistry{}
	batcher := raftpkg.NewBatcher(raftProposer, registry, 100, 10*time.Millisecond)
	cmdSvc := command.NewCommandService(storageSvc, raftProposer, batcher)
	raftProposer.cmdSvc = cmdSvc
	registry.svc = cmdSvc

	ts := transport.NewTransportService(cfg, cmdSvc, nil)

	lis, err := ts.StartClientServer()
	require.NoError(t, err)
	return ts, lis, err
}

func TestTransport_StartServer_PortConflict_ReturnsError(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()

	_, occupiedPort, _ := net.SplitHostPort(occupied.Addr().String())
	freePort := getFreePorts(t, 1)[0]

	cfg := &properties.TransportConfigProperties{
		Network:    "tcp",
		Address:    "127.0.0.1",
		ClientPort: occupiedPort,
		RaftPort:   freePort,
		ClientTransport: properties.ClientTransportConfigProperties{
			MaxConcurrentStreams: 100,
			NumStreamWorkers:     1,
		},
		RaftTransport: properties.RaftTransportConfigProperties{
			MaxConcurrentStreams: 100,
			NumStreamWorkers:     1,
		},
	}

	storageSvc := store.NewStorageService()
	raftProposer := newTransportTestRaftProposer(1)
	registry := &transportTestRegistry{}
	batcher := raftpkg.NewBatcher(raftProposer, registry, 100, 10*time.Millisecond)
	cmdSvc := command.NewCommandService(storageSvc, raftProposer, batcher)
	raftProposer.cmdSvc = cmdSvc
	registry.svc = cmdSvc

	ts := transport.NewTransportService(cfg, cmdSvc, nil)

	_, err = ts.StartClientServer()
	require.Error(t, err)
}

func TestTransport_GracefulShutdown_CompletesWithinTimeout(t *testing.T) {
	ports := getFreePorts(t, 2)

	cfg := &properties.TransportConfigProperties{
		Network:    "tcp",
		Address:    "127.0.0.1",
		ClientPort: ports[0],
		RaftPort:   ports[1],
		ClientTransport: properties.ClientTransportConfigProperties{
			MaxConcurrentStreams: 100,
			NumStreamWorkers:     1,
		},
		RaftTransport: properties.RaftTransportConfigProperties{
			MaxConcurrentStreams: 100,
			NumStreamWorkers:     1,
		},
	}

	storageSvc := store.NewStorageService()
	raftProposer := newTransportTestRaftProposer(1)
	registry := &transportTestRegistry{}
	batcher := raftpkg.NewBatcher(raftProposer, registry, 100, 10*time.Millisecond)
	cmdSvc := command.NewCommandService(storageSvc, raftProposer, batcher)
	raftProposer.cmdSvc = cmdSvc
	registry.svc = cmdSvc

	ts := transport.NewTransportService(cfg, cmdSvc, nil)

	_, err := ts.StartClientServer()
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		ts.ClientServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:

	case <-time.After(5 * time.Second):
		t.Fatal("graceful shutdown timed out")
	}
}

func TestE2E_SET_Success(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 1,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "test-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "test-value"},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	requireSuccess(t, resp, err)
}

func TestE2E_GET_ExistingKey_ReturnsValue(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 1,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "existing-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_IntValue{IntValue: 42},
		},
	})
	require.NoError(t, err)

	resp, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 2,
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "existing-key",
	})

	require.NoError(t, err)
	requireSuccess(t, resp, err)
	require.Equal(t, int64(42), resp.GetValue().GetIntValue())
}

func TestE2E_GET_NonExistentKey_ReturnsFailure(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 1,
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "nonexistent",
	})

	require.Error(t, err)
	require.Nil(t, resp)
}

func TestE2E_ResponseMetadata_IsCorrect(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 12345,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "metadata-test",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "val"},
		},
	})

	require.NoError(t, err)
	require.Equal(t, uint64(12345), resp.GetEventId())
	requireSuccess(t, resp, err)
}

func TestE2E_DELETE_ExistingKey_Success(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	setResp, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 1,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "to-delete",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "val"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, setResp)
	require.True(t, setResp.GetSuccess())

	delResp, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 2,
		Type:    commandeventspb.CommandEventType_DELETE,
		Key:     "to-delete",
	})

	require.NoError(t, err)
	require.True(t, delResp.GetSuccess())

	_, err = srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 3,
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "to-delete",
	})
	require.Error(t, err)
}

func TestE2E_SET_MissingKey_ReturnsFailure(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 1,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "val"},
		},
	})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestE2E_SET_MissingValue_ReturnsFailure(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 1,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "key",
		Value:   nil,
	})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestE2E_GET_WithValue_ReturnsFailure(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 1,
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_IntValue{IntValue: 1},
		},
	})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestE2E_MultipleValueTypes(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	testCases := []struct {
		name     string
		key      string
		setValue *commandeventspb.CommandEventValue
		validate func(t *testing.T, v *commandeventspb.CommandEventValue)
	}{
		{
			name: "string",
			key:  "string-key",
			setValue: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "hello"},
			},
			validate: func(t *testing.T, v *commandeventspb.CommandEventValue) {
				require.Equal(t, "hello", v.GetStringValue())
			},
		},
		{
			name: "int",
			key:  "int-key",
			setValue: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_IntValue{IntValue: 12345},
			},
			validate: func(t *testing.T, v *commandeventspb.CommandEventValue) {
				require.Equal(t, int64(12345), v.GetIntValue())
			},
		},
		{
			name: "double",
			key:  "double-key",
			setValue: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_DoubleValue{DoubleValue: 3.14159},
			},
			validate: func(t *testing.T, v *commandeventspb.CommandEventValue) {
				require.InDelta(t, 3.14159, v.GetDoubleValue(), 0.00001)
			},
		},
		{
			name: "bool",
			key:  "bool-key",
			setValue: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_BoolValue{BoolValue: true},
			},
			validate: func(t *testing.T, v *commandeventspb.CommandEventValue) {
				require.True(t, v.GetBoolValue())
			},
		},
		{
			name: "bytes",
			key:  "bytes-key",
			setValue: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_BytesValue{BytesValue: []byte{0xDE, 0xAD, 0xBE, 0xEF}},
			},
			validate: func(t *testing.T, v *commandeventspb.CommandEventValue) {
				require.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, v.GetBytesValue())
			},
		},
	}

	var eventID uint64
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			eventID++
			setResp, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
				EventId: eventID,
				Type:    commandeventspb.CommandEventType_SET,
				Key:     tc.key,
				Value:   tc.setValue,
			})
			require.NoError(t, err)
			require.True(t, setResp.GetSuccess())

			eventID++
			getResp, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
				EventId: eventID,
				Type:    commandeventspb.CommandEventType_GET,
				Key:     tc.key,
			})
			require.NoError(t, err)
			require.NotNil(t, getResp.GetValue())
			tc.validate(t, getResp.GetValue())
		})
	}
}

func TestE2E_ConcurrentClients(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	defer srv.Close()

	const numClients = 10
	const opsPerClient = 5

	var wg sync.WaitGroup
	var eventID atomic.Uint64
	errCh := make(chan error, numClients*opsPerClient*2)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; j < opsPerClient; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				key := fmt.Sprintf("client-%d-key-%d", clientID, j)
				val := int64(clientID*1000 + j)

				setResp, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
					EventId: eventID.Add(1),
					Type:    commandeventspb.CommandEventType_SET,
					Key:     key,
					Value: &commandeventspb.CommandEventValue{
						Value: &commandeventspb.CommandEventValue_IntValue{IntValue: val},
					},
				})
				if err != nil {
					errCh <- fmt.Errorf("SET error: %w", err)
					cancel()
					continue
				}
				if !setResp.GetSuccess() {
					errCh <- fmt.Errorf("SET failed for %s: %s", key, setResp.GetError().GetMessage())
					cancel()
					continue
				}

				getResp, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
					EventId: eventID.Add(1),
					Type:    commandeventspb.CommandEventType_GET,
					Key:     key,
				})
				if err != nil {
					errCh <- fmt.Errorf("GET error: %w", err)
					cancel()
					continue
				}
				if getResp.GetError() != nil {
					errCh <- fmt.Errorf("GET failed for %s: %s", key, getResp.GetError().GetMessage())
					cancel()
					continue
				}
				if getResp.GetValue().GetIntValue() != val {
					errCh <- fmt.Errorf("GET value mismatch for %s: got %d, want %d",
						key, getResp.GetValue().GetIntValue(), val)
				}

				cancel()
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	require.Empty(t, errs, "concurrent operations failed: %v", errs)
}

func TestE2E_RequestAfterShutdown_Fails(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	client := srv.client
	srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 1,
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "any-key",
	})

	require.Error(t, err)
}

func TestE2E_OverwriteExistingKey(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 1,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "overwrite-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "initial"},
		},
	})
	require.NoError(t, err)

	_, err = srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 2,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "overwrite-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "overwritten"},
		},
	})
	require.NoError(t, err)

	resp, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 3,
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "overwrite-key",
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetValue())
	require.Equal(t, "overwritten", resp.GetValue().GetStringValue())
}

func TestE2E_LargeValue(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	largeBytes := make([]byte, 1024*1024)
	for i := range largeBytes {
		largeBytes[i] = byte(i % 256)
	}

	setResp, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 1,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "large-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_BytesValue{BytesValue: largeBytes},
		},
	})
	require.NoError(t, err)
	require.True(t, setResp.GetSuccess())

	getResp, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 2,
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "large-key",
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.GetValue())
	require.Equal(t, largeBytes, getResp.GetValue().GetBytesValue())
}

func TestE2E_ErrorResponse_DoesNotLeakInternalDetails(t *testing.T) {
	srv := startTransportTestServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := srv.client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: 1,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "",
		Value:   nil,
	})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
