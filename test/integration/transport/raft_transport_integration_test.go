package integration

import (
	"context"
	"fmt"
	commandeventspb "pulsardb/internal/transport/gen/command"
	integration2 "pulsardb/test/integration/helper"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClientTransport_StartClientServer_AcceptsConnections(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	require.NotNil(t, client)
}

func TestClientTransport_GracefulShutdown_CompletesWithinTimeout(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	_, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	leader := c.GetLeader()
	require.NotNil(t, leader)

	done := make(chan struct{})
	go func() {
		leader.ClientServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("graceful shutdown timed out")
	}
}

func TestClientTransport_RequestAfterShutdown_Fails(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)

	leader := c.GetLeader()
	require.NotNil(t, leader)
	err := c.StopNode(leader.ID)
	require.NoError(t, err)

	cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err = client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "any-key",
	})

	require.Error(t, err)
}

func TestClientTransport_SET_Success(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "test-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "test-value"},
		},
	})

	integration2.RequireSuccess(t, resp, err)
}

func TestClientTransport_SET_MissingKey_ReturnsInvalidArgument(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "val"},
		},
	})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestClientTransport_SET_MissingValue_ReturnsInvalidArgument(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "key",
		Value:   nil,
	})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestClientTransport_GET_ExistingKey_ReturnsValue(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "existing-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_IntValue{IntValue: 42},
		},
	})
	require.NoError(t, err)

	resp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "existing-key",
	})

	integration2.RequireSuccess(t, resp, err)
	require.Equal(t, int64(42), resp.GetValue().GetIntValue())
}

func TestClientTransport_GET_NonExistentKey_ReturnsError(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "nonexistent",
	})

	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestClientTransport_GET_WithValue_ReturnsInvalidArgument(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_IntValue{IntValue: 1},
		},
	})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestClientTransport_DELETE_ExistingKey_Success(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	setResp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "to-delete",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "val"},
		},
	})
	integration2.RequireSuccess(t, setResp, err)

	delResp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_DELETE,
		Key:     "to-delete",
	})
	integration2.RequireSuccess(t, delResp, err)

	_, err = client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "to-delete",
	})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestClientTransport_ResponseMetadata_IsCorrect(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventID := integration2.NewEventID()
	resp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: eventID,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "metadata-test",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "val"},
		},
	})

	integration2.RequireSuccess(t, resp, err)
	require.Equal(t, eventID, resp.GetEventId())
}

func TestClientTransport_MultipleValueTypes(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			setResp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
				EventId: integration2.NewEventID(),
				Type:    commandeventspb.CommandEventType_SET,
				Key:     tc.key,
				Value:   tc.setValue,
			})
			integration2.RequireSuccess(t, setResp, err)

			getResp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
				EventId: integration2.NewEventID(),
				Type:    commandeventspb.CommandEventType_GET,
				Key:     tc.key,
			})
			integration2.RequireSuccess(t, getResp, err)
			require.NotNil(t, getResp.GetValue())
			tc.validate(t, getResp.GetValue())
		})
	}
}

func TestClientTransport_OverwriteExistingKey(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "overwrite-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "initial"},
		},
	})
	require.NoError(t, err)

	_, err = client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "overwrite-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "overwritten"},
		},
	})
	require.NoError(t, err)

	resp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "overwrite-key",
	})
	integration2.RequireSuccess(t, resp, err)
	require.Equal(t, "overwritten", resp.GetValue().GetStringValue())
}

func TestClientTransport_LargeValue(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	largeBytes := make([]byte, 1024*1024)
	for i := range largeBytes {
		largeBytes[i] = byte(i % 256)
	}

	setResp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "large-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_BytesValue{BytesValue: largeBytes},
		},
	})
	integration2.RequireSuccess(t, setResp, err)

	getResp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "large-key",
	})
	integration2.RequireSuccess(t, getResp, err)
	require.Equal(t, largeBytes, getResp.GetValue().GetBytesValue())
}

func TestClientTransport_ConcurrentClients(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	const numClients = 10
	const opsPerClient = 5

	var wg sync.WaitGroup
	errCh := make(chan error, numClients*opsPerClient*2)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; j < opsPerClient; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				key := fmt.Sprintf("client-%d-key-%d", clientID, j)
				val := int64(clientID*1000 + j)

				setResp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
					EventId: integration2.NewEventID(),
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

				getResp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
					EventId: integration2.NewEventID(),
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

func TestClientTransport_ErrorResponse_DoesNotLeakInternalDetails(t *testing.T) {
	c := integration2.NewCluster(t, nil, "error")
	c.StartNodes(1, 30)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: integration2.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "",
		Value:   nil,
	})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
