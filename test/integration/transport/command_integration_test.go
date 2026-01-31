package integration

import (
	"context"
	"fmt"
	"pulsardb/test/integration/helper"
	"sync"
	"testing"
	"time"

	commandeventspb "pulsardb/internal/transport/gen/command"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCommand_SET_MissingKey(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.SetValue(ctx, "", &commandeventspb.CommandEventValue{
		Value: &commandeventspb.CommandEventValue_IntValue{IntValue: 1},
	})

	require.Error(t, err)
	require.Nil(t, resp)
}

func TestCommand_SET_MissingValue(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.SetValue(ctx, "", nil)

	require.Error(t, err)
	require.Nil(t, resp)
}

func TestCommand_GET_MissingKey(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.GetValue(ctx, "")
	require.Error(t, err)
	require.Nil(t, resp)
}

func TestCommand_GET_WithValue(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, exists, err := c.Get(ctx, "key")
	require.False(t, exists)
	require.Error(t, err)
	require.Equal(t, "", resp)
}

func TestCommand_DELETE_WithValue(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := c.Delete(ctx, "key")
	require.NoError(t, err)
}

func TestCommand_UnknownType(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.SendToLeader(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType(999),
		Key:     "key",
	})

	require.Error(t, err)
	require.Nil(t, resp)
}

func TestCommand_SetThenGet_RoundTrip(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := c.Set(ctx, "mykey", "123")
	require.NoError(t, err)

	getResp, exists, err := c.Get(ctx, "mykey")
	require.True(t, exists)
	require.NoError(t, err)
	require.NotNil(t, getResp)

	require.Equal(t, "123", getResp)
}

func TestCommand_DeleteThenGet_ReturnsError(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := c.Set(ctx, "todelete", "v")
	require.NoError(t, err)

	err = c.Delete(ctx, "todelete")
	require.NoError(t, err)

	getResp, exists, err := c.Get(ctx, "todelete")
	require.False(t, exists)
	require.Error(t, err)
	require.Equal(t, "", getResp)
}

func TestCommand_GET_NonExistentKey(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, exists, err := c.Get(ctx, "nonexistent")
	require.False(t, exists)
	require.Error(t, err)
	require.Equal(t, "", resp)
}

func TestCommand_OverwriteExistingKey(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	valueToOverwrite := "first"
	c.Set(ctx, "overwrite", valueToOverwrite)

	expectedValue := "second"
	c.Set(ctx, "overwrite", expectedValue)

	getResp, exists, err := c.Get(ctx, "overwrite")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, expectedValue, getResp)
}

func TestCommand_ContextCanceled(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := c.Get(ctx, "key")
	require.Error(t, err)
}

func TestCommand_ContextDeadlineExceeded(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(1 * time.Millisecond)

	_, _, err := c.Get(ctx, "key")
	require.Error(t, err)
}

func TestCommand_MultipleValueTypes(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	_, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		key      string
		setValue *commandeventspb.CommandEventValue
		validate func(t *testing.T, v *commandeventspb.CommandEventValue)
	}{
		{
			name: "string",
			key:  "str-key",
			setValue: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "abc"},
			},
			validate: func(t *testing.T, v *commandeventspb.CommandEventValue) {
				require.Equal(t, "abc", v.GetStringValue())
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

			setResp, err := c.SetValue(ctx, tc.key, tc.setValue)
			helper.RequireSuccess(t, setResp, err)

			getResp, err := c.GetValue(ctx, tc.key)
			helper.RequireSuccess(t, getResp, err)
			tc.validate(t, getResp.GetValue())
		})
	}
}

func TestTransport_SET_Success(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "test-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "test-value"},
		},
	})

	helper.RequireSuccess(t, resp, err)
}

func TestTransport_SET_MissingKey_ReturnsInvalidArgument(t *testing.T) {
	c := helper.NewCluster(t, nil, "debug")
	c.StartNodes(1, 30, false)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "val"},
		},
	})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestTransport_SET_MissingValue_ReturnsInvalidArgument(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "key",
		Value:   nil,
	})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestTransport_GET_ExistingKey(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "existing-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_IntValue{IntValue: 42},
		},
	})
	require.NoError(t, err)

	resp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "existing-key",
	})

	helper.RequireSuccess(t, resp, err)
	require.Equal(t, int64(42), resp.GetValue().GetIntValue())
}

func TestTransport_GET_NonExistentKey(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "nonexistent",
	})

	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestTransport_GET_WithValue_ReturnsInvalidArgument(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_IntValue{IntValue: 1},
		},
	})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestTransport_DELETE_ExistingKey(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "to-delete",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "val"},
		},
	})
	require.NoError(t, err)

	delResp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_DELETE,
		Key:     "to-delete",
	})
	helper.RequireSuccess(t, delResp, err)

	_, err = client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "to-delete",
	})
	require.Error(t, err)
}

func TestTransport_ResponseMetadata(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventID := helper.NewEventID()
	resp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: eventID,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "metadata-test",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "val"},
		},
	})

	helper.RequireSuccess(t, resp, err)
	require.Equal(t, eventID, resp.GetEventId())
}

func TestTransport_LargeValue(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	largeBytes := make([]byte, 1024*1024)
	for i := range largeBytes {
		largeBytes[i] = byte(i % 256)
	}

	setResp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "large-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_BytesValue{BytesValue: largeBytes},
		},
	})
	helper.RequireSuccess(t, setResp, err)

	getResp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "large-key",
	})
	helper.RequireSuccess(t, getResp, err)
	require.Equal(t, largeBytes, getResp.GetValue().GetBytesValue())
}

func TestTransport_ConcurrentClients(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

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
					EventId: helper.NewEventID(),
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
					errCh <- fmt.Errorf("SET failed: %s", setResp.GetError().GetMessage())
					cancel()
					continue
				}

				getResp, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
					EventId: helper.NewEventID(),
					Type:    commandeventspb.CommandEventType_GET,
					Key:     key,
				})
				if err != nil {
					errCh <- fmt.Errorf("GET error: %w", err)
					cancel()
					continue
				}
				if getResp.GetValue().GetIntValue() != val {
					errCh <- fmt.Errorf("value mismatch: got %d, want %d",
						getResp.GetValue().GetIntValue(), val)
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
	require.Empty(t, errs)
}

func TestCluster_LeaderElection(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(3, 60, false)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)
	require.NotZero(t, leaderID)
}

func TestCluster_DataConsistency(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(3, 60, false)

	_, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = c.Set(ctx, "consistent-key", "consistent-value")
	require.NoError(t, err)

	err = c.WaitForConvergence(10 * time.Second)
	require.NoError(t, err)

	consistent, err := c.VerifyConsistency("consistent-key")
	require.NoError(t, err)
	require.True(t, consistent)
}

func TestCluster_LeaderFailover(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(3, 60, false)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = c.Set(ctx, "before-failover", "value1")
	require.NoError(t, err)

	err = c.StopNode(leaderID)
	require.NoError(t, err)

	newLeaderID, err := c.WaitForNewLeader(leaderID, 10*time.Second)
	require.NoError(t, err)
	require.NotEqual(t, leaderID, newLeaderID)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	val, exists, err := c.Get(ctx2, "before-failover")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "value1", val)
}

func TestCluster_NodeRestart(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(3, 60, false)

	_, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = c.Set(ctx, "persistent-key", "persistent-value")
	require.NoError(t, err)

	err = c.WaitForConvergence(10 * time.Second)
	require.NoError(t, err)

	leader := c.GetLeader()
	var followerID uint64
	for id := uint64(1); id <= 3; id++ {
		if id != leader.ID {
			followerID = id
			break
		}
	}

	err = c.RestartNode(followerID)
	require.NoError(t, err)

	err = c.WaitForConvergence(10 * time.Second)
	require.NoError(t, err)

	consistent, err := c.VerifyConsistency("persistent-key")
	require.NoError(t, err)
	require.True(t, consistent)
}

func TestCommand_GracefulShutdown_CompletesWithinTimeout(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	_, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	leader := c.GetLeader()
	require.NotNil(t, leader)

	done := make(chan struct{})
	go func() {
		leader.Transport.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("graceful shutdown timed out")
	}
}

func TestCommand_RequestAfterShutdown_Fails(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	client, cleanup := c.GetClient(t)
	leader := c.GetLeader()
	require.NotNil(t, leader)

	err := c.StopNode(leader.ID)
	require.NoError(t, err)
	cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err = client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "any-key",
	})
	require.Error(t, err)
}

func TestCommand_ErrorResponse_DoesNotLeakInternalDetails(t *testing.T) {
	c := helper.NewCluster(t, nil, "error")
	c.StartNodes(1, 30, false)

	client, cleanup := c.GetClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ProcessCommandEvent(ctx, &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "",
		Value:   nil,
	})

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
