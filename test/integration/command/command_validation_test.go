package integration

import (
	"context"
	"testing"
	"time"

	"pulsardb/internal/transport/gen/commandevents"
	"pulsardb/internal/types"

	"github.com/stretchr/testify/require"
)

func TestCmdService_ValidationErrors(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	testCases := []struct {
		name string
		req  *commandeventspb.CommandEventRequest
	}{
		{
			name: "SET without key",
			req: &commandeventspb.CommandEventRequest{
				EventId: 1,
				Type:    commandeventspb.CommandEventType_SET,
				Key:     "",
				Value:   types.ValueToProto("value"),
			},
		},
		{
			name: "SET without value",
			req: &commandeventspb.CommandEventRequest{
				EventId: 2,
				Type:    commandeventspb.CommandEventType_SET,
				Key:     "key",
				Value:   nil,
			},
		},
		{
			name: "GET without key",
			req: &commandeventspb.CommandEventRequest{
				EventId: 3,
				Type:    commandeventspb.CommandEventType_GET,
				Key:     "",
			},
		},
		{
			name: "GET with value",
			req: &commandeventspb.CommandEventRequest{
				EventId: 4,
				Type:    commandeventspb.CommandEventType_GET,
				Key:     "key",
				Value:   types.ValueToProto("should not have"),
			},
		},
		{
			name: "DELETE without key",
			req: &commandeventspb.CommandEventRequest{
				EventId: 5,
				Type:    commandeventspb.CommandEventType_DELETE,
				Key:     "",
			},
		},
		{
			name: "DELETE with value",
			req: &commandeventspb.CommandEventRequest{
				EventId: 6,
				Type:    commandeventspb.CommandEventType_DELETE,
				Key:     "key",
				Value:   types.ValueToProto("should not have"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := cluster.ProcessCommand(ctx, tc.req)
			require.Error(t, err)
			require.False(t, resp.Success)
		})
	}
}

func TestCmdService_ContextTimeout(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(1 * time.Millisecond)

	err = cluster.SetValue(ctx, "timeout-key", "value")
	require.Error(t, err)
}

func TestCmdService_ContextCancellation(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = cluster.SetValue(ctx, "cancelled-key", "value")
	require.Error(t, err)
}

func TestCmdService_UnknownCommandType(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &commandeventspb.CommandEventRequest{
		EventId: 100,
		Type:    commandeventspb.CommandEventType(999),
		Key:     "key",
	}

	resp, err := cluster.ProcessCommand(ctx, req)
	require.Error(t, err)
	require.False(t, resp.Success)
}
