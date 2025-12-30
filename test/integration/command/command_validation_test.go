package integration

import (
	"context"
	"pulsardb/internal/transport/gen/commandevents"
	"pulsardb/test/integration/helper"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCmdService_ValidationErrors(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	testCases := []struct {
		name string
		req  *commandeventspb.CommandEventRequest
	}{
		{
			name: "SET without key",
			req: &commandeventspb.CommandEventRequest{
				EventId: helper.NewEventID(),
				Type:    commandeventspb.CommandEventType_SET,
				Key:     "",
				Value: &commandeventspb.CommandEventValue{
					Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "value"},
				},
			},
		},
		{
			name: "SET without value",
			req: &commandeventspb.CommandEventRequest{
				EventId: helper.NewEventID(),
				Type:    commandeventspb.CommandEventType_SET,
				Key:     "key",
				Value:   nil,
			},
		},
		{
			name: "GET without key",
			req: &commandeventspb.CommandEventRequest{
				EventId: helper.NewEventID(),
				Type:    commandeventspb.CommandEventType_GET,
				Key:     "",
			},
		},
		{
			name: "GET with value",
			req: &commandeventspb.CommandEventRequest{
				EventId: helper.NewEventID(),
				Type:    commandeventspb.CommandEventType_GET,
				Key:     "key",
				Value: &commandeventspb.CommandEventValue{
					Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "should not have"},
				},
			},
		},
		{
			name: "DELETE without key",
			req: &commandeventspb.CommandEventRequest{
				EventId: helper.NewEventID(),
				Type:    commandeventspb.CommandEventType_DELETE,
				Key:     "",
			},
		},
		{
			name: "DELETE with value",
			req: &commandeventspb.CommandEventRequest{
				EventId: helper.NewEventID(),
				Type:    commandeventspb.CommandEventType_DELETE,
				Key:     "key",
				Value: &commandeventspb.CommandEventValue{
					Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "should not have"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := cluster.SendToLeader(ctx, tc.req)
			require.Error(t, err)
		})
	}
}

func TestCmdService_ContextTimeout(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(1 * time.Millisecond)

	err := cluster.Set(ctx, "timeout-key", "value")
	require.Error(t, err)
}

func TestCmdService_ContextCancellation(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := cluster.Set(ctx, "cancelled-key", "value")
	require.Error(t, err)
}

func TestCmdService_UnknownCommandType(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType(999),
		Key:     "key",
	}

	_, err := cluster.SendToLeader(ctx, req)
	require.Error(t, err)
}
