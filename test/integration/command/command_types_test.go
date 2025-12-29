package integration

import (
	"context"
	"testing"
	"time"

	"pulsardb/internal/transport/gen/commandevents"

	"github.com/stretchr/testify/require"
)

func TestCmdService_MultipleDataTypes(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	testCases := []struct {
		name  string
		key   string
		value *commandeventspb.CommandEventValue
		check func(t *testing.T, resp *commandeventspb.CommandEventResponse)
	}{
		{
			name: "string",
			key:  "type-string",
			value: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "hello"},
			},
			check: func(t *testing.T, resp *commandeventspb.CommandEventResponse) {
				require.Equal(t, "hello", resp.GetValue().GetStringValue())
			},
		},
		{
			name: "int",
			key:  "type-int",
			value: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_IntValue{IntValue: 42},
			},
			check: func(t *testing.T, resp *commandeventspb.CommandEventResponse) {
				require.Equal(t, int64(42), resp.GetValue().GetIntValue())
			},
		},
		{
			name: "negative int",
			key:  "type-neg-int",
			value: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_IntValue{IntValue: -12345},
			},
			check: func(t *testing.T, resp *commandeventspb.CommandEventResponse) {
				require.Equal(t, int64(-12345), resp.GetValue().GetIntValue())
			},
		},
		{
			name: "double",
			key:  "type-double",
			value: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_DoubleValue{DoubleValue: 3.14159},
			},
			check: func(t *testing.T, resp *commandeventspb.CommandEventResponse) {
				require.InDelta(t, 3.14159, resp.GetValue().GetDoubleValue(), 0.00001)
			},
		},
		{
			name: "bool true",
			key:  "type-bool-true",
			value: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_BoolValue{BoolValue: true},
			},
			check: func(t *testing.T, resp *commandeventspb.CommandEventResponse) {
				require.True(t, resp.GetValue().GetBoolValue())
			},
		},
		{
			name: "bool false",
			key:  "type-bool-false",
			value: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_BoolValue{BoolValue: false},
			},
			check: func(t *testing.T, resp *commandeventspb.CommandEventResponse) {
				require.False(t, resp.GetValue().GetBoolValue())
			},
		},
		{
			name: "bytes",
			key:  "type-bytes",
			value: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_BytesValue{BytesValue: []byte{0xDE, 0xAD, 0xBE, 0xEF}},
			},
			check: func(t *testing.T, resp *commandeventspb.CommandEventResponse) {
				require.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, resp.GetValue().GetBytesValue())
			},
		},
		{
			name: "empty bytes",
			key:  "type-empty-bytes",
			value: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_BytesValue{BytesValue: []byte{}},
			},
			check: func(t *testing.T, resp *commandeventspb.CommandEventResponse) {
				require.Empty(t, resp.GetValue().GetBytesValue())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setReq := &commandeventspb.CommandEventRequest{
				EventId: newEventID(),
				Type:    commandeventspb.CommandEventType_SET,
				Key:     tc.key,
				Value:   tc.value,
			}
			resp, err := cluster.ProcessCommand(ctx, setReq)
			require.NoError(t, err)
			require.True(t, resp.Success)

			getReq := &commandeventspb.CommandEventRequest{
				EventId: newEventID(),
				Type:    commandeventspb.CommandEventType_GET,
				Key:     tc.key,
			}
			resp, err = cluster.ProcessCommand(ctx, getReq)
			require.NoError(t, err)
			require.True(t, resp.Success)
			tc.check(t, resp)
		})
	}
}

func TestCmdService_LargeValue(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	largeValue := make([]byte, 32*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	req := &commandeventspb.CommandEventRequest{
		EventId: newEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "large-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_BytesValue{BytesValue: largeValue},
		},
	}

	resp, err := cluster.ProcessCommand(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Success)

	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	consistent, err := cluster.VerifyConsistency("large-key")
	require.NoError(t, err)
	require.True(t, consistent)
}

func TestCmdService_TypeOverwrite(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	key := "type-overwrite"

	req := &commandeventspb.CommandEventRequest{
		EventId: newEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     key,
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "hello"},
		},
	}
	resp, err := cluster.ProcessCommand(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Success)

	req = &commandeventspb.CommandEventRequest{
		EventId: newEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     key,
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_IntValue{IntValue: 42},
		},
	}
	resp, err = cluster.ProcessCommand(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Success)

	getReq := &commandeventspb.CommandEventRequest{
		EventId: newEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     key,
	}
	resp, err = cluster.ProcessCommand(ctx, getReq)
	require.NoError(t, err)
	require.True(t, resp.Success)
	require.Equal(t, int64(42), resp.GetValue().GetIntValue())
}
