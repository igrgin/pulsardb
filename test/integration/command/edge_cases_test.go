package integration

import (
	"context"
	commandeventspb "pulsardb/internal/transport/gen/command"
	"pulsardb/test/integration/helper"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCmdService_SpecialKeys(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10, false)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	keys := []string{
		"key with spaces",
		"key/with/slashes",
		"key:colons",
		"key.dots",
		"key-dashes",
		"key_underscores",
		"キー日本語",
		"🔑emoji",
		"key\twith\ttabs",
		"key\nwith\nnewlines",
	}

	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			require.NoError(t, cluster.Set(ctx, key, "value"))

			val, exists, err := cluster.Get(ctx, key)
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, "value", val)
		})
	}
}

func TestCmdService_CrossNodeConsistency(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10, false)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	require.NoError(t, cluster.Set(ctx, "consistent-key", "consistent-value"))
	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	consistent, err := cluster.VerifyConsistency("consistent-key")
	require.NoError(t, err)
	require.True(t, consistent)
}

func TestCmdService_ReadFromFollower_IsLeasedBasedTrue(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10, true)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	require.NoError(t, cluster.Set(ctx, "follower-read", "test-value"))
	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	followers := cluster.GetFollowers()
	require.NotEmpty(t, followers)

	follower := followers[0]

	req := &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "follower-read",
	}

	_, err := cluster.SendToNode(ctx, follower.ID, req)
	require.Error(t, err)
	require.ErrorContains(t, err, "operation must be performed on the leader")
}

func TestCmdService_ReadFromFollower_IsLeasedBasedFalse(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10, false)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	require.NoError(t, cluster.Set(ctx, "follower-read", "test-value"))
	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	followers := cluster.GetFollowers()
	require.NotEmpty(t, followers)

	follower := followers[0]

	req := &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "follower-read",
	}

	resp, err := cluster.SendToNode(ctx, follower.ID, req)
	require.NoError(t, err)
	require.True(t, resp.Success)
	require.Equal(t, "test-value", resp.GetValue().GetStringValue())
}

func TestCmdService_ReadFromLeader_IsLeasedBasedTrue(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10, true)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	require.NoError(t, cluster.Set(ctx, "follower-read", "test-value"))
	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	req := &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "follower-read",
	}

	resp, err := cluster.SendToLeader(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Success)
	require.Equal(t, "test-value", resp.GetValue().GetStringValue())
}

func TestCmdService_ReadFromLeader_IsLeasedBasedFalse(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10, false)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	require.NoError(t, cluster.Set(ctx, "follower-read", "test-value"))
	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	req := &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "follower-read",
	}

	resp, err := cluster.SendToLeader(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Success)
	require.Equal(t, "test-value", resp.GetValue().GetStringValue())
}

func TestCmdService_LongKey(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10, false)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	longKey := strings.Repeat("a", 1000)

	require.NoError(t, cluster.Set(ctx, longKey, "value"))

	val, exists, err := cluster.Get(ctx, longKey)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "value", val)
}

func TestCmdService_ZeroEventID(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10, false)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &commandeventspb.CommandEventRequest{
		EventId: 0,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "zero-event-id",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "value"},
		},
	}

	resp, err := cluster.SendToLeader(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Success)
}

func TestCmdService_SameKeyMultipleNodes(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10, false)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	key := "shared-key"

	require.NoError(t, cluster.Set(ctx, key, "leader-write"))
	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	for id := uint64(1); id <= 3; id++ {
		node := cluster.GetNode(id)
		require.NotNil(t, node, "node %d should exist", id)

		val, exists := node.StorageService.Get(key)
		require.True(t, exists, "key should exist on node %d", id)
		require.Equal(t, "leader-write", val, "value mismatch on node %d", id)
	}
}

func TestCmdService_DeleteThenRecreate(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10, false)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	key := "recreate-key"

	require.NoError(t, cluster.Set(ctx, key, "first"))
	val, exists, err := cluster.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "first", val)

	err = cluster.Delete(ctx, key)
	require.NoError(t, err)
	_, exists, err = cluster.Get(ctx, key)
	require.Error(t, err)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok, "expected gRPC status error, got: %v", err)
	require.Equal(t, codes.NotFound, st.Code())
	require.Contains(t, st.Message(), "key not found")

	require.False(t, exists)

	require.NoError(t, cluster.Set(ctx, key, "second"))
	val, exists, err = cluster.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "second", val)
}

func TestCmdService_SpecialStringValues(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10, false)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	values := []string{
		"",
		" ",
		"\t",
		"\n",
		"null",
		"undefined",
		"true",
		"false",
		"0",
		"-1",
		"日本語テキスト",
		"emoji 🎉🎊🎁",
		`{"json": "value"}`,
		`<xml>value</xml>`,
	}

	for i, value := range values {
		key := string(rune('a' + i))
		t.Run(key, func(t *testing.T) {
			require.NoError(t, cluster.Set(ctx, key, value))
			val, exists, err := cluster.Get(ctx, key)
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, value, val)
		})
	}
}
