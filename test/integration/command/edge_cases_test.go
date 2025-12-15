package integration

import (
	"context"
	commandeventspb "pulsardb/internal/transport/gen/commandevents"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCmdService_SpecialKeys(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

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
			require.NoError(t, cluster.SetValue(ctx, key, "value"))

			val, exists, err := cluster.GetValue(ctx, key)
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, "value", val)
		})
	}
}

func TestCmdService_EmptyStringValue(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, cluster.SetValue(ctx, "empty", ""))

	val, exists, err := cluster.GetValue(ctx, "empty")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "", val)
}

func TestCmdService_CrossNodeConsistency(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	require.NoError(t, cluster.SetValue(ctx, "consistent-key", "consistent-value"))
	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	consistent, err := cluster.VerifyConsistency("consistent-key")
	require.NoError(t, err)
	require.True(t, consistent)
}

func TestCmdService_ReadFromFollower(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	require.NoError(t, cluster.SetValue(ctx, "follower-read", "test-value"))
	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	followers := cluster.GetFollowers()
	require.NotEmpty(t, followers)

	follower := followers[0]

	req := &commandeventspb.CommandEventRequest{
		EventId: uint64(time.Now().UnixNano()),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "follower-read",
	}

	resp, err := follower.CmdService.ProcessCommand(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Success)
	require.Equal(t, "test-value", resp.GetValue().GetStringValue())
}

func TestCmdService_LongKey(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	longKey := ""
	for i := 0; i < 1000; i++ {
		longKey += "a"
	}

	require.NoError(t, cluster.SetValue(ctx, longKey, "value"))

	val, exists, err := cluster.GetValue(ctx, longKey)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "value", val)
}

func TestCmdService_ZeroEventID(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

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

	resp, err := cluster.ProcessCommand(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Success)
}

func TestCmdService_SameKeyMultipleNodes(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	key := "shared-key"

	require.NoError(t, cluster.SetValue(ctx, key, "leader-write"))
	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	cluster.mu.RLock()
	for _, node := range cluster.nodes {
		if node.stopped {
			continue
		}
		val, exists := node.StorageService.Get(key)
		require.True(t, exists, "key should exist on node %d", node.ID)
		require.Equal(t, "leader-write", val, "value mismatch on node %d", node.ID)
	}
	cluster.mu.RUnlock()
}

func TestCmdService_DeleteThenRecreate(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	key := "recreate-key"

	require.NoError(t, cluster.SetValue(ctx, key, "first"))
	val, exists, err := cluster.GetValue(ctx, key)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "first", val)

	require.NoError(t, cluster.DeleteValue(ctx, key))
	_, exists, err = cluster.GetValue(ctx, key)
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, cluster.SetValue(ctx, key, "second"))
	val, exists, err = cluster.GetValue(ctx, key)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "second", val)
}

func TestCmdService_SpecialStringValues(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

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
			require.NoError(t, cluster.SetValue(ctx, key, value))
			val, exists, err := cluster.GetValue(ctx, key)
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, value, val)
		})
	}
}
