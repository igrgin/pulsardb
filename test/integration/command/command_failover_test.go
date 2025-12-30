package integration

import (
	"context"
	"fmt"
	"pulsardb/test/integration/helper"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCmdService_LeaderFailover(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10)

	leaderID, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		require.NoError(t, cluster.Set(ctx, fmt.Sprintf("pre-%d", i), fmt.Sprintf("val-%d", i)))
	}
	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	require.NoError(t, cluster.StopNode(leaderID))
	t.Logf("stopped leader %d", leaderID)

	newLeaderID, err := cluster.WaitForNewLeader(leaderID, 15*time.Second)
	require.NoError(t, err)
	t.Logf("new leader: %d", newLeaderID)

	for i := 0; i < 5; i++ {
		require.NoError(t, cluster.Set(ctx, fmt.Sprintf("post-%d", i), fmt.Sprintf("newval-%d", i)))
	}
	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	for i := 0; i < 5; i++ {
		consistent, err := cluster.VerifyConsistency(fmt.Sprintf("pre-%d", i))
		require.NoError(t, err)
		require.True(t, consistent)
	}

	for i := 0; i < 5; i++ {
		consistent, err := cluster.VerifyConsistency(fmt.Sprintf("post-%d", i))
		require.NoError(t, err)
		require.True(t, consistent)
	}
}

func TestCmdService_NodeRestart(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "debug")
	cluster.StartNodes(3, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		require.NoError(t, cluster.Set(ctx, fmt.Sprintf("persist-%d", i), fmt.Sprintf("val-%d", i)))
	}
	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	followers := cluster.GetFollowers()
	require.NotEmpty(t, followers)

	followerID := followers[0].ID
	require.NoError(t, cluster.StopNode(followerID))
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, cluster.RestartNode(followerID))
	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	node := cluster.GetNode(followerID)
	require.NotNil(t, node)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("persist-%d", i)
		val, exists := node.StorageService.Get(key)
		require.True(t, exists)
		require.Equal(t, fmt.Sprintf("val-%d", i), val)
	}
}

func TestCmdService_MultipleFailovers(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(5, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	require.NoError(t, cluster.Set(ctx, "survive", "initial"))

	for failover := 0; failover < 2; failover++ {
		leaderID, err := cluster.WaitForLeader(10 * time.Second)
		require.NoError(t, err)

		require.NoError(t, cluster.StopNode(leaderID))
		t.Logf("failover %d: stopped leader %d", failover, leaderID)

		newLeaderID, err := cluster.WaitForNewLeader(leaderID, 15*time.Second)
		require.NoError(t, err)
		t.Logf("failover %d: new leader %d", failover, newLeaderID)

		require.NoError(t, cluster.Set(ctx, "survive", fmt.Sprintf("after-failover-%d", failover)))
	}

	require.NoError(t, cluster.WaitForConvergence(10*time.Second))

	val, exists, err := cluster.Get(ctx, "survive")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "after-failover-1", val)
}

func TestCmdService_FollowerRestart_CatchUp(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "error")
	cluster.StartNodes(3, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	followers := cluster.GetFollowers()
	require.NotEmpty(t, followers)
	followerID := followers[0].ID

	require.NoError(t, cluster.StopNode(followerID))
	t.Logf("stopped follower %d", followerID)

	for i := 0; i < 10; i++ {
		require.NoError(t, cluster.Set(ctx, fmt.Sprintf("missed-%d", i), fmt.Sprintf("val-%d", i)))
	}

	require.NoError(t, cluster.RestartNode(followerID))
	t.Logf("restarted follower %d", followerID)

	require.NoError(t, cluster.WaitForConvergence(10*time.Second))

	node := cluster.GetNode(followerID)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("missed-%d", i)
		val, exists := node.StorageService.Get(key)
		require.True(t, exists, "key %s should exist after catch-up", key)
		require.Equal(t, fmt.Sprintf("val-%d", i), val)
	}
}
