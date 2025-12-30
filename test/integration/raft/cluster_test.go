package integration

import (
	"context"
	"pulsardb/test/integration/helper"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterBootstrap_ThreeNodes(t *testing.T) {
	tc := helper.NewCluster(t, nil, "warn")

	tc.StartNodes(3, 60)

	leaderID, err := tc.WaitForLeaderConvergence(10 * time.Second)
	require.NoError(t, err, "failed to elect leader")

	t.Logf("Leader elected: node %d", leaderID)

	for id := uint64(1); id <= 3; id++ {
		node := tc.GetNode(id)
		require.NotNil(t, node, "node %d not found", id)

		status := node.RaftNode.Status()
		require.Equal(t, leaderID, status.Lead, "node %d thinks leader is %d, expected %d", id, status.Lead, leaderID)
	}

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	confState := leader.RaftNode.ConfState()
	require.Len(t, confState.Voters, 3, "expected 3 voters")
}

func TestSingleNodeCluster(t *testing.T) {
	tc := helper.NewCluster(t, nil, "warn")

	tc.StartNodes(1, 30)

	leaderID, err := tc.WaitForLeader(5 * time.Second)
	require.NoError(t, err, "failed to elect leader")
	require.Equal(t, uint64(1), leaderID, "expected node 1 to be leader")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = tc.Set(ctx, "key1", "value1")
	require.NoError(t, err, "failed to set value")
}

func TestLeaderElectionAfterLeaderFailure(t *testing.T) {
	tc := helper.NewCluster(t, nil, "warn")

	tc.StartNodes(3, 60)

	oldLeaderID, err := tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err, "failed to elect leader")

	t.Logf("Original leader: node %d", oldLeaderID)

	err = tc.StopNode(oldLeaderID)
	require.NoError(t, err, "failed to stop leader")

	time.Sleep(500 * time.Millisecond)

	newLeaderID, err := tc.WaitForNewLeader(oldLeaderID, 10*time.Second)
	require.NoError(t, err, "failed to elect new leader")
	require.NotEqual(t, oldLeaderID, newLeaderID, "new leader should be different from old leader")

	t.Logf("New leader elected: node %d", newLeaderID)
}

func TestFiveNodeCluster(t *testing.T) {
	tc := helper.NewCluster(t, nil, "warn")

	tc.StartNodes(5, 60)

	leaderID, err := tc.WaitForLeader(15 * time.Second)
	require.NoError(t, err, "failed to elect leader")

	t.Logf("Leader elected: node %d", leaderID)

	followers := tc.GetFollowers()
	require.GreaterOrEqual(t, len(followers), 2, "expected at least 2 followers")

	_ = tc.StopNode(followers[0].ID)
	_ = tc.StopNode(followers[1].ID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = tc.Set(ctx, "test-key", "test-value")
	require.NoError(t, err, "cluster should still accept writes with 2 failures")
}
