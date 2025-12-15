package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterBootstrap_ThreeNodes(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	leaderID, err := tc.WaitForLeaderConvergence(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	t.Logf("Leader elected: node %d", leaderID)

	for id := uint64(1); id <= 3; id++ {
		node := tc.GetNode(id)
		if node == nil {
			t.Fatalf("node %d not found", id)
		}
		status := node.Node.Status()
		if status.Lead != leaderID {
			t.Errorf("node %d thinks leader is %d, expected %d", id, status.Lead, leaderID)
		}
	}

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	confState := leader.Node.ConfState()
	if len(confState.Voters) != 3 {
		t.Errorf("expected 3 voters, got %d", len(confState.Voters))
	}
}

func TestSingleNodeCluster(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(1); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}

	leaderID, err := tc.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	if leaderID != 1 {
		t.Errorf("expected node 1 to be leader, got %d", leaderID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := tc.ProposeValue(ctx, "key1", "value1"); err != nil {
		t.Fatalf("failed to propose value: %v", err)
	}
}

func TestLeaderElectionAfterLeaderFailure(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	oldLeaderID, err := tc.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	t.Logf("Original leader: node %d", oldLeaderID)

	if err := tc.StopNode(oldLeaderID); err != nil {
		t.Fatalf("failed to stop leader: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	newLeaderID, err := tc.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect new leader: %v", err)
	}

	if newLeaderID == oldLeaderID {
		t.Errorf("new leader should be different from old leader")
	}

	t.Logf("New leader elected: node %d", newLeaderID)
}

func TestFiveNodeCluster(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(5); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	leaderID, err := tc.WaitForLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	t.Logf("Leader elected: node %d", leaderID)

	followers := tc.GetFollowers()
	if len(followers) < 2 {
		t.Fatalf("expected at least 2 followers")
	}

	_ = tc.StopNode(followers[0].ID)
	_ = tc.StopNode(followers[1].ID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := tc.ProposeValue(ctx, "test-key", "test-value"); err != nil {
		t.Errorf("cluster should still accept writes with 2 failures: %v", err)
	}
}
