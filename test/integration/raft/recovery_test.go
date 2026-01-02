package integration

import (
	"context"
	"fmt"
	"pulsardb/test/integration/helper"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRecoveryFromWALOnly(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := c.Set(ctx, fmt.Sprintf("wal-key-%d", i), fmt.Sprintf("wal-value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	leader := c.GetLeader()
	require.NotNil(t, leader)

	appliedBefore := leader.RaftService.LastApplied()

	if err := c.StopNode(leaderID); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := c.RestartNode(leaderID); err != nil {
		t.Fatalf("restart failed: %v", err)
	}

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader after restart: %v", err)
	}

	time.Sleep(2 * time.Second)

	node := c.GetNode(leaderID)
	appliedAfter := node.RaftService.LastApplied()

	t.Logf("Applied before: %d, after: %d", appliedBefore, appliedAfter)

	if appliedAfter < appliedBefore {
		t.Errorf("lost applied entries: before=%d, after=%d", appliedBefore, appliedAfter)
	}
}

func TestRecoveryFromSnapshotPlusWAL(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 30; i++ {
		if err := c.Set(ctx, fmt.Sprintf("snap-wal-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose %d failed: %v", i, err)
		}
	}

	leader := c.GetLeader()
	require.NotNil(t, leader)

	if err := leader.RaftService.TriggerSnapshot(10); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	snapIndex := leader.RaftNode.Storage().SnapshotIndex()
	t.Logf("Snapshot at index %d", snapIndex)

	for i := 30; i < 50; i++ {
		if err := c.Set(ctx, fmt.Sprintf("snap-wal-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose %d failed: %v", i, err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	finalApplied := leader.RaftService.LastApplied()

	if err := c.StopNode(leaderID); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := c.RestartNode(leaderID); err != nil {
		t.Fatalf("restart failed: %v", err)
	}

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	node := c.GetNode(leaderID)
	recoveredApplied := node.RaftService.LastApplied()

	t.Logf("Final applied: %d, Recovered: %d", finalApplied, recoveredApplied)

	if recoveredApplied < finalApplied {
		t.Errorf("lost entries: final=%d, recovered=%d", finalApplied, recoveredApplied)
	}
}

func TestHardStatePersistence(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	leader := c.GetLeader()
	require.NotNil(t, leader)

	hs := leader.RaftNode.Storage().HardState()
	termBefore := hs.Term
	t.Logf("Term before restart: %d", termBefore)

	if err := c.StopNode(leaderID); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := c.RestartNode(leaderID); err != nil {
		t.Fatalf("restart failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	node := c.GetNode(leaderID)
	hsAfter := node.RaftNode.Storage().HardState()
	termAfter := hsAfter.Term

	t.Logf("Term after restart: %d", termAfter)

	if termAfter < termBefore {
		t.Errorf("term went backwards: before=%d, after=%d", termBefore, termAfter)
	}
}

func TestConfStatePersistence(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	leader := c.GetLeader()
	require.NotNil(t, leader)

	confBefore := leader.RaftNode.ConfState()
	t.Logf("ConfState before: voters=%v", confBefore.Voters)

	for id := uint64(1); id <= 3; id++ {
		_ = c.StopNode(id)
	}

	time.Sleep(500 * time.Millisecond)

	for id := uint64(1); id <= 3; id++ {
		if err := c.RestartNode(id); err != nil {
			t.Fatalf("restart node %d failed: %v", id, err)
		}
	}

	if _, err := c.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("failed to elect leader after restart: %v", err)
	}

	newLeader := c.GetLeader()
	require.NotNil(t, newLeader)

	confAfter := newLeader.RaftNode.ConfState()
	t.Logf("ConfState after: voters=%v", confAfter.Voters)

	if len(confAfter.Voters) != len(confBefore.Voters) {
		t.Errorf("voter count changed: before=%d, after=%d",
			len(confBefore.Voters), len(confAfter.Voters))
	}
}

func TestFullClusterShutdownAndRestart(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testKeys := make(map[string]string)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("full-restart-key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		testKeys[key] = value
		if err := c.Set(ctx, key, value); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	for id := uint64(1); id <= 3; id++ {
		if err := c.StopNode(id); err != nil {
			t.Fatalf("stop node %d failed: %v", id, err)
		}
	}

	t.Log("All nodes stopped")
	time.Sleep(1 * time.Second)

	for id := uint64(1); id <= 3; id++ {
		if err := c.RestartNode(id); err != nil {
			t.Fatalf("restart node %d failed: %v", id, err)
		}
	}

	t.Log("All nodes restarted")

	if _, err := c.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("failed to elect leader after full restart: %v", err)
	}

	time.Sleep(3 * time.Second)

	for id := uint64(1); id <= 3; id++ {
		node := c.GetNode(id)
		if node == nil {
			t.Errorf("node %d not found", id)
			continue
		}

		for key, expectedValue := range testKeys {
			val, exists := node.StorageService.Get(key)
			if !exists {
				t.Logf("node %d: key %s not in state machine", id, key)
				continue
			}
			strVal, ok := val.(string)
			if !ok {
				t.Errorf("node %d: key %s expected string, got %T", id, key, val)
				continue
			}
			if strVal != expectedValue {
				t.Errorf("node %d: key %s expected %q, got %q", id, key, expectedValue, strVal)
			}
		}
	}
}

func TestRecoveryFromWALDeletion(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 15)

	leaderID, err := c.WaitForLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for i := 0; i < 20; i++ {
		if err := c.Set(ctx, fmt.Sprintf("recovery-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(20 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	followers := c.GetFollowers()
	require.NotEmpty(t, followers, "expected at least one follower")

	victimID := followers[0].ID
	t.Logf("Victim node: %d, Leader: %d", victimID, leaderID)

	clusterApplied := c.GetClusterAppliedIndex()
	t.Logf("Cluster applied index before victim stop: %d", clusterApplied)

	if err := c.StopNode(victimID); err != nil {
		t.Fatalf("failed to stop victim: %v", err)
	}

	if err := c.DeleteNodeData(victimID); err != nil {
		t.Fatalf("failed to delete node data: %v", err)
	}

	leader := c.GetLeader()
	require.NotNil(t, leader)

	if err := leader.RaftService.ProposeRemoveNode(ctx, victimID); err != nil {
		t.Fatalf("ProposeRemoveNode failed: %v", err)
	}

	t.Log("Deleted WAL and snapshots from victim node")

	for i := 20; i < 30; i++ {
		if err := c.Set(ctx, fmt.Sprintf("recovery-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	newClusterApplied := c.GetClusterAppliedIndex()
	t.Logf("Cluster applied index after more writes: %d", newClusterApplied)

	if err := c.RestartNodeAfterDataLoss(victimID); err != nil {
		t.Fatalf("failed to restart victim: %v", err)
	}

	t.Log("Victim node restarted")

	if err := c.WaitForApplied(newClusterApplied, 15*time.Second); err != nil {
		t.Fatalf("victim failed to catch up: %v", err)
	}

	victim := c.GetNode(victimID)
	require.NotNil(t, victim)

	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("recovery-key-%d", i)
		val, exists := victim.StorageService.Get(key)
		if !exists {
			t.Errorf("key %s missing on recovered node", key)
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			t.Errorf("key %s: expected string, got %T", key, val)
			continue
		}
		expected := fmt.Sprintf("value-%d", i)
		if strVal != expected {
			t.Errorf("key %s: expected %q, got %q", key, expected, strVal)
		}
	}

	t.Logf("Victim recovered to applied index: %d", victim.RaftService.LastApplied())
}

func TestRecoveryFromWALDeletionWithSnapshot(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	_, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 50; i++ {
		if err := c.Set(ctx, fmt.Sprintf("snap-recovery-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	leader := c.GetLeader()
	require.NotNil(t, leader)

	if err := leader.RaftService.TriggerSnapshot(10); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	snapIndex := leader.RaftNode.Storage().SnapshotIndex()
	t.Logf("Leader snapshot at index %d", snapIndex)

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	followers := c.GetFollowers()
	require.NotEmpty(t, followers)
	victimID := followers[0].ID

	if err := c.StopNode(victimID); err != nil {
		t.Fatalf("failed to stop victim: %v", err)
	}

	if err := c.DeleteNodeData(victimID); err != nil {
		t.Fatalf("failed to delete node data: %v", err)
	}

	for i := 50; i < 70; i++ {
		if err := c.Set(ctx, fmt.Sprintf("snap-recovery-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	targetApplied := c.GetClusterAppliedIndex()

	if err := c.RestartNodeAfterDataLoss(victimID); err != nil {
		t.Fatalf("failed to restart victim: %v", err)
	}

	if err := c.WaitForApplied(targetApplied, 20*time.Second); err != nil {
		t.Fatalf("victim failed to catch up: %v", err)
	}

	victim := c.GetNode(victimID)
	require.NotNil(t, victim)

	for i := 0; i < 70; i++ {
		key := fmt.Sprintf("snap-recovery-key-%d", i)
		val, exists := victim.StorageService.Get(key)
		if !exists {
			t.Errorf("key %s missing", key)
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			t.Errorf("key %s: expected string, got %T", key, val)
			continue
		}
		expected := fmt.Sprintf("value-%d", i)
		if strVal != expected {
			t.Errorf("key %s: expected %q, got %q", key, expected, strVal)
		}
	}

	t.Logf("Victim recovered with snapshot, applied: %d", victim.RaftService.LastApplied())
}

func TestLeaderRecoveryFromWALDeletion(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for i := 0; i < 30; i++ {
		if err := c.Set(ctx, fmt.Sprintf("leader-recovery-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	t.Logf("Stopping leader %d and deleting its data", leaderID)

	if err := c.StopNode(leaderID); err != nil {
		t.Fatalf("failed to stop leader: %v", err)
	}

	if err := c.DeleteNodeData(leaderID); err != nil {
		t.Fatalf("failed to delete leader data: %v", err)
	}

	newLeaderID, err := c.WaitForNewLeader(leaderID, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to elect new leader: %v", err)
	}
	t.Logf("New leader elected: %d", newLeaderID)

	for i := 30; i < 40; i++ {
		if err := c.Set(ctx, fmt.Sprintf("leader-recovery-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	targetApplied := c.GetClusterAppliedIndex()

	if err := c.RestartNodeAfterDataLoss(leaderID); err != nil {
		t.Fatalf("failed to restart old leader: %v", err)
	}

	if err := c.WaitForApplied(targetApplied, 15*time.Second); err != nil {
		t.Fatalf("old leader failed to catch up: %v", err)
	}

	recovered := c.GetNode(leaderID)
	require.NotNil(t, recovered)

	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("leader-recovery-key-%d", i)
		val, exists := recovered.StorageService.Get(key)
		if !exists {
			t.Errorf("key %s missing", key)
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			t.Errorf("key %s: expected string, got %T", key, val)
			continue
		}
		expected := fmt.Sprintf("value-%d", i)
		if strVal != expected {
			t.Errorf("key %s: expected %q, got %q", key, expected, strVal)
		}
	}

	t.Logf("Old leader recovered, applied: %d", recovered.RaftService.LastApplied())
}

func TestMultipleNodesRecoverFromDataLoss(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(5, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 30; i++ {
		if err := c.Set(ctx, fmt.Sprintf("multi-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	followers := c.GetFollowers()
	require.GreaterOrEqual(t, len(followers), 2)

	victim1 := followers[0].ID
	victim2 := followers[1].ID
	t.Logf("Victims: %d, %d (leader: %d)", victim1, victim2, leaderID)

	for _, victimID := range []uint64{victim1, victim2} {
		if err := c.StopNode(victimID); err != nil {
			t.Fatalf("failed to stop node %d: %v", victimID, err)
		}
		if err := c.DeleteNodeData(victimID); err != nil {
			t.Fatalf("failed to delete data for node %d: %v", victimID, err)
		}
	}

	for i := 30; i < 40; i++ {
		if err := c.Set(ctx, fmt.Sprintf("multi-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	targetApplied := c.GetClusterAppliedIndex()

	for _, victimID := range []uint64{victim1, victim2} {
		if err := c.RestartNodeAfterDataLoss(victimID); err != nil {
			t.Fatalf("failed to restart node %d: %v", victimID, err)
		}
	}

	if err := c.WaitForApplied(targetApplied, 15*time.Second); err != nil {
		t.Fatalf("nodes failed to catch up: %v", err)
	}

	for _, victimID := range []uint64{victim1, victim2} {
		node := c.GetNode(victimID)
		require.NotNil(t, node)

		for i := 0; i < 40; i++ {
			key := fmt.Sprintf("multi-key-%d", i)
			val, exists := node.StorageService.Get(key)
			if !exists {
				t.Errorf("node %d: key %s missing", victimID, key)
				continue
			}
			strVal, ok := val.(string)
			if !ok {
				t.Errorf("node %d: key %s expected string, got %T", victimID, key, val)
				continue
			}
			expected := fmt.Sprintf("value-%d", i)
			if strVal != expected {
				t.Errorf("node %d: key %s expected %q, got %q", victimID, key, expected, strVal)
			}
		}
		t.Logf("Node %d recovered, applied: %d", victimID, node.RaftService.LastApplied())
	}
}
