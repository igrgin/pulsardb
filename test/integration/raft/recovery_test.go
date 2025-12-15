package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRecoveryFromWALOnly(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	leaderID, err := tc.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("wal-key-%d", i), fmt.Sprintf("wal-value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := tc.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	appliedBefore := leader.Service.LastApplied()

	if err := tc.StopNode(leaderID); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := tc.RestartNode(leaderID); err != nil {
		t.Fatalf("restart failed: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader after restart: %v", err)
	}

	time.Sleep(2 * time.Second)

	node := tc.GetNode(leaderID)
	appliedAfter := node.Service.LastApplied()

	t.Logf("Applied before: %d, after: %d", appliedBefore, appliedAfter)

	if appliedAfter < appliedBefore {
		t.Errorf("lost applied entries: before=%d, after=%d", appliedBefore, appliedAfter)
	}
}

func TestRecoveryFromSnapshotPlusWAL(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	leaderID, err := tc.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 30; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("snap-wal-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose %d failed: %v", i, err)
		}
	}

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	if err := leader.Service.TriggerSnapshot(10); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	snapIndex := leader.Node.Storage().SnapshotIndex()
	t.Logf("Snapshot at index %d", snapIndex)

	for i := 30; i < 50; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("snap-wal-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose %d failed: %v", i, err)
		}
	}

	if err := tc.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	finalApplied := leader.Service.LastApplied()

	if err := tc.StopNode(leaderID); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := tc.RestartNode(leaderID); err != nil {
		t.Fatalf("restart failed: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	node := tc.GetNode(leaderID)
	recoveredApplied := node.Service.LastApplied()

	t.Logf("Final applied: %d, Recovered: %d", finalApplied, recoveredApplied)

	if recoveredApplied < finalApplied {
		t.Errorf("lost entries: final=%d, recovered=%d", finalApplied, recoveredApplied)
	}
}

func TestHardStatePersistence(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	leaderID, err := tc.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	hs := leader.Node.Storage().HardState()
	termBefore := hs.Term
	t.Logf("Term before restart: %d", termBefore)

	if err := tc.StopNode(leaderID); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := tc.RestartNode(leaderID); err != nil {
		t.Fatalf("restart failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	node := tc.GetNode(leaderID)
	hsAfter := node.Node.Storage().HardState()
	termAfter := hsAfter.Term

	t.Logf("Term after restart: %d", termAfter)

	if termAfter < termBefore {
		t.Errorf("term went backwards: before=%d, after=%d", termBefore, termAfter)
	}
}

func TestConfStatePersistence(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	confBefore := leader.Node.ConfState()
	t.Logf("ConfState before: voters=%v", confBefore.Voters)

	for id := uint64(1); id <= 3; id++ {
		_ = tc.StopNode(id)
	}

	time.Sleep(500 * time.Millisecond)

	for id := uint64(1); id <= 3; id++ {
		if err := tc.RestartNode(id); err != nil {
			t.Fatalf("restart node %d failed: %v", id, err)
		}
	}

	if _, err := tc.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("failed to elect leader after restart: %v", err)
	}

	newLeader := tc.GetLeader()
	require.NotNil(t, newLeader)

	confAfter := newLeader.Node.ConfState()
	t.Logf("ConfState after: voters=%v", confAfter.Voters)

	if len(confAfter.Voters) != len(confBefore.Voters) {
		t.Errorf("voter count changed: before=%d, after=%d",
			len(confBefore.Voters), len(confAfter.Voters))
	}
}

func TestFullClusterShutdownAndRestart(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testKeys := make(map[string]string)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("full-restart-key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		testKeys[key] = value
		if err := tc.ProposeValue(ctx, key, value); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := tc.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	for id := uint64(1); id <= 3; id++ {

		if err := tc.StopNode(id); err != nil {
			t.Fatalf("stop node %d failed: %v", id, err)
		}
	}

	t.Log("All nodes stopped")
	time.Sleep(1 * time.Second)

	for id := uint64(1); id <= 3; id++ {
		if err := tc.RestartNode(id); err != nil {
			t.Fatalf("restart node %d failed: %v", id, err)
		}
	}

	t.Log("All nodes restarted")

	if _, err := tc.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("failed to elect leader after full restart: %v", err)
	}

	time.Sleep(3 * time.Second)

	for id := uint64(1); id <= 3; id++ {
		node := tc.GetNode(id)
		if node == nil {
			t.Errorf("node %d not found", id)
			continue
		}

		for key, expectedValue := range testKeys {
			val, exists := node.StateMachine.Get(key)
			if !exists {
				t.Logf("node %d: key %s not in state machine", id, key)
				continue
			}
			if string(val) != expectedValue {
				t.Errorf("node %d: key %s expected %q, got %q", id, key, expectedValue, val)
			}
		}
	}
}
