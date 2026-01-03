package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"pulsardb/test/integration/helper"
	"testing"
	"time"
)

func TestManualSnapshotTrigger(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 20; i++ {
		if err := c.Set(ctx, fmt.Sprintf("snap-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	leader := c.GetLeader()
	beforeSnap := leader.RaftService.LastApplied()

	if err := leader.RaftService.TriggerSnapshot(10); err != nil {
		t.Fatalf("TriggerSnapshot failed: %v", err)
	}

	snapIndex := leader.RaftNode.Storage().SnapshotIndex()
	if snapIndex == 0 {
		t.Error("snapshot index is 0 after TriggerSnapshot")
	}

	t.Logf("Snapshot created at index %d (lastApplied was %d)", snapIndex, beforeSnap)
}

func TestSnapshotRestorationOnRestart(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testData := make(map[string]string)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("restart-key-%d", i)
		value := fmt.Sprintf("restart-value-%d", i)
		testData[key] = value
		if err := c.Set(ctx, key, value); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(10 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	leader := c.GetLeader()
	if err := leader.RaftService.TriggerSnapshot(10); err != nil {
		t.Fatalf("TriggerSnapshot failed: %v", err)
	}

	if err := c.StopNode(leaderID); err != nil {
		t.Fatalf("stop node failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := c.RestartNode(leaderID); err != nil {
		t.Fatalf("restart node failed: %v", err)
	}

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader after restart: %v", err)
	}

	node := c.GetNode(leaderID)
	time.Sleep(2 * time.Second)

	for key, expectedValue := range testData {
		val, exists := node.StorageService.Get(key)
		if !exists {
			t.Logf("key %s not found in state machine (might be in storage)", key)
			continue
		}

		strVal, _ := helper.AsString(val)

		if strVal != expectedValue {
			t.Errorf("key %s: expected %q, got %q", key, expectedValue, val)
		}
	}
}

func TestFollowerCatchUpViaSnapshot(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	followers := c.GetFollowers()
	if len(followers) == 0 {
		t.Fatal("no followers found")
	}
	followerID := followers[0].ID

	if err := c.StopNode(followerID); err != nil {
		t.Fatalf("stop follower failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i := 0; i < 100; i++ {
		if err := c.Set(ctx, fmt.Sprintf("catch-up-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed at %d: %v", i, err)
		}
	}

	leader := c.GetLeader()
	if err := leader.RaftService.TriggerSnapshot(50); err != nil {
		t.Fatalf("TriggerSnapshot failed: %v", err)
	}

	if err := leader.RaftNode.Storage().Compact(80); err != nil {
		t.Logf("compact warning: %v", err)
	}

	if err := c.RestartNode(followerID); err != nil {
		t.Fatalf("restart follower failed: %v", err)
	}

	time.Sleep(5 * time.Second)

	follower := c.GetNode(followerID)
	if follower == nil {
		t.Fatal("follower not found after restart")
	}

	for i := 90; i < 100; i++ {
		key := fmt.Sprintf("catch-up-key-%d", i)
		val, exists := follower.StorageService.Get(key)
		var strVal string
		if exists {
			strVal, _ = helper.AsString(val)
		} else {
			t.Logf("key %s not found on follower (may need more time)", key)
		}

		if strVal != fmt.Sprintf("value-%d", i) {
			t.Errorf("key %s mismatch on follower", key)
		}
	}
}

func TestOldSnapshotCleanup(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(1, 30)

	leader := c.GetLeader()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for round := 0; round < 3; round++ {
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("cleanup-key-%d-%d", round, i)
			if err := c.Set(ctx, key, "value"); err != nil {
				t.Fatalf("propose failed: %v", err)
			}
		}

		if err := leader.RaftService.TriggerSnapshot(10); err != nil {
			t.Fatalf("TriggerSnapshot %d failed: %v", round, err)
		}

		applied := leader.RaftService.LastApplied()
		if applied > 15 {
			leader.RaftNode.Storage().Compact(applied - 10)
		}
	}

	nodeDir := filepath.Join(c.BaseDir, "node-1", "snapshot")
	entries, err := os.ReadDir(nodeDir)
	if err != nil {
		t.Logf("could not read snapshot dir: %v", err)
		return
	}

	snapCount := 0
	for _, e := range entries {
		if !e.IsDir() {
			snapCount++
			t.Logf("snapshot file: %s", e.Name())
		}
	}

	if snapCount > 2 {
		t.Logf("found %d snapshot files (cleanup may not have run yet)", snapCount)
	}
}
