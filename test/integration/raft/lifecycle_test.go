package integration

import (
	"context"
	"fmt"
	"pulsardb/test/integration/helper"
	"testing"
	"time"
)

func TestLeaderGracefulShutdown(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	t.Logf("Initial leader: %d", leaderID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := c.Set(ctx, fmt.Sprintf("shutdown-key-%d", i), "value"); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Logf("convergence warning before shutdown: %v", err)
	}

	if err := c.StopNode(leaderID); err != nil {
		t.Fatalf("stop leader failed: %v", err)
	}

	t.Logf("Stopped leader %d, waiting for new election...", leaderID)

	newLeaderID, err := c.WaitForNewLeader(leaderID, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to elect new leader: %v", err)
	}

	if newLeaderID == leaderID {
		t.Error("new leader should be different")
	}

	t.Logf("New leader: %d (old was %d)", newLeaderID, leaderID)

	if err := c.Set(ctx, "post-shutdown-key", "value"); err != nil {
		t.Errorf("write after leader shutdown failed: %v", err)
	}
}

func TestFollowerGracefulShutdown(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	t.Logf("Leader: %d", leaderID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	followers := c.GetFollowers()
	if len(followers) == 0 {
		t.Fatal("no followers")
	}
	followerID := followers[0].ID

	t.Logf("Stopping follower %d", followerID)

	if err := c.StopNode(followerID); err != nil {
		t.Fatalf("stop follower failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	leader := c.GetLeader()
	if leader == nil {
		t.Fatal("no leader after follower stop")
	}

	if leader.ID != leaderID {
		t.Logf("leader changed from %d to %d after follower stop (acceptable)", leaderID, leader.ID)
	}

	for i := 0; i < 10; i++ {
		if err := c.Set(ctx, fmt.Sprintf("follower-down-key-%d", i), "value"); err != nil {
			t.Errorf("write %d failed with follower down: %v", i, err)
		}
	}

	t.Log("Writes succeeded with follower down")
}

func TestSequentialNodeShutdowns(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(5, 60)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var stoppedNodes []uint64

	for i := 0; i < 2; i++ {
		followers := c.GetFollowers()
		if len(followers) == 0 {
			t.Logf("no more followers to stop at iteration %d", i)
			break
		}

		followerID := followers[0].ID
		t.Logf("Stopping node %d", followerID)

		if err := c.StopNode(followerID); err != nil {
			t.Fatalf("stop node %d failed: %v", followerID, err)
		}
		stoppedNodes = append(stoppedNodes, followerID)

		time.Sleep(300 * time.Millisecond)

		if err := c.Set(ctx, fmt.Sprintf("seq-shutdown-key-%d", i), "value"); err != nil {
			t.Errorf("write failed after stopping node %d: %v", followerID, err)
		}
	}

	t.Logf("Stopped nodes: %v, cluster still functional", stoppedNodes)
}

func TestRapidRestarts(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	for cycle := 0; cycle < 3; cycle++ {
		_, err := c.WaitForLeader(10 * time.Second)
		if err != nil {
			t.Fatalf("cycle %d: no leader before restart: %v", cycle, err)
		}

		followers := c.GetFollowers()
		if len(followers) == 0 {
			t.Logf("cycle %d: no followers, waiting for election", cycle)
			time.Sleep(2 * time.Second)
			continue
		}

		targetID := followers[0].ID
		t.Logf("Cycle %d: restarting node %d", cycle, targetID)

		if err := c.StopNode(targetID); err != nil {
			t.Fatalf("stop failed: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		if err := c.RestartNode(targetID); err != nil {
			t.Fatalf("restart failed: %v", err)
		}

		time.Sleep(1 * time.Second)

		if err := c.Set(ctx, fmt.Sprintf("rapid-restart-key-%d", cycle), "value"); err != nil {
			t.Errorf("write failed after cycle %d: %v", cycle, err)
		}

		t.Logf("Cycle %d completed successfully", cycle)
	}
}

func TestLeaderFailoverDuringWrites(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		if err := c.Set(ctx, fmt.Sprintf("pre-failover-key-%d", i), "value"); err != nil {
			t.Fatalf("pre-failover write failed: %v", err)
		}
	}

	t.Logf("Stopping leader %d during operation", leaderID)
	if err := c.StopNode(leaderID); err != nil {
		t.Fatalf("stop leader failed: %v", err)
	}

	newLeaderID, err := c.WaitForNewLeader(leaderID, 15*time.Second)
	if err != nil {
		t.Fatalf("failed to elect new leader: %v", err)
	}

	t.Logf("New leader elected: %d", newLeaderID)

	for i := 0; i < 5; i++ {
		if err := c.Set(ctx, fmt.Sprintf("post-failover-key-%d", i), "value"); err != nil {
			t.Errorf("post-failover write %d failed: %v", i, err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Logf("convergence warning: %v", err)
	}
}

func TestAllNodesRestart(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := c.Set(ctx, fmt.Sprintf("before-restart-key-%d", i), "value"); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	t.Log("Stopping all nodes...")

	for id := uint64(1); id <= 3; id++ {
		if err := c.StopNode(id); err != nil {
			t.Fatalf("stop node %d failed: %v", id, err)
		}
	}

	time.Sleep(1 * time.Second)

	t.Log("Restarting all nodes...")

	for id := uint64(1); id <= 3; id++ {
		if err := c.RestartNode(id); err != nil {
			t.Fatalf("restart node %d failed: %v", id, err)
		}
	}

	newLeaderID, err := c.WaitForLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader after full restart: %v", err)
	}

	t.Logf("Leader after restart: %d", newLeaderID)

	for i := 0; i < 5; i++ {
		if err := c.Set(ctx, fmt.Sprintf("after-restart-key-%d", i), "value"); err != nil {
			t.Errorf("post-restart write %d failed: %v", i, err)
		}
	}

	t.Log("Full cluster restart completed successfully")
}
