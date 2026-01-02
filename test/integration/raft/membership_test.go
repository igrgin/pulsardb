package integration

import (
	"context"
	"fmt"
	"pulsardb/test/integration/helper"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAddNodeToCluster(t *testing.T) {
	c := helper.NewCluster(t, nil, "debug")

	c.StartNodes(3, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := c.Set(ctx, fmt.Sprintf("pre-add-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	newNodeID := uint64(4)
	newRaftAddr, newClientAddr, err := c.AddNewNode(newNodeID)
	if err != nil {
		t.Fatalf("failed to add new node: %v", err)
	}

	t.Logf("New node %d started at raft=%s, client=%s", newNodeID, newRaftAddr, newClientAddr)

	leader := c.GetLeader()
	require.NotNil(t, leader)
	require.Equal(t, leaderID, leader.ID)

	if err := leader.RaftService.ProposeAddNode(ctx, newNodeID, newRaftAddr); err != nil {
		t.Fatalf("ProposeAddNode failed: %v", err)
	}

	time.Sleep(3 * time.Second)

	newLeader := c.GetLeader()
	require.NotNil(t, newLeader)

	confState := newLeader.RaftNode.ConfState()
	found := false
	for _, v := range confState.Voters {
		if v == newNodeID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("new node %d not found in voters: %v", newNodeID, confState.Voters)
	}

	for i := 10; i < 20; i++ {
		if err := c.Set(ctx, fmt.Sprintf("post-add-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(10 * time.Second); err != nil {
		t.Logf("convergence warning: %v", err)
	}

	newNode := c.GetNode(newNodeID)
	if newNode == nil {
		t.Fatal("new node not found")
	}

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("pre-add-key-%d", i)
		if i >= 10 {
			key = fmt.Sprintf("post-add-key-%d", i)
		}
		val, exists := newNode.StorageService.Get(key)
		if !exists {
			t.Logf("key %s not yet replicated to new node", key)
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

	t.Logf("New node applied index: %d", newNode.RaftService.LastApplied())
}

func TestRemoveNodeFromCluster(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(5, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := c.Set(ctx, fmt.Sprintf("remove-test-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	followers := c.GetFollowers()
	require.NotEmpty(t, followers)

	victimID := followers[0].ID
	t.Logf("Removing node %d (leader is %d)", victimID, leaderID)
	err = c.StopNode(victimID)
	require.NoError(t, err)

	leader := c.GetLeader()
	require.NotNil(t, leader)

	if err := leader.RaftService.ProposeRemoveNode(ctx, victimID); err != nil {
		t.Fatalf("ProposeRemoveNode failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	newLeader := c.GetLeader()
	require.NotNil(t, newLeader)

	confState := newLeader.RaftNode.ConfState()
	for _, v := range confState.Voters {
		if v == victimID {
			t.Errorf("removed node %d still in voters: %v", victimID, confState.Voters)
		}
	}

	t.Logf("Cluster voters after removal: %v", confState.Voters)

	for i := 10; i < 20; i++ {
		if err := c.Set(ctx, fmt.Sprintf("remove-test-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose after removal failed: %v", err)
		}
	}

	c.StopNode(victimID)

	if err := c.Set(ctx, "after-stop-key", "value"); err != nil {
		t.Fatalf("propose after stopping removed node failed: %v", err)
	}
}

func TestRemoveLeaderFromCluster(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(5, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := c.Set(ctx, fmt.Sprintf("leader-remove-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	t.Logf("Removing leader %d", leaderID)
	err = c.StopNode(leaderID)
	require.NoError(t, err)
	c.WaitForNewLeader(leaderID, 15*time.Second)

	newLeader := c.GetLeader()
	require.NotNil(t, newLeader)

	if err := newLeader.RaftService.ProposeRemoveNode(ctx, leaderID); err != nil {
		t.Fatalf("ProposeRemoveNode for leader failed: %v", err)
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	confState := newLeader.RaftNode.ConfState()
	for _, v := range confState.Voters {
		if v == leaderID {
			t.Errorf("old leader %d still in voters: %v", leaderID, confState.Voters)
		}
	}

	for i := 10; i < 20; i++ {
		if err := c.Set(ctx, fmt.Sprintf("leader-remove-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose after leader removal failed: %v", err)
		}
	}
}

func TestAddLearnerNode(t *testing.T) {

	cfg := &helper.TestClusterConfig{
		TickInterval:           100 * time.Millisecond,
		ElectionTick:           10,
		BatchSize:              10,
		BatchWait:              2,
		PromotionThreshold:     10000,
		PromotionCheckInterval: 1 * time.Minute,
	}
	c := helper.NewCluster(t, cfg, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := c.Set(ctx, fmt.Sprintf("learner-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	learnerID := uint64(4)
	learnerRaftAddr, _, err := c.AddNewNode(learnerID)
	if err != nil {
		t.Fatalf("failed to add learner node: %v", err)
	}

	leader := c.GetLeader()
	require.NotNil(t, leader)

	if err := leader.RaftService.ProposeAddLearner(ctx, learnerID, learnerRaftAddr); err != nil {
		t.Fatalf("ProposeAddLearner failed: %v", err)
	}

	time.Sleep(3 * time.Second)

	confState := leader.RaftNode.ConfState()

	foundInLearners := false
	for _, l := range confState.Learners {
		if l == learnerID {
			foundInLearners = true
			break
		}
	}

	foundInVoters := false
	for _, v := range confState.Voters {
		if v == learnerID {
			foundInVoters = true
			break
		}
	}

	if !foundInLearners {
		t.Errorf("node %d not found in learners: %v", learnerID, confState.Learners)
	}
	if foundInVoters {
		t.Errorf("learner %d should not be in voters: %v", learnerID, confState.Voters)
	}

	t.Logf("Voters: %v, Learners: %v", confState.Voters, confState.Learners)

	learnerNode := c.GetNode(learnerID)
	if learnerNode != nil {
		t.Logf("Learner applied index: %d", learnerNode.RaftService.LastApplied())
	}
}

func TestPromoteLearnerToVoter(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	learnerID := uint64(4)
	learnerRaftAddr, _, err := c.AddNewNode(learnerID)
	if err != nil {
		t.Fatalf("failed to add learner node: %v", err)
	}

	leader := c.GetLeader()
	require.NotNil(t, leader)

	if err := leader.RaftService.ProposeAddLearner(ctx, learnerID, learnerRaftAddr); err != nil {
		t.Fatalf("ProposeAddLearner failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < 20; i++ {
		if err := c.Set(ctx, fmt.Sprintf("promote-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	time.Sleep(2 * time.Second)

	if err := leader.RaftService.ProposeAddNode(ctx, learnerID, learnerRaftAddr); err != nil {
		t.Fatalf("ProposeAddNode (promote) failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	confState := leader.RaftNode.ConfState()

	foundInVoters := false
	for _, v := range confState.Voters {
		if v == learnerID {
			foundInVoters = true
			break
		}
	}

	foundInLearners := false
	for _, l := range confState.Learners {
		if l == learnerID {
			foundInLearners = true
			break
		}
	}

	if !foundInVoters {
		t.Errorf("promoted node %d not in voters: %v", learnerID, confState.Voters)
	}
	if foundInLearners {
		t.Errorf("promoted node %d should not be in learners: %v", learnerID, confState.Learners)
	}

	t.Logf("After promotion - Voters: %v, Learners: %v", confState.Voters, confState.Learners)
}

func TestRemoveLearnerFromCluster(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	learnerID := uint64(4)
	learnerRaftAddr, _, err := c.AddNewNode(learnerID)
	if err != nil {
		t.Fatalf("failed to add learner node: %v", err)
	}

	leader := c.GetLeader()
	require.NotNil(t, leader)

	if err := leader.RaftService.ProposeAddLearner(ctx, learnerID, learnerRaftAddr); err != nil {
		t.Fatalf("ProposeAddLearner failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	if err := leader.RaftService.ProposeRemoveNode(ctx, learnerID); err != nil {
		t.Fatalf("ProposeRemoveNode failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	confState := leader.RaftNode.ConfState()

	for _, l := range confState.Learners {
		if l == learnerID {
			t.Errorf("removed learner %d still in learners: %v", learnerID, confState.Learners)
		}
	}

	t.Logf("After removal - Voters: %v, Learners: %v", confState.Voters, confState.Learners)
}

func TestAddRemoveMultipleNodes(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	leader := c.GetLeader()
	require.NotNil(t, leader)

	for newID := uint64(4); newID <= 5; newID++ {
		newRaftAddr, _, err := c.AddNewNode(newID)
		if err != nil {
			t.Fatalf("failed to add node %d: %v", newID, err)
		}

		targetApplied := leader.RaftService.LastApplied()

		if err := leader.RaftService.ProposeAddNode(ctx, newID, newRaftAddr); err != nil {
			t.Fatalf("ProposeAddNode for %d failed: %v", newID, err)
		}

		require.Eventually(t, func() bool {
			currentLeader := c.GetLeader()
			if currentLeader == nil {
				return false
			}
			confState := currentLeader.RaftNode.ConfState()
			for _, v := range confState.Voters {
				if v == newID {
					return true
				}
			}
			return false
		}, 10*time.Second, 100*time.Millisecond, "node %d not added to voters", newID)

		newNode := c.GetNode(newID)
		require.NotNil(t, newNode, "new node %d not found", newID)

		require.Eventually(t, func() bool {
			return newNode.RaftService.LastApplied() >= targetApplied
		}, 10*time.Second, 100*time.Millisecond, "node %d didn't catch up to index %d", newID, targetApplied)

		t.Logf("Added node %d, applied index: %d", newID, newNode.RaftService.LastApplied())
	}

	confState := leader.RaftNode.ConfState()
	if len(confState.Voters) != 5 {
		t.Errorf("expected 5 voters, got %d: %v", len(confState.Voters), confState.Voters)
	}

	for i := 0; i < 10; i++ {
		if err := c.Set(ctx, fmt.Sprintf("five-node-key-%d", i), "value"); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	targetIndex := c.GetLeader().RaftService.LastApplied()
	if err := c.WaitForApplied(targetIndex, 30*time.Second); err != nil {
		t.Fatalf("convergence failed before removals: %v", err)
	}

	for removeID := uint64(2); removeID <= 3; removeID++ {
		require.NoError(t, c.StopNode(removeID))

		leaderID, err := c.WaitForLeader(15 * time.Second)
		require.NoError(t, err)

		leaderNode := c.GetNode(leaderID)
		require.NotNil(t, leaderNode)

		require.NoError(t, leaderNode.RaftService.ProposeRemoveNode(ctx, removeID))

		require.Eventually(t, func() bool {
			currentLeader := c.GetLeader()
			if currentLeader == nil {
				return false
			}
			cs := currentLeader.RaftNode.ConfState()
			for _, v := range cs.Voters {
				if v == removeID {
					return false
				}
			}
			return true
		}, 10*time.Second, 100*time.Millisecond, "node %d not removed from voters", removeID)

		t.Logf("Removed node %d", removeID)
	}

	leader = c.GetLeader()
	confState = leader.RaftNode.ConfState()
	if len(confState.Voters) != 3 {
		t.Errorf("expected 3 voters, got %d: %v", len(confState.Voters), confState.Voters)
	}

	for i := 10; i < 20; i++ {
		if err := c.Set(ctx, fmt.Sprintf("three-node-key-%d", i), "value"); err != nil {
			t.Fatalf("propose after removal failed: %v", err)
		}
	}

	t.Logf("Final voters: %v", confState.Voters)
}

func TestMembershipChangeNotLeader(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	followers := c.GetFollowers()
	require.NotEmpty(t, followers)

	follower := followers[0]

	err := follower.RaftService.ProposeAddNode(ctx, 99, "127.0.0.1:99999")
	if err == nil {
		t.Error("ProposeAddNode from follower should have failed")
	} else {
		t.Logf("ProposeAddNode from follower correctly failed: %v", err)
	}

	err = follower.RaftService.ProposeRemoveNode(ctx, 1)
	if err == nil {
		t.Error("ProposeRemoveNode from follower should have failed")
	} else {
		t.Logf("ProposeRemoveNode from follower correctly failed: %v", err)
	}
}

func TestClusterSurvivesNodeRemovalDuringWrites(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(5, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	writesDone := make(chan struct{})
	writeErrors := make(chan error, 100)

	go func() {
		defer close(writesDone)
		for i := 0; i < 100; i++ {
			err := c.Set(ctx, fmt.Sprintf("concurrent-remove-key-%d", i), "value")
			if err != nil {
				writeErrors <- err
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	time.Sleep(500 * time.Millisecond)

	followers := c.GetFollowers()
	require.NotEmpty(t, followers)

	leader := c.GetLeader()
	require.NotNil(t, leader)

	victimID := followers[0].ID
	t.Logf("Removing node %d during writes", victimID)

	if err := leader.RaftService.ProposeRemoveNode(ctx, victimID); err != nil {
		t.Logf("ProposeRemoveNode failed (may be expected): %v", err)
	}

	<-writesDone
	close(writeErrors)

	var errCount int
	for err := range writeErrors {
		t.Logf("write error: %v", err)
		errCount++
	}

	if errCount > 20 {
		t.Errorf("too many write errors during removal: %d", errCount)
	}

	t.Logf("Write errors during removal: %d/100", errCount)
}

func TestNewNodeGetsSnapshotOnJoin(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i := 0; i < 100; i++ {
		if err := c.Set(ctx, fmt.Sprintf("snap-join-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	leader := c.GetLeader()
	require.NotNil(t, leader)

	if err := leader.RaftService.TriggerSnapshot(10); err != nil {
		t.Fatalf("TriggerSnapshot failed: %v", err)
	}

	if err := leader.RaftNode.Storage().Compact(80); err != nil {
		t.Logf("compact warning: %v", err)
	}

	snapIndex := leader.RaftNode.Storage().SnapshotIndex()
	t.Logf("Leader snapshot at index %d", snapIndex)

	newNodeID := uint64(4)
	newRaftAddr, _, err := c.AddNewNode(newNodeID)
	if err != nil {
		t.Fatalf("failed to add new node: %v", err)
	}

	if err := leader.RaftService.ProposeAddNode(ctx, newNodeID, newRaftAddr); err != nil {
		t.Fatalf("ProposeAddNode failed: %v", err)
	}

	targetApplied := leader.RaftService.LastApplied()
	if err := c.WaitForApplied(targetApplied, 20*time.Second); err != nil {
		t.Fatalf("new node failed to catch up: %v", err)
	}

	newNode := c.GetNode(newNodeID)
	require.NotNil(t, newNode)

	for i := 90; i < 100; i++ {
		key := fmt.Sprintf("snap-join-key-%d", i)
		val, exists := newNode.StorageService.Get(key)
		if !exists {
			t.Errorf("key %s missing on new node", key)
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

	t.Logf("New node applied: %d, snap index: %d", newNode.RaftService.LastApplied(), newNode.RaftNode.Storage().SnapshotIndex())
}

func TestConfStatePersistsAcrossRestart(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	leader := c.GetLeader()
	require.NotNil(t, leader)

	newNodeID := uint64(4)
	newRaftAddr, _, err := c.AddNewNode(newNodeID)
	if err != nil {
		t.Fatalf("failed to add node: %v", err)
	}

	if err := leader.RaftService.ProposeAddNode(ctx, newNodeID, newRaftAddr); err != nil {
		t.Fatalf("ProposeAddNode failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	confBefore := leader.RaftNode.ConfState()
	require.Equal(t, 4, len(confBefore.Voters), "expected 4 voters")
	t.Logf("Voters before restart: %v", confBefore.Voters)

	for id := uint64(1); id <= 4; id++ {
		c.StopNode(id)
	}

	time.Sleep(1 * time.Second)

	for id := uint64(1); id <= 4; id++ {
		if err := c.RestartNode(id); err != nil {
			t.Fatalf("restart node %d failed: %v", id, err)
		}
	}

	if _, err := c.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("leader election after restart failed: %v", err)
	}

	newLeader := c.GetLeader()
	require.NotNil(t, newLeader)

	confAfter := newLeader.RaftNode.ConfState()
	if len(confAfter.Voters) != len(confBefore.Voters) {
		t.Errorf("voter count changed: before=%d, after=%d", len(confBefore.Voters), len(confAfter.Voters))
	}
	t.Logf("Voters after restart: %v", confAfter.Voters)

	if err := c.Set(ctx, "after-restart-key", "value"); err != nil {
		t.Fatalf("propose after restart failed: %v", err)
	}
}
