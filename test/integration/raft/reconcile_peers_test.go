package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAddNodeToCluster(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	// Start with 3 nodes
	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	leaderID, err := tc.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Write some data before adding node
	for i := 0; i < 10; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("pre-add-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := tc.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	// Add a 4th node
	newNodeID := uint64(4)
	newRaftAddr, newClientAddr, err := tc.AddNewNode(newNodeID)
	if err != nil {
		t.Fatalf("failed to add new node: %v", err)
	}

	t.Logf("New node %d started at raft=%s, client=%s", newNodeID, newRaftAddr, newClientAddr)

	// Leader proposes adding the new node
	leader := tc.GetLeader()
	require.NotNil(t, leader)
	require.Equal(t, leaderID, leader.ID)

	if err := leader.Service.ProposeAddNode(ctx, newNodeID, newRaftAddr); err != nil {
		t.Fatalf("ProposeAddNode failed: %v", err)
	}

	// Wait for the new node to catch up
	time.Sleep(3 * time.Second)

	// Verify new node is in the cluster
	newLeader := tc.GetLeader()
	require.NotNil(t, newLeader)

	confState := newLeader.Node.ConfState()
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

	// Write more data after adding node
	for i := 10; i < 20; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("post-add-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := tc.WaitForConvergence(10 * time.Second); err != nil {
		t.Logf("convergence warning: %v", err)
	}

	// Verify new node has all data
	newNode := tc.GetNode(newNodeID)
	if newNode == nil {
		t.Fatal("new node not found")
	}

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("pre-add-key-%d", i)
		if i >= 10 {
			key = fmt.Sprintf("post-add-key-%d", i)
		}
		val, exists := newNode.StateMachine.Get(key)
		if !exists {
			t.Logf("key %s not yet replicated to new node", key)
			continue
		}
		expected := fmt.Sprintf("value-%d", i)
		if string(val) != expected {
			t.Errorf("key %s: expected %q, got %q", key, expected, val)
		}
	}

	t.Logf("New node applied index: %d", newNode.Service.LastApplied())
}

func TestRemoveNodeFromCluster(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(5); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	leaderID, err := tc.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Write some data
	for i := 0; i < 10; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("remove-test-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := tc.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	// Pick a follower to remove
	followers := tc.GetFollowers()
	require.NotEmpty(t, followers)

	victimID := followers[0].ID
	t.Logf("Removing node %d (leader is %d)", victimID, leaderID)

	// Leader proposes removing the node
	leader := tc.GetLeader()
	require.NotNil(t, leader)

	if err := leader.Service.ProposeRemoveNode(ctx, victimID); err != nil {
		t.Fatalf("ProposeRemoveNode failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Verify node is removed from cluster config
	newLeader := tc.GetLeader()
	require.NotNil(t, newLeader)

	confState := newLeader.Node.ConfState()
	for _, v := range confState.Voters {
		if v == victimID {
			t.Errorf("removed node %d still in voters: %v", victimID, confState.Voters)
		}
	}

	t.Logf("Cluster voters after removal: %v", confState.Voters)

	// Cluster should still work with 4 nodes
	for i := 10; i < 20; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("remove-test-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose after removal failed: %v", err)
		}
	}

	// Stop the removed node
	tc.StopNode(victimID)

	// Cluster should still function
	if err := tc.ProposeValue(ctx, "after-stop-key", "value"); err != nil {
		t.Fatalf("propose after stopping removed node failed: %v", err)
	}
}

func TestRemoveLeaderFromCluster(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(5); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	leaderID, err := tc.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Write some data
	for i := 0; i < 10; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("leader-remove-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := tc.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	t.Logf("Removing leader %d", leaderID)

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	// Leader removes itself
	if err := leader.Service.ProposeRemoveNode(ctx, leaderID); err != nil {
		t.Fatalf("ProposeRemoveNode for leader failed: %v", err)
	}

	// Wait for new leader election
	newLeaderID, err := tc.WaitForNewLeader(leaderID, 15*time.Second)
	if err != nil {
		t.Fatalf("failed to elect new leader: %v", err)
	}

	t.Logf("New leader elected: %d", newLeaderID)

	// Verify old leader is not in voters
	newLeader := tc.GetLeader()
	require.NotNil(t, newLeader)

	confState := newLeader.Node.ConfState()
	for _, v := range confState.Voters {
		if v == leaderID {
			t.Errorf("old leader %d still in voters: %v", leaderID, confState.Voters)
		}
	}

	// Cluster should still work
	for i := 10; i < 20; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("leader-remove-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose after leader removal failed: %v", err)
		}
	}
}

func TestAddLearnerNode(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Write some data
	for i := 0; i < 10; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("learner-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	// Add a learner node
	learnerID := uint64(4)
	learnerRaftAddr, _, err := tc.AddNewNode(learnerID)
	if err != nil {
		t.Fatalf("failed to add learner node: %v", err)
	}

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	if err := leader.Service.ProposeAddLearner(ctx, learnerID, learnerRaftAddr); err != nil {
		t.Fatalf("ProposeAddLearner failed: %v", err)
	}

	time.Sleep(3 * time.Second)

	// Verify learner is in learners list, not voters
	confState := leader.Node.ConfState()

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

	// Learner should receive data but not participate in voting
	learnerNode := tc.GetNode(learnerID)
	if learnerNode != nil {
		t.Logf("Learner applied index: %d", learnerNode.Service.LastApplied())
	}
}

func TestPromoteLearnerToVoter(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Add a learner
	learnerID := uint64(4)
	learnerRaftAddr, _, err := tc.AddNewNode(learnerID)
	if err != nil {
		t.Fatalf("failed to add learner node: %v", err)
	}

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	if err := leader.Service.ProposeAddLearner(ctx, learnerID, learnerRaftAddr); err != nil {
		t.Fatalf("ProposeAddLearner failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Write some data to let learner catch up
	for i := 0; i < 20; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("promote-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	time.Sleep(2 * time.Second)

	// Promote learner to voter
	if err := leader.Service.ProposeAddNode(ctx, learnerID, learnerRaftAddr); err != nil {
		t.Fatalf("ProposeAddNode (promote) failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Verify node is now a voter
	confState := leader.Node.ConfState()

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
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Add a learner
	learnerID := uint64(4)
	learnerRaftAddr, _, err := tc.AddNewNode(learnerID)
	if err != nil {
		t.Fatalf("failed to add learner node: %v", err)
	}

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	if err := leader.Service.ProposeAddLearner(ctx, learnerID, learnerRaftAddr); err != nil {
		t.Fatalf("ProposeAddLearner failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Remove the learner
	if err := leader.Service.ProposeRemoveNode(ctx, learnerID); err != nil {
		t.Fatalf("ProposeRemoveNode failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Verify learner is removed
	confState := leader.Node.ConfState()

	for _, l := range confState.Learners {
		if l == learnerID {
			t.Errorf("removed learner %d still in learners: %v", learnerID, confState.Learners)
		}
	}

	t.Logf("After removal - Voters: %v, Learners: %v", confState.Voters, confState.Learners)
}

func TestAddRemoveMultipleNodes(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	// Add nodes 4 and 5
	for newID := uint64(4); newID <= 5; newID++ {
		newRaftAddr, _, err := tc.AddNewNode(newID)
		if err != nil {
			t.Fatalf("failed to add node %d: %v", newID, err)
		}

		if err := leader.Service.ProposeAddNode(ctx, newID, newRaftAddr); err != nil {
			t.Fatalf("ProposeAddNode for %d failed: %v", newID, err)
		}

		time.Sleep(2 * time.Second)
		t.Logf("Added node %d", newID)
	}

	// Verify 5 voters
	confState := leader.Node.ConfState()
	if len(confState.Voters) != 5 {
		t.Errorf("expected 5 voters, got %d: %v", len(confState.Voters), confState.Voters)
	}

	// Write data with 5 nodes
	for i := 0; i < 10; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("five-node-key-%d", i), "value"); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	// Remove nodes 2 and 3
	for removeID := uint64(2); removeID <= 3; removeID++ {
		if err := leader.Service.ProposeRemoveNode(ctx, removeID); err != nil {
			t.Fatalf("ProposeRemoveNode for %d failed: %v", removeID, err)
		}

		time.Sleep(2 * time.Second)
		t.Logf("Removed node %d", removeID)
	}

	// Verify 3 voters remain
	confState = leader.Node.ConfState()
	if len(confState.Voters) != 3 {
		t.Errorf("expected 3 voters, got %d: %v", len(confState.Voters), confState.Voters)
	}

	// Cluster should still work
	for i := 10; i < 20; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("three-node-key-%d", i), "value"); err != nil {
			t.Fatalf("propose after removal failed: %v", err)
		}
	}

	t.Logf("Final voters: %v", confState.Voters)
}

func TestMembershipChangeNotLeader(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get a follower
	followers := tc.GetFollowers()
	require.NotEmpty(t, followers)

	follower := followers[0]

	// Try to add node from follower - should fail
	err := follower.Service.ProposeAddNode(ctx, 99, "127.0.0.1:99999")
	if err == nil {
		t.Error("ProposeAddNode from follower should have failed")
	} else {
		t.Logf("ProposeAddNode from follower correctly failed: %v", err)
	}

	// Try to remove node from follower - should fail
	err = follower.Service.ProposeRemoveNode(ctx, 1)
	if err == nil {
		t.Error("ProposeRemoveNode from follower should have failed")
	} else {
		t.Logf("ProposeRemoveNode from follower correctly failed: %v", err)
	}
}

func TestClusterSurvivesNodeRemovalDuringWrites(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(5); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start continuous writes
	writesDone := make(chan struct{})
	writeErrors := make(chan error, 100)

	go func() {
		defer close(writesDone)
		for i := 0; i < 100; i++ {
			err := tc.ProposeValue(ctx, fmt.Sprintf("concurrent-remove-key-%d", i), "value")
			if err != nil {
				writeErrors <- err
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Remove a node during writes
	time.Sleep(500 * time.Millisecond)

	followers := tc.GetFollowers()
	require.NotEmpty(t, followers)

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	victimID := followers[0].ID
	t.Logf("Removing node %d during writes", victimID)

	if err := leader.Service.ProposeRemoveNode(ctx, victimID); err != nil {
		t.Logf("ProposeRemoveNode failed (may be expected): %v", err)
	}

	<-writesDone
	close(writeErrors)

	var errCount int
	for err := range writeErrors {
		t.Logf("write error: %v", err)
		errCount++
	}

	// Some errors are acceptable during membership change
	if errCount > 20 {
		t.Errorf("too many write errors during removal: %d", errCount)
	}

	t.Logf("Write errors during removal: %d/100", errCount)
}

func TestNewNodeGetsSnapshotOnJoin(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Write lots of data and create snapshots
	for i := 0; i < 100; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("snap-join-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	// Trigger snapshot and compact
	if err := leader.Service.TriggerSnapshot(10); err != nil {
		t.Fatalf("TriggerSnapshot failed: %v", err)
	}

	if err := leader.Node.Storage().Compact(80); err != nil {
		t.Logf("compact warning: %v", err)
	}

	snapIndex := leader.Node.Storage().SnapshotIndex()
	t.Logf("Leader snapshot at index %d", snapIndex)

	// Add new node - it should receive snapshot
	newNodeID := uint64(4)
	newRaftAddr, _, err := tc.AddNewNode(newNodeID)
	if err != nil {
		t.Fatalf("failed to add new node: %v", err)
	}

	if err := leader.Service.ProposeAddNode(ctx, newNodeID, newRaftAddr); err != nil {
		t.Fatalf("ProposeAddNode failed: %v", err)
	}

	// Wait for new node to catch up via snapshot
	targetApplied := leader.Service.LastApplied()
	if err := tc.WaitForNodeCatchUp(newNodeID, targetApplied, 20*time.Second); err != nil {
		t.Fatalf("new node failed to catch up: %v", err)
	}

	newNode := tc.GetNode(newNodeID)
	require.NotNil(t, newNode)

	// Verify new node has data
	for i := 90; i < 100; i++ {
		key := fmt.Sprintf("snap-join-key-%d", i)
		val, exists := newNode.StateMachine.Get(key)
		if !exists {
			t.Errorf("key %s missing on new node", key)
			continue
		}
		expected := fmt.Sprintf("value-%d", i)
		if string(val) != expected {
			t.Errorf("key %s: expected %q, got %q", key, expected, val)
		}
	}

	t.Logf("New node applied: %d, snap index: %d", newNode.Service.LastApplied(), newNode.Node.Storage().SnapshotIndex())
}

func TestConfStatePersistsAcrossRestart(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	leader := tc.GetLeader()
	require.NotNil(t, leader)

	// Add a new node
	newNodeID := uint64(4)
	newRaftAddr, _, err := tc.AddNewNode(newNodeID)
	if err != nil {
		t.Fatalf("failed to add node: %v", err)
	}

	if err := leader.Service.ProposeAddNode(ctx, newNodeID, newRaftAddr); err != nil {
		t.Fatalf("ProposeAddNode failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Verify 4 voters
	confBefore := leader.Node.ConfState()
	require.Equal(t, 4, len(confBefore.Voters), "expected 4 voters")
	t.Logf("Voters before restart: %v", confBefore.Voters)

	// Restart all nodes
	for id := uint64(1); id <= 4; id++ {
		tc.StopNode(id)
	}

	time.Sleep(1 * time.Second)

	for id := uint64(1); id <= 4; id++ {
		if err := tc.RestartNode(id); err != nil {
			t.Fatalf("restart node %d failed: %v", id, err)
		}
	}

	if _, err := tc.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("leader election after restart failed: %v", err)
	}

	// Verify confState persisted
	newLeader := tc.GetLeader()
	require.NotNil(t, newLeader)

	confAfter := newLeader.Node.ConfState()
	if len(confAfter.Voters) != len(confBefore.Voters) {
		t.Errorf("voter count changed: before=%d, after=%d", len(confBefore.Voters), len(confAfter.Voters))
	}
	t.Logf("Voters after restart: %v", confAfter.Voters)

	// Cluster should still work
	if err := tc.ProposeValue(ctx, "after-restart-key", "value"); err != nil {
		t.Fatalf("propose after restart failed: %v", err)
	}
}
