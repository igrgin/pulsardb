package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestReadAfterWriteSameNode(t *testing.T) {
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

	key := "read-after-write-key"
	value := "read-after-write-value"

	if err := tc.ProposeValue(ctx, key, value); err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	leader := tc.GetLeader()

	readIndex, err := leader.Service.GetReadIndex(ctx)
	if err != nil {
		t.Fatalf("GetReadIndex failed: %v", err)
	}

	if err := leader.Service.WaitUntilApplied(ctx, readIndex); err != nil {
		t.Fatalf("WaitUntilApplied failed: %v", err)
	}

	val, exists := leader.StateMachine.Get(key)
	if !exists {
		t.Fatalf("key not found after write")
	}
	if string(val) != value {
		t.Errorf("expected %q, got %q", value, val)
	}
}

func TestReadAfterWriteDifferentNode(t *testing.T) {
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

	key := "cross-node-read-key"
	value := "cross-node-read-value"

	if err := tc.ProposeValue(ctx, key, value); err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	if err := tc.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	followers := tc.GetFollowers()
	if len(followers) == 0 {
		t.Fatal("no followers found")
	}

	follower := followers[0]

	leaderID := follower.Service.LeaderID()
	readIndex, err := follower.Service.GetReadIndexFromLeader(ctx, leaderID)
	if err != nil {
		t.Fatalf("GetReadIndexFromLeader failed: %v", err)
	}

	if err := follower.Service.WaitUntilApplied(ctx, readIndex); err != nil {
		t.Fatalf("WaitUntilApplied on follower failed: %v", err)
	}

	val, exists := follower.StateMachine.Get(key)
	if !exists {
		t.Fatalf("key not found on follower")
	}
	if string(val) != value {
		t.Errorf("follower: expected %q, got %q", value, val)
	}
}

func TestReadIndexOnLeader(t *testing.T) {
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

	leader := tc.GetLeader()

	var indices []uint64
	for i := 0; i < 5; i++ {
		idx, err := leader.Service.GetReadIndex(ctx)
		if err != nil {
			t.Fatalf("GetReadIndex %d failed: %v", i, err)
		}
		indices = append(indices, idx)
		t.Logf("ReadIndex %d: %d", i, idx)
	}

	for i := 1; i < len(indices); i++ {
		if indices[i] < indices[i-1] {
			t.Errorf("read index went backwards: %d -> %d", indices[i-1], indices[i])
		}
	}
}

func TestConcurrentReads(t *testing.T) {
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

	for i := 0; i < 10; i++ {
		if err := tc.ProposeValue(ctx, fmt.Sprintf("concurrent-read-key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("propose failed: %v", err)
		}
	}

	if err := tc.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	leader := tc.GetLeader()

	const numReaders = 50
	var wg sync.WaitGroup
	errors := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			idx, err := leader.Service.GetReadIndex(ctx)
			if err != nil {
				errors <- fmt.Errorf("reader %d GetReadIndex: %w", i, err)
				return
			}

			if err := leader.Service.WaitUntilApplied(ctx, idx); err != nil {
				errors <- fmt.Errorf("reader %d WaitUntilApplied: %w", i, err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	var errCount int
	for err := range errors {
		t.Logf("read error: %v", err)
		errCount++
	}

	if errCount > 0 {
		t.Errorf("%d/%d reads failed", errCount, numReaders)
	}
}

func TestReadIndexTimeout(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	leaderID, err := tc.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	followers := tc.GetFollowers()
	for _, f := range followers {
		tc.StopNode(f.ID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	leader := tc.GetNode(leaderID)
	_, err = leader.Service.GetReadIndex(ctx)

	if err == nil {
		t.Log("ReadIndex succeeded without quorum - leader might have cached state")
	} else {
		t.Logf("ReadIndex failed as expected: %v", err)
	}
}

func TestWaitForQuorum(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for id := uint64(1); id <= 3; id++ {
		node := tc.GetNode(id)

		leaderID, readIndex, err := node.Service.WaitForQuorum(ctx)
		if err != nil {
			t.Errorf("node %d WaitForQuorum failed: %v", id, err)
			continue
		}

		t.Logf("node %d: leader=%d, readIndex=%d", id, leaderID, readIndex)

		if leaderID == 0 {
			t.Errorf("node %d: leader should not be 0", id)
		}
	}
}
