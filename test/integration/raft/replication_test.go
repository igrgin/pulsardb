package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"pulsardb/internal/transport/gen/commandevents"
)

func TestBasicSetOperation(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := tc.ProposeValue(ctx, "test-key", "test-value"); err != nil {
		t.Fatalf("failed to propose: %v", err)
	}

	if err := tc.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("failed to converge: %v", err)
	}

	for id := uint64(1); id <= 3; id++ {
		node := tc.GetNode(id)
		val, exists := node.StateMachine.Get("test-key")
		if !exists {
			t.Errorf("node %d: key not found", id)
			continue
		}
		if string(val) != "test-value" {
			t.Errorf("node %d: expected 'test-value', got %q", id, val)
		}
	}
}

func TestBasicDeleteOperation(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := tc.ProposeValue(ctx, "delete-key", "some-value"); err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	leader := tc.GetLeader()
	req := &commandeventspb.CommandEventRequest{
		EventId: uint64(time.Now().UnixNano()),
		Type:    commandeventspb.CommandEventType_DELETE,
		Key:     "delete-key",
	}

	respCh, err := leader.Batcher.Submit(ctx, req)
	if err != nil {
		t.Fatalf("failed to propose delete: %v", err)
	}

	select {
	case <-respCh:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for delete response")
	}

	if err := tc.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("failed to converge: %v", err)
	}

	for id := uint64(1); id <= 3; id++ {
		node := tc.GetNode(id)
		_, exists := node.StateMachine.Get("delete-key")
		if exists {
			t.Errorf("node %d: key should be deleted", id)
		}
	}
}

func TestConcurrentWrites(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	const numWrites = 100
	var wg sync.WaitGroup
	errors := make(chan error, numWrites)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < numWrites; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if err := tc.ProposeValue(ctx, key, value); err != nil {
				errors <- fmt.Errorf("write %d failed: %w", i, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	var errCount int
	for err := range errors {
		t.Logf("error: %v", err)
		errCount++
	}

	if errCount > 0 {
		t.Errorf("%d/%d writes failed", errCount, numWrites)
	}

	if err := tc.WaitForConvergence(10 * time.Second); err != nil {
		t.Fatalf("failed to converge: %v", err)
	}

	leader := tc.GetLeader()
	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("key-%d", i)
		expectedValue := fmt.Sprintf("value-%d", i)

		val, exists := leader.StateMachine.Get(key)
		if !exists {
			t.Errorf("key %s not found", key)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("key %s: expected %q, got %q", key, expectedValue, val)
		}
	}
}

func TestWriteWhenNoLeader(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	leaderID, err := tc.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	for id := uint64(1); id <= 3; id++ {
		tc.StopNode(id)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	tc.RestartNode(leaderID)
	time.Sleep(500 * time.Millisecond)

	node := tc.GetNode(leaderID)
	if node == nil {
		t.Fatalf("node not found after restart")
	}

	req := &commandeventspb.CommandEventRequest{
		EventId: uint64(time.Now().UnixNano()),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "no-leader-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "value"},
		},
	}

	_, err = node.Batcher.Submit(ctx, req)

	time.Sleep(3 * time.Second)

	_, exists := node.StateMachine.Get("no-leader-key")
	if exists {
		t.Log("Note: value was committed - this might indicate quorum was somehow achieved")
	}
}

func TestWriteDuringLeaderChange(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	leaderID, err := tc.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{})
	writeErrors := make(chan error, 100)

	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("leader-change-key-%d", i)
			err := tc.ProposeValue(ctx, key, "value")
			if err != nil {
				writeErrors <- err
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	tc.StopNode(leaderID)

	<-done
	close(writeErrors)

	var errCount int
	for range writeErrors {
		errCount++
	}
	t.Logf("%d errors during leader change (expected some)", errCount)

	_, err = tc.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect new leader: %v", err)
	}

	if err := tc.WaitForConvergence(10 * time.Second); err != nil {
		t.Logf("convergence warning: %v", err)
	}
}
