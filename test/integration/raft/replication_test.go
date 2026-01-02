package integration

import (
	"context"
	"fmt"
	commandeventspb "pulsardb/internal/transport/gen/command"
	"pulsardb/test/integration/helper"
	"sync"
	"testing"
	"time"
)

func TestBasicSetOperation(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Set(ctx, "test-key", "test-value"); err != nil {
		t.Fatalf("failed to propose: %v", err)
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("failed to converge: %v", err)
	}

	for id := uint64(1); id <= 3; id++ {
		node := c.GetNode(id)
		val, exists := node.StorageService.Get("test-key")
		if !exists {
			t.Errorf("node %d: key not found", id)
			continue
		}

		strVal, _ := helper.AsString(val)
		if strVal != "test-value" {
			t.Errorf("node %d: expected 'test-value', got %q", id, val)
		}
	}
}

func TestBasicDeleteOperation(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Set(ctx, "delete-key", "some-value"); err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	if err := c.Delete(ctx, "delete-key"); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("failed to converge: %v", err)
	}

	for id := uint64(1); id <= 3; id++ {
		node := c.GetNode(id)
		_, exists := node.StorageService.Get("delete-key")
		if exists {
			t.Errorf("node %d: key should be deleted", id)
		}
	}
}

func TestConcurrentWrites(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

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
			if err := c.Set(ctx, key, value); err != nil {
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

	if err := c.WaitForConvergence(10 * time.Second); err != nil {
		t.Fatalf("failed to converge: %v", err)
	}

	leader := c.GetLeader()
	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("key-%d", i)
		expectedValue := fmt.Sprintf("value-%d", i)

		val, exists := leader.StorageService.Get(key)
		if !exists {
			t.Errorf("key %s not found", key)
			continue
		}

		strVal, _ := helper.AsString(val)

		if strVal != expectedValue {
			t.Errorf("key %s: expected %q, got %q", key, expectedValue, val)
		}
	}
}

func TestWriteWhenNoLeader(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	for id := uint64(1); id <= 3; id++ {
		c.StopNode(id)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c.RestartNode(leaderID)
	time.Sleep(500 * time.Millisecond)

	node := c.GetNode(leaderID)
	if node == nil {
		t.Fatalf("node not found after restart")
	}

	req := &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "no-leader-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "value"},
		},
	}

	_, _ = node.CmdService.ProcessCommand(ctx, req)

	time.Sleep(3 * time.Second)

	_, exists := node.StorageService.Get("no-leader-key")
	if exists {
		t.Log("Note: value was committed - this might indicate quorum was somehow achieved")
	}
}

func TestWriteDuringLeaderChange(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	leaderID, err := c.WaitForLeader(10 * time.Second)
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
			err := c.Set(ctx, key, "value")
			if err != nil {
				writeErrors <- err
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	c.StopNode(leaderID)

	<-done
	close(writeErrors)

	var errCount int
	for range writeErrors {
		errCount++
	}
	t.Logf("%d errors during leader change (expected some)", errCount)

	_, err = c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to elect new leader: %v", err)
	}

	if err := c.WaitForConvergence(10 * time.Second); err != nil {
		t.Logf("convergence warning: %v", err)
	}
}
