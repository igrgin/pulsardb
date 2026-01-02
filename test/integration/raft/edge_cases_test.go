package integration

import (
	"context"
	"errors"
	"fmt"
	commandeventspb "pulsardb/internal/transport/gen/command"
	"pulsardb/test/integration/helper"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEmptyProposal(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	leader := c.GetLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: ""},
		},
	}

	resp, err := leader.CmdService.ProcessCommand(ctx, req)
	if err != nil {
		t.Logf("empty proposal rejected: %v", err)
		return
	}

	t.Logf("empty proposal response: success=%v", resp.Success)
}

func TestLargeValue(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	leader := c.GetLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	largeValue := strings.Repeat("x", 100*1024)

	req := &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "large-value-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: largeValue},
		},
	}

	resp, err := leader.CmdService.ProcessCommand(ctx, req)
	if err != nil {
		t.Fatalf("large proposal failed: %v", err)
	}

	if !resp.Success {
		t.Error("large value proposal failed")
	} else {
		t.Log("large value proposal succeeded")
	}

	if err := c.WaitForConvergence(10 * time.Second); err != nil {
		t.Logf("convergence warning: %v", err)
	}
}

func TestRapidLeaderChurn(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(5, 60)

	if _, err := c.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		leader := c.GetLeader()
		if leader == nil {
			t.Logf("iteration %d: no leader, waiting", i)
			time.Sleep(2 * time.Second)
			continue
		}

		leaderID := leader.ID
		t.Logf("Iteration %d: killing leader %d", i, leaderID)

		writeErr := c.Set(ctx, fmt.Sprintf("churn-pre-key-%d", i), "value")
		if writeErr != nil {
			t.Logf("pre-kill write failed: %v", writeErr)
		}

		c.StopNode(leaderID)

		newLeaderID, err := c.WaitForNewLeader(leaderID, 10*time.Second)
		if err != nil {
			t.Logf("iteration %d: failed to elect new leader: %v", i, err)

			c.RestartNode(leaderID)
			time.Sleep(2 * time.Second)
			continue
		}

		t.Logf("Iteration %d: new leader is %d", i, newLeaderID)

		if err := c.Set(ctx, fmt.Sprintf("churn-post-key-%d", i), "value"); err != nil {
			t.Logf("post-kill write failed: %v", err)
		}

		c.RestartNode(leaderID)
		time.Sleep(2 * time.Second)
	}

	if _, err := c.WaitForLeader(15 * time.Second); err != nil {
		t.Errorf("cluster failed to stabilize: %v", err)
	}
}

func TestConcurrentProposalsAndReads(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	writeErrors := make(chan error, 100)
	readErrors := make(chan error, 100)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("concurrent-key-%d-%d", i, j)
				if err := c.Set(ctx, key, "value"); err != nil {
					writeErrors <- err
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}

	leader := c.GetLeader()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				idx, err := leader.RaftService.GetReadIndex(ctx)
				if err != nil {
					readErrors <- err
					return
				}
				if err := c.WaitForApplied(idx, 5*time.Second); err != nil {
					readErrors <- err
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	close(writeErrors)
	close(readErrors)

	var writeErrCount, readErrCount int
	for err := range writeErrors {
		t.Logf("write error: %v", err)
		writeErrCount++
	}
	for err := range readErrors {
		t.Logf("read error: %v", err)
		readErrCount++
	}

	t.Logf("Write errors: %d, Read errors: %d", writeErrCount, readErrCount)
}

func TestProposalAfterContextCancel(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	leader := c.GetLeader()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "cancelled-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "value"},
		},
	}

	_, err := leader.CmdService.ProcessCommand(ctx, req)

	if errors.Is(err, context.Canceled) {
		t.Log("proposal correctly rejected with cancelled context")
	} else {
		t.Logf("proposal result: %v", err)
	}
}

func TestMultipleValuesForSameKey(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "overwrite-key"

	for i := 0; i < 10; i++ {
		value := fmt.Sprintf("value-%d", i)
		if err := c.Set(ctx, key, value); err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	if err := c.WaitForConvergence(5 * time.Second); err != nil {
		t.Fatalf("convergence failed: %v", err)
	}

	for id := uint64(1); id <= 3; id++ {
		node := c.GetNode(id)
		val, exists := node.StorageService.Get(key)
		if !exists {
			t.Errorf("node %d: key not found", id)
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			t.Errorf("node %d: expected string value, got %T", id, val)
			continue
		}
		if strVal != "value-9" {
			t.Errorf("node %d: expected 'value-9', got %q", id, strVal)
		}
	}
}

func TestNodeIDZeroHandling(t *testing.T) {
	c := helper.NewCluster(t, nil, "info")

	c.StartNodes(3, 60)

	if _, err := c.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	node := c.GetNode(0)
	if node != nil {
		t.Error("should not have node for ID 0")
	}

	node = c.GetNode(999)
	if node != nil {
		t.Error("should not have node for non-existent ID 999")
	}
}
