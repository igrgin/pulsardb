package integration

import (
	"context"
	"fmt"
	"pulsardb/internal/raft/coordinator"
	commandeventspb "pulsardb/internal/transport/gen/command"
	"pulsardb/test/integration/helper"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatchBySize(t *testing.T) {
	cfg := DefaultTestClusterConfigurations()
	c := helper.NewCluster(t, cfg, "info")
	c.StartNodes(3, 60, false)

	_, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	leader := c.GetLeader()
	require.NotNil(t, leader)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	batchSize := 10
	var wg sync.WaitGroup
	var errCount int
	var mu sync.Mutex

	for i := 0; i < batchSize; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := &commandeventspb.CommandEventRequest{
				EventId: helper.NewEventID(),
				Type:    commandeventspb.CommandEventType_SET,
				Key:     "batch-key",
				Value: &commandeventspb.CommandEventValue{
					Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "batch-value"},
				},
			}

			resp, err := leader.CmdService.ProcessCommand(ctx, req)
			if err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
				t.Errorf("request %d error: %v", i, err)
				return
			}
			if !resp.Success {
				mu.Lock()
				errCount++
				mu.Unlock()
				t.Errorf("request %d failed", i)
			}
		}(i)
	}

	wg.Wait()
	require.Zero(t, errCount, "expected all batch requests to succeed")
}

func TestBatchByTime(t *testing.T) {
	cfg := DefaultTestClusterConfigurations()
	cfg.BatchSize = 100

	c := helper.NewCluster(t, cfg, "info")
	c.StartNodes(3, 60, false)

	_, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	leader := c.GetLeader()
	require.NotNil(t, leader)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "time-batch-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "time-batch-value"},
		},
	}

	start := time.Now()
	resp, err := leader.CmdService.ProcessCommand(ctx, req)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.True(t, resp.Success, "expected success, got error: %v", resp.GetError())

	t.Logf("response came in %v (batch timer: %v)", elapsed, cfg.BatchWait)
}

func TestMixedBatchTriggers(t *testing.T) {
	cfg := DefaultTestClusterConfigurations()
	c := helper.NewCluster(t, cfg, "info")
	c.StartNodes(3, 60, false)

	_, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	leader := c.GetLeader()
	require.NotNil(t, leader)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var successCount int
	var mu sync.Mutex

	for wave := 0; wave < 5; wave++ {

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(w, i int) {
				defer wg.Done()
				req := &commandeventspb.CommandEventRequest{
					EventId: helper.NewEventID(),
					Type:    commandeventspb.CommandEventType_SET,
					Key:     fmt.Sprintf("mixed-key-%d-%d", w, i),
					Value: &commandeventspb.CommandEventValue{
						Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "value"},
					},
				}
				resp, err := leader.CmdService.ProcessCommand(ctx, req)
				if err == nil && resp.Success {
					mu.Lock()
					successCount++
					mu.Unlock()
				}
			}(wave, i)
		}

		time.Sleep(20 * time.Millisecond)

		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(w, i int) {
				defer wg.Done()
				req := &commandeventspb.CommandEventRequest{
					EventId: helper.NewEventID(),
					Type:    commandeventspb.CommandEventType_SET,
					Key:     fmt.Sprintf("mixed-partial-key-%d-%d", w, i),
					Value: &commandeventspb.CommandEventValue{
						Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "value"},
					},
				}
				resp, err := leader.CmdService.ProcessCommand(ctx, req)
				if err == nil && resp.Success {
					mu.Lock()
					successCount++
					mu.Unlock()
				}
			}(wave, i)
		}

		time.Sleep(20 * time.Millisecond)
	}

	wg.Wait()

	expectedTotal := 5 * (10 + 3)
	t.Logf("Success: %d/%d", successCount, expectedTotal)

	require.GreaterOrEqual(t, successCount, expectedTotal/2,
		"too many failures: %d/%d succeeded", successCount, expectedTotal)
}

func TestFailBatchNoQuorum(t *testing.T) {
	cfg := DefaultTestClusterConfigurations()
	cfg.BatchSize = 1
	cfg.BatchWait = 10 * time.Millisecond

	c := helper.NewCluster(t, cfg, "info")
	c.StartNodes(3, 60, false)

	_, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	leader := c.GetLeader()
	require.NotNil(t, leader)

	followers := c.GetFollowers()
	require.Len(t, followers, 2)
	for _, f := range followers {
		require.NoError(t, c.StopNode(f.ID))
	}

	require.Eventually(t, func() bool {
		return !leader.Coordinator.IsLeader()
	}, 6*time.Second, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "failbatch-noquorum-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "value"},
		},
	}

	_, err = leader.CmdService.ProcessCommand(ctx, req)
	require.Error(t, err)
	require.ErrorIs(t, err, coordinator.ErrNoLeader)
}

func TestFlushSkippedAllCommandsExpired(t *testing.T) {
	cfg := DefaultTestClusterConfigurations()

	cfg.BatchSize = 100

	cfg.BatchWait = 50 * time.Millisecond

	cfg.CleanupTickInterval = 10 * time.Millisecond

	c := helper.NewCluster(t, cfg, "info")
	c.StartNodes(3, 60, false)

	_, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	leader := c.GetLeader()
	require.NotNil(t, leader)

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer shortCancel()

	req := &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "flush-skipped-expired-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "value"},
		},
	}

	resp, err := leader.CmdService.ProcessCommand(shortCtx, req)
	require.Error(t, err)
	require.Nil(t, resp)

	time.Sleep(cfg.BatchWait + 30*time.Millisecond)

	leader.CmdService.Batcher.Mu.Lock()
	require.Len(t, leader.CmdService.Batcher.Pending, 0)
	require.Nil(t, leader.CmdService.Batcher.Timer)
	leader.CmdService.Batcher.Mu.Unlock()

	okCtx, okCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer okCancel()

	okReq := &commandeventspb.CommandEventRequest{
		EventId: helper.NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "flush-skipped-expired-key-2",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "value2"},
		},
	}

	okResp, err := leader.CmdService.ProcessCommand(okCtx, okReq)
	require.NoError(t, err)
	require.True(t, okResp.GetSuccess(), "expected success, got error: %v", okResp.GetError())
}

func DefaultTestClusterConfigurations() *helper.TestClusterConfig {
	cfg := &helper.TestClusterConfig{
		TickInterval:           100 * time.Millisecond,
		ElectionTick:           10,
		BatchSize:              10,
		BatchWait:              50 * time.Millisecond,
		PromotionThreshold:     100,
		PromotionCheckInterval: 5 * time.Second,
	}
	return cfg
}
