package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"pulsardb/internal/transport/gen/commandevents"

	"github.com/stretchr/testify/require"
)

func TestBatchBySize(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	leader := tc.GetLeader()
	batchSize := 10
	require.NotNil(t, leader)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	responses := make([]chan *commandeventspb.CommandEventResponse, batchSize)

	for i := 0; i < batchSize; i++ {
		req := &commandeventspb.CommandEventRequest{
			EventId: uint64(time.Now().UnixNano()) + uint64(i),
			Type:    commandeventspb.CommandEventType_SET,
			Key:     "batch-key",
			Value: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "batch-value"},
			},
		}

		respCh, err := leader.Batcher.Submit(ctx, req)
		if err != nil {
			t.Fatalf("propose %d failed: %v", i, err)
		}
		responses[i] = respCh
	}

	for i, respCh := range responses {
		wg.Add(1)
		go func(i int, ch chan *commandeventspb.CommandEventResponse) {
			defer wg.Done()
			select {
			case resp := <-ch:
				if !resp.Success {
					t.Errorf("request %d failed", i)
				}
			case <-ctx.Done():
				t.Errorf("request %d timed out", i)
			}
		}(i, respCh)
	}

	wg.Wait()
}

func TestBatchByTime(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	leader := tc.GetLeader()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &commandeventspb.CommandEventRequest{
		EventId: uint64(time.Now().UnixNano()),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "time-batch-key",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "time-batch-value"},
		},
	}

	start := time.Now()
	respCh, err := leader.Batcher.Submit(ctx, req)
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	select {
	case resp := <-respCh:
		elapsed := time.Since(start)
		if !resp.Success {
			t.Errorf("request failed")
		}
		if elapsed < 5*time.Millisecond {
			t.Logf("response came in %v (batch timer: 5ms)", elapsed)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for response")
	}
}

func TestMixedBatchTriggers(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Cleanup()

	if err := tc.StartNodes(3); err != nil {
		t.Fatalf("failed to start nodes: %v", err)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to elect leader: %v", err)
	}

	leader := tc.GetLeader()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for wave := 0; wave < 5; wave++ {

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(w, i int) {
				defer wg.Done()
				req := &commandeventspb.CommandEventRequest{
					EventId: uint64(time.Now().UnixNano()),
					Type:    commandeventspb.CommandEventType_SET,
					Key:     "mixed-key",
					Value: &commandeventspb.CommandEventValue{
						Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "value"},
					},
				}
				respCh, err := leader.Batcher.Submit(ctx, req)
				if err != nil {
					return
				}
				select {
				case resp := <-respCh:
					if resp.Success {
						mu.Lock()
						successCount++
						mu.Unlock()
					}
				case <-ctx.Done():
				}
			}(wave, i)
		}

		time.Sleep(20 * time.Millisecond)

		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(w, i int) {
				defer wg.Done()
				req := &commandeventspb.CommandEventRequest{
					EventId: uint64(time.Now().UnixNano()),
					Type:    commandeventspb.CommandEventType_SET,
					Key:     "mixed-partial-key",
					Value: &commandeventspb.CommandEventValue{
						Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "value"},
					},
				}
				respCh, err := leader.Batcher.Submit(ctx, req)
				if err != nil {
					return
				}
				select {
				case resp := <-respCh:
					if resp.Success {
						mu.Lock()
						successCount++
						mu.Unlock()
					}
				case <-ctx.Done():
				}
			}(wave, i)
		}

		time.Sleep(20 * time.Millisecond)
	}

	wg.Wait()

	expectedTotal := 5 * (10 + 3)
	t.Logf("Success: %d/%d", successCount, expectedTotal)

	if successCount < expectedTotal/2 {
		t.Errorf("too many failures: %d/%d succeeded", successCount, expectedTotal)
	}
}
