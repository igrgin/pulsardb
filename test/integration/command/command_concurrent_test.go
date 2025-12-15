package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCmdService_ConcurrentWritesDifferentKeys(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const numWrites = 50
	var wg sync.WaitGroup
	errors := make(chan error, numWrites)

	for i := 0; i < numWrites; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if err := cluster.SetValue(ctx, key, value); err != nil {
				errors <- fmt.Errorf("write %d: %w", i, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	var errCount int
	for err := range errors {
		t.Log(err)
		errCount++
	}
	require.Zero(t, errCount)

	require.NoError(t, cluster.WaitForConvergence(10*time.Second))

	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("concurrent-%d", i)
		consistent, err := cluster.VerifyConsistency(key)
		require.NoError(t, err)
		require.True(t, consistent, "key %s should be consistent", key)
	}
}

func TestCmdService_ConcurrentWritesSameKey(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const numWrites = 30
	var wg sync.WaitGroup
	var successCount atomic.Int32

	for i := 0; i < numWrites; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := cluster.SetValue(ctx, "contended", fmt.Sprintf("value-%d", i)); err == nil {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	require.Equal(t, int32(numWrites), successCount.Load())

	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	consistent, err := cluster.VerifyConsistency("contended")
	require.NoError(t, err)
	require.True(t, consistent)
}

func TestCmdService_BulkOperations(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const numKeys = 200
	var wg sync.WaitGroup
	var successCount atomic.Int32

	for i := 0; i < numKeys; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := cluster.SetValue(ctx, fmt.Sprintf("bulk-%d", i), fmt.Sprintf("val-%d", i)); err == nil {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("successful: %d/%d", successCount.Load(), numKeys)
	require.GreaterOrEqual(t, successCount.Load(), int32(numKeys*9/10))

	require.NoError(t, cluster.WaitForConvergence(15*time.Second))
}

func TestCmdService_ConcurrentReadsAndWrites(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		require.NoError(t, cluster.SetValue(ctx, fmt.Sprintf("rw-key-%d", i), fmt.Sprintf("initial-%d", i)))
	}

	var wg sync.WaitGroup
	var readSuccess, writeSuccess atomic.Int32

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("rw-key-%d", i%10)
			if _, exists, err := cluster.GetValue(ctx, key); err == nil && exists {
				readSuccess.Add(1)
			}
		}(i)
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("rw-key-%d", i%10)
			if err := cluster.SetValue(ctx, key, fmt.Sprintf("updated-%d", i)); err == nil {
				writeSuccess.Add(1)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("reads: %d, writes: %d", readSuccess.Load(), writeSuccess.Load())
	require.GreaterOrEqual(t, readSuccess.Load(), int32(15))
	require.GreaterOrEqual(t, writeSuccess.Load(), int32(15))
}
