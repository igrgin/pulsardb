package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCmdService_SetAndGet(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, cluster.SetValue(ctx, "mykey", "myvalue"))

	val, exists, err := cluster.GetValue(ctx, "mykey")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "myvalue", val)
}

func TestCmdService_GetNonExistent(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, exists, err := cluster.GetValue(ctx, "nonexistent")
	require.Error(t, err)
	require.False(t, exists)
}

func TestCmdService_Delete(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, cluster.SetValue(ctx, "to-delete", "value"))

	val, exists, err := cluster.GetValue(ctx, "to-delete")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "value", val)

	require.NoError(t, cluster.DeleteValue(ctx, "to-delete"))

	_, exists, err = cluster.GetValue(ctx, "to-delete")
	require.Error(t, err)
	require.False(t, exists)
}

func TestCmdService_Overwrite(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, cluster.SetValue(ctx, "overwrite", "first"))
	require.NoError(t, cluster.SetValue(ctx, "overwrite", "second"))
	require.NoError(t, cluster.SetValue(ctx, "overwrite", "third"))

	val, exists, err := cluster.GetValue(ctx, "overwrite")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "third", val)
}

func TestCmdService_DeleteNonExistent(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = cluster.DeleteValue(ctx, "does-not-exist")
	require.NoError(t, err)
}

func TestCmdService_SequentialOperations(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "sequential"

	for i := 0; i < 20; i++ {
		value := fmt.Sprintf("iteration-%d", i)
		require.NoError(t, cluster.SetValue(ctx, key, value))

		val, exists, err := cluster.GetValue(ctx, key)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, value, val)
	}
}

func TestCmdService_RapidSetDelete(t *testing.T) {
	cluster := NewCommandTestCluster(t)
	defer cluster.Cleanup()

	require.NoError(t, cluster.StartNodes(3))
	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "toggle"

	for i := 0; i < 15; i++ {
		require.NoError(t, cluster.SetValue(ctx, key, fmt.Sprintf("v%d", i)))
		require.NoError(t, cluster.DeleteValue(ctx, key))
	}

	require.NoError(t, cluster.WaitForConvergence(5*time.Second))

	_, exists, err := cluster.GetValue(ctx, key)
	require.Error(t, err)
	require.False(t, exists)
}
