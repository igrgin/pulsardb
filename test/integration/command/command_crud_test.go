package integration

import (
	"context"
	"fmt"
	"pulsardb/internal/command"
	"pulsardb/test/integration/helper"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCmdService_SetAndGet(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "warn")

	cluster.StartNodes(3, 60)

	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, cluster.Set(ctx, "mykey", "myvalue"))

	val, exists, err := cluster.Get(ctx, "mykey")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "myvalue", val)
}

func TestCmdService_GetNonExistent(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "warn")

	cluster.StartNodes(3, 60)

	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, exists, err := cluster.Get(ctx, "nonexistent")
	require.Error(t, err)
	require.False(t, exists)
	require.ErrorContains(t, err, "key not found")
}

func TestCmdService_Delete(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "warn")

	cluster.StartNodes(3, 60)

	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, cluster.Set(ctx, "to-delete", "value"))

	val, exists, err := cluster.Get(ctx, "to-delete")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "value", val)

	require.NoError(t, cluster.Delete(ctx, "to-delete"))

	_, exists, err = cluster.Get(ctx, "to-delete")
	require.ErrorIs(t, err, command.ErrKeyNotFound)
	require.False(t, exists)
}

func TestCmdService_Overwrite(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "warn")

	cluster.StartNodes(3, 60)

	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, cluster.Set(ctx, "overwrite", "first"))
	require.NoError(t, cluster.Set(ctx, "overwrite", "second"))
	require.NoError(t, cluster.Set(ctx, "overwrite", "third"))

	val, exists, err := cluster.Get(ctx, "overwrite")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "third", val)
}

func TestCmdService_DeleteNonExistent(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "warn")

	cluster.StartNodes(3, 60)

	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = cluster.Delete(ctx, "does-not-exist")
	require.NoError(t, err)
}

func TestCmdService_SequentialOperations(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "warn")

	cluster.StartNodes(3, 60)

	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "sequential"

	for i := 0; i < 20; i++ {
		value := fmt.Sprintf("iteration-%d", i)
		require.NoError(t, cluster.Set(ctx, key, value))

		val, exists, err := cluster.Get(ctx, key)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, value, val)
	}
}

func TestCmdService_RapidSetDelete(t *testing.T) {
	cluster := helper.NewCluster(t, nil, "warn")

	cluster.StartNodes(3, 60)

	_, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "toggle"

	for i := 0; i < 15; i++ {
		require.NoError(t, cluster.Set(ctx, key, fmt.Sprintf("v%d", i)))
		require.NoError(t, cluster.Delete(ctx, key))
	}

	require.NoError(t, cluster.WaitForConvergence(10*time.Second))

	_, exists, err := cluster.Get(ctx, key)
	require.Error(t, err)
	require.ErrorIs(t, err, command.ErrKeyNotFound)
	require.False(t, exists)
}
