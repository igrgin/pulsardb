package coordinator

import (
	"errors"
	"fmt"
	"log/slog"
	"pulsardb/internal/metrics"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func (c *Coordinator) maybeTriggerSnapshot(appliedIndex uint64) error {
	if c.snapCount == 0 {
		return nil
	}

	snapIndex := c.snapshotIndex()

	if appliedIndex <= snapIndex {
		return nil
	}

	if (appliedIndex - snapIndex) >= c.snapCount {
		slog.Debug("snapshot threshold reached",
			"lastApplied", appliedIndex,
			"snapIndex", snapIndex,
			"snapCount", c.snapCount,
		)
		return c.triggerSnapshot(appliedIndex, nil)
	}

	return nil
}

func (c *Coordinator) triggerSnapshot(appliedIndex uint64, confState *raftpb.ConfState) error {
	start := time.Now()

	if appliedIndex == 0 {
		slog.Debug("skip snapshot: no applied entries")
		return nil
	}

	slog.Debug("triggering snapshot", "lastApplied", appliedIndex)

	data, err := c.store.Snapshot()
	metrics.StorageSnapshotSize.Set(float64(len(data)))

	if err != nil {
		return fmt.Errorf("get snapshot data: %w", err)
	}

	if len(data) == 0 {
		slog.Debug("no application data to snapshot",
			"last_applied", appliedIndex,
			"storage_len", c.store.Len(),
		)
		return nil
	}

	metrics.StorageSnapshotSize.Set(float64(len(data)))

	snap, err := c.node.Storage().CreateSnapshot(appliedIndex, confState, data)
	if err != nil {
		if errors.Is(err, etcdraft.ErrSnapOutOfDate) {
			slog.Debug("snapshot already exists", "index", appliedIndex)
			return nil
		}
		return fmt.Errorf("create snapshot: %w", err)
	}

	if err := c.node.Storage().SaveSnapshot(snap); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}

	compactIndex := uint64(1)
	if appliedIndex > c.snapCount {
		compactIndex = appliedIndex - c.snapCount
	}

	if err := c.node.Storage().Compact(compactIndex); err != nil {
		slog.Warn("compact failed", "error", err)
	}

	metrics.RaftSnapshotsTotal.Inc()
	metrics.RaftSnapshotDuration.Observe(time.Since(start).Seconds())

	slog.Info("triggered snapshot",
		"index", snap.Metadata.Index,
		"term", snap.Metadata.Term,
		"compact_index", compactIndex,
		"data_size", len(data),
	)

	return nil
}

func (c *Coordinator) snapshotIndex() uint64 {
	return c.node.Storage().SnapshotIndex()
}

func (c *Coordinator) snapshotData() []byte {
	return c.node.Storage().SnapshotData()
}
