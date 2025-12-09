package raft

import (
	"errors"
	"log/slog"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	tickInterval = 100 * time.Millisecond
	snapCount    = uint64(10000) // Snapshot every 10000 entries
)

// startLoop begins the main raft event loop in a goroutine.
func (n *Node) startLoop() {
	n.stoppedWg.Add(1)

	go func() {
		defer n.stoppedWg.Done()
		n.runLoop()
	}()
}

// runLoop is the main raft event loop.
func (n *Node) runLoop() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	slog.Info("raft loop started", "node_id", n.Id)

	// Track snapshot index for triggering new snapshots
	snapshotIndex := n.lastApplied

	for {
		select {
		case <-n.stopCh:
			slog.Info("raft loop stopping", "node_id", n.Id)
			return

		case <-ticker.C:
			n.raftNode.Tick()

		case rd, ok := <-n.raftNode.Ready():
			if !ok {
				slog.Info("raft Ready channel closed", "node_id", n.Id)
				return
			}

			if err := n.processReady(rd); err != nil {
				slog.Error("failed to process raft Ready", "node_id", n.Id, "error", err)
				return
			}

			// Check if we should trigger a snapshot
			if n.lastApplied-snapshotIndex >= snapCount {
				if err := n.triggerSnapshot(); err != nil {
					slog.Error("failed to trigger snapshot",
						"node_id", n.Id,
						"error", err,
					)
				} else {
					snapshotIndex = n.lastApplied
				}
			}
		}
	}
}

// processReady handles a raft Ready batch.
func (n *Node) processReady(rd etcdraft.Ready) error {
	// 1) Handle snapshot from leader (if any)
	if !etcdraft.IsEmptySnap(rd.Snapshot) {
		slog.Info("received snapshot from leader",
			"node_id", n.Id,
			"index", rd.Snapshot.Metadata.Index,
			"term", rd.Snapshot.Metadata.Term,
		)
		if err := n.applySnapshot(rd.Snapshot); err != nil {
			return err
		}
	}

	// 2) Persist snapshot, entries, and hard state
	if err := n.storage.SaveReady(rd); err != nil {
		return err
	}

	// 3) Apply committed entries to the state machine
	if len(rd.CommittedEntries) > 0 {
		n.applyCommitted(rd.CommittedEntries)
	}

	// 4) Compute commit index for ReadIndex waiters
	commitIdx := n.computeCommitIndex(rd)

	// 5) Wake up ReadIndex waiters
	n.handleReadStates(rd.ReadStates, commitIdx)

	// 6) Send outgoing raft messages (async)
	n.sendMessages(rd.Messages)

	// 7) Signal raft that we're done with this Ready batch
	n.raftNode.Advance()

	return nil
}

// applySnapshot applies a snapshot received from the leader.
func (n *Node) applySnapshot(snap raftpb.Snapshot) error {
	if len(snap.Data) > 0 {
		if err := n.storeSvc.RestoreFromSnapshot(snap.Data); err != nil {
			return err
		}
		slog.Info("applied snapshot to storage",
			"node_id", n.Id,
			"index", snap.Metadata.Index,
		)
	}

	// Update confState from snapshot
	n.confState = snap.Metadata.ConfState
	if err := n.storage.SaveConfState(snap.Metadata.ConfState); err != nil {
		slog.Error("failed to save confState from snapshot", "error", err)
	}

	n.lastApplied = snap.Metadata.Index
	return nil
}

// triggerSnapshot creates a snapshot of the current state.
func (n *Node) triggerSnapshot() error {
	if n.getSnapshot == nil {
		return nil
	}

	data, err := n.getSnapshot()
	if err != nil {
		return err
	}

	snap, err := n.storage.RaftStorage().CreateSnapshot(n.lastApplied, &n.confState, data)
	if err != nil {
		return err
	}

	if err := n.storage.SaveSnapshot(snap); err != nil {
		return err
	}

	// Compact the raft log to free memory
	compactIndex := uint64(1)
	if n.lastApplied > snapCount {
		compactIndex = n.lastApplied - snapCount
	}
	if err := n.storage.RaftStorage().Compact(compactIndex); err != nil {
		if !errors.Is(err, etcdraft.ErrCompacted) {
			slog.Warn("failed to compact raft log", "error", err)
		}
	}

	slog.Info("created snapshot",
		"node_id", n.Id,
		"index", n.lastApplied,
		"compact_index", compactIndex,
	)

	return nil
}

// computeCommitIndex calculates the current commit index from Ready.
func (n *Node) computeCommitIndex(rd etcdraft.Ready) uint64 {
	commitIdx := rd.HardState.Commit
	if len(rd.CommittedEntries) > 0 {
		last := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		if last.Index > commitIdx {
			commitIdx = last.Index
		}
	}
	return commitIdx
}
