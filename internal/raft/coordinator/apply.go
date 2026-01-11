package coordinator

import (
	"fmt"
	"log/slog"
	"pulsardb/convert"
	"pulsardb/internal/metrics"
	"pulsardb/internal/raft/ops"
	commandeventspb "pulsardb/internal/transport/gen/command"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func (c *Coordinator) applyEntries(entries []raftpb.Entry) (uint64, error) {
	if len(entries) > 0 {
		slog.Debug("applying committed entries", "count", len(entries))
	}

	var lastIndex uint64
	for _, entry := range entries {
		switch entry.Type {
		case raftpb.EntryConfChange:
			c.applyConfChange(entry)
		case raftpb.EntryNormal:
			c.applyNormalEntry(entry)
		default:
			slog.Warn("ignoring unsupported raft entry type",
				"node_id", c.node.ID(),
				"index", entry.Index,
				"term", entry.Term,
				"type", entry.Type,
			)
		}

		lastIndex = entry.Index
		c.SetLastApplied(entry.Index)
	}

	return lastIndex, nil
}

func (c *Coordinator) applyNormalEntry(entry raftpb.Entry) {
	slog.Debug("applying entry", "index", entry.Index, "term", entry.Term, "data_len", len(entry.Data))
	if len(entry.Data) == 0 {
		return
	}
	start := time.Now()
	err := c.stateMachine.Apply(entry.Data)
	metrics.CommandDuration.WithLabelValues("apply").Observe(time.Since(start).Seconds())

	if err != nil {
		metrics.CommandsTotal.WithLabelValues("apply", "error").Inc()
		slog.Error("state machine apply failed",
			"node_id", c.node.ID(),
			"index", entry.Index,
			"error", err,
		)
	} else {
		metrics.CommandsTotal.WithLabelValues("apply", "success").Inc()
	}
}

func (c *Coordinator) applyConfChange(entry raftpb.Entry) {
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		slog.Error("failed to unmarshal conf change",
			"node_id", c.node.ID(),
			"index", entry.Index,
			"error", err,
		)
		return
	}

	slog.Debug("applying conf change",
		"node_id", c.node.ID(),
		"type", cc.Type,
		"target_node", cc.NodeID,
		"index", entry.Index,
	)

	confState := c.node.ApplyConfChange(cc)
	if confState != nil {
		c.node.SetConfState(*confState)
		if err := c.node.Storage().SaveConfState(*confState); err != nil {
			slog.Error("failed to persist confState",
				"node_id", c.node.ID(),
				"error", err,
			)
		}
	}

	c.handleConfChangeEffect(cc)
}

func (c *Coordinator) handleConfChangeEffect(cc raftpb.ConfChange) {
	isLeader := c.IsLeader()
	lastApplied := c.LastApplied()

	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		c.handleAddNode(cc, isLeader, lastApplied)
	case raftpb.ConfChangeRemoveNode:
		c.handleRemoveNode(cc)
	case raftpb.ConfChangeAddLearnerNode:
		c.handleAddLearner(cc, isLeader, lastApplied)
	default:
		slog.Debug("unhandled conf change type",
			"node_id", c.node.ID(),
			"type", cc.Type,
		)
	}
}

func (c *Coordinator) recoverState() error {
	snapIndex := c.snapshotIndex()
	snapData := c.snapshotData()

	slog.Info("recovering state", "node_id", c.node.ID())

	if len(snapData) > 0 {
		if err := c.store.Restore(snapData); err != nil {
			return fmt.Errorf("restore state machine from snapshot: %w", err)
		}
		slog.Info("restored application state from snapshot", "index", snapIndex)
	}

	entries, err := c.node.Storage().EntriesAfter(snapIndex)
	if err != nil {
		return fmt.Errorf("get entries after snapshot: %w", err)
	}

	if len(entries) == 0 {
		if snapIndex > c.LastApplied() {
			c.SetLastApplied(snapIndex)
		}
		slog.Debug("no entries to replay", "snapIndex", snapIndex)
		return nil
	}

	slog.Debug("replaying entries", "count", len(entries), "afterIndex", snapIndex)
	for _, entry := range entries {
		if err := c.replayEntry(entry); err != nil {
			slog.Error("failed to replay entry",
				"node_id", c.node.ID(),
				"index", entry.Index,
				"error", err,
			)
		}
	}

	lastIndex := entries[len(entries)-1].Index
	c.SetLastApplied(lastIndex)

	slog.Info("replayed entries to application",
		"count", len(entries),
		"last_index", lastIndex,
	)

	return nil
}

func (c *Coordinator) replayEntry(entry raftpb.Entry) error {
	switch entry.Type {
	case raftpb.EntryConfChange:
		return nil

	case raftpb.EntryNormal:
		if len(entry.Data) == 0 {
			return nil
		}

		var batch commandeventspb.BatchedCommands
		if err := proto.Unmarshal(entry.Data, &batch); err == nil && len(batch.Commands) > 0 {
			for _, req := range batch.Commands {
				if err := c.applyToStorage(req); err != nil {
					return err
				}
			}
			return nil
		}

		var req commandeventspb.CommandEventRequest
		if err := proto.Unmarshal(entry.Data, &req); err != nil {
			return err
		}
		return c.applyToStorage(&req)

	default:
		return nil
	}
}

func (c *Coordinator) applyToStorage(req *commandeventspb.CommandEventRequest) error {
	switch req.GetType() {
	case commandeventspb.CommandEventType_SET:
		val := convert.FromCommandProto(req.GetValue())
		c.store.Set(req.GetKey(), val)
		slog.Debug("replayed SET", "key", req.GetKey())
	case commandeventspb.CommandEventType_DELETE:
		c.store.Delete(req.GetKey())
		slog.Debug("replayed DELETE", "key", req.GetKey())
	}
	return nil
}

func (c *Coordinator) applyLeaderSnapshot(snap raftpb.Snapshot) error {
	slog.Info("applying snapshot to application",
		"node_id", c.node.ID(),
		"index", snap.Metadata.Index,
		"term", snap.Metadata.Term,
		"data_size", len(snap.Data),
	)

	if len(snap.Data) > 0 {
		if err := c.store.Restore(snap.Data); err != nil {
			slog.Error("failed to restore from snapshot", "node_id", c.node.ID(), "error", err)
			return err
		}
		slog.Info("application state restored from snapshot", "node_id", c.node.ID())
	} else {
		slog.Warn("snapshot has no data, skipping application restore",
			"node_id", c.node.ID(),
			"index", snap.Metadata.Index,
		)
	}

	c.node.SetConfState(snap.Metadata.ConfState)
	c.SetLastApplied(snap.Metadata.Index)

	return nil
}

func (c *Coordinator) handleAddLearner(cc raftpb.ConfChange, isLeader bool, lastApplied uint64) {
	if cc.NodeID == c.node.ID() {
		slog.Info("this node added as learner", "node_id", c.node.ID())
		return
	}

	raftAddr, clientAddr := ops.DecodePeerMetadata(cc.Context)

	c.transport.AddPeer(cc.NodeID, raftAddr, clientAddr)

	if err := c.transport.InitPeerClient(cc.NodeID, raftAddr); err != nil {
		slog.Error("failed to init client for learner",
			"learner_id", cc.NodeID,
			"error", err,
		)
	}

	c.transport.StartPeerSender(cc.NodeID, c.sendQueueSize)

	slog.Info("learner added to cluster",
		"node_id", c.node.ID(),
		"learner_id", cc.NodeID,
		"raftAddr", raftAddr,
	)

	if isLeader {
		confState := c.node.ConfState()
		if err := c.triggerSnapshot(lastApplied, &confState); err != nil {
			slog.Warn("failed to trigger snapshot after adding learner",
				"learner_id", cc.NodeID,
				"error", err,
			)
		} else {
			slog.Info("triggered snapshot for new learner", "learner_id", cc.NodeID)
		}
	}
}

func (c *Coordinator) handleAddNode(cc raftpb.ConfChange, isLeader bool, lastApplied uint64) {
	if cc.NodeID == c.node.ID() {
		slog.Info("this node added as voter", "node_id", c.node.ID())
		return
	}

	raftAddr, clientAddr := ops.DecodePeerMetadata(cc.Context)

	if raftAddr == "" {
		raftAddr, _ = c.transport.GetPeerAddrs(cc.NodeID)
	}

	if raftAddr == "" {
		slog.Warn("no address for new voter", "voter_id", cc.NodeID)
		return
	}

	c.transport.AddPeer(cc.NodeID, raftAddr, clientAddr)

	if err := c.transport.InitPeerClient(cc.NodeID, raftAddr); err != nil {
		slog.Error("failed to init client for voter",
			"voter_id", cc.NodeID,
			"error", err,
		)
		return
	}

	c.transport.StartPeerSender(cc.NodeID, c.sendQueueSize)

	slog.Info("voter added to cluster",
		"node_id", c.node.ID(),
		"voter_id", cc.NodeID,
		"raftAddr", raftAddr,
	)

	if isLeader {
		go func() {
			confState := c.node.ConfState()
			if err := c.triggerSnapshot(lastApplied, &confState); err != nil {
				slog.Warn("failed to trigger snapshot after adding voter",
					"voter_id", cc.NodeID,
					"error", err,
				)
			} else {
				slog.Info("triggered snapshot for new voter", "voter_id", cc.NodeID)
			}
		}()
	}
}

func (c *Coordinator) handleRemoveNode(cc raftpb.ConfChange) {
	if cc.NodeID == c.node.ID() {
		slog.Warn("this node was removed from cluster", "node_id", c.node.ID())
		return
	}

	slog.Info("removing node from cluster", "node_id", c.node.ID(), "removed_id", cc.NodeID)

	c.transport.StopPeerSender(cc.NodeID)
	if err := c.transport.ClosePeerClient(cc.NodeID); err != nil {
		slog.Warn("failed to close removed peer client",
			"peer_id", cc.NodeID,
			"error", err,
		)
	}
	c.transport.RemovePeer(cc.NodeID)

	slog.Info("node removed from cluster",
		"node_id", c.node.ID(),
		"removed_id", cc.NodeID,
	)
}
