package coordinator

import (
	"context"
	"log/slog"
	"pulsardb/internal/metrics"
	"pulsardb/internal/raft/ops"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

func (c *Coordinator) runMainLoop() {
	ticker := time.NewTicker(c.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			slog.Debug("raft loop stopping", "node_id", c.node.ID())
			return

		case <-ticker.C:
			c.node.Tick()

		case req := <-c.stepInbox:
			err := c.node.Step(req.Ctx, req.Msg)
			select {
			case req.Resp <- err:
			default:
			}

		case rd, ok := <-c.node.Ready():
			if !ok {
				slog.Warn("raft ready channel closed", "node_id", c.node.ID())
				return
			}
			if err := c.processReady(rd); err != nil {
				slog.Error("processReady failed", "node_id", c.node.ID(), "error", err)
				return
			}
		}
	}
}

func (c *Coordinator) processReady(rd etcdraft.Ready) error {
	slog.Debug("processing ready",
		"node_id", c.node.ID(),
		"entries", len(rd.Entries),
		"committed", len(rd.CommittedEntries),
		"messages", len(rd.Messages),
		"hasSnapshot", !etcdraft.IsEmptySnap(rd.Snapshot),
	)

	ok := c.acquireInflight()
	if ok {
		metrics.CommandsInFlight.Inc()
		defer c.releaseInflight()
		defer metrics.CommandsInFlight.Dec()
	}

	start := time.Now()
	if err := c.node.Storage().SaveReady(rd); err != nil {
		return err
	}

	metrics.WALWriteDuration.Observe(time.Since(start).Seconds())
	metrics.WALWritesTotal.Add(float64(len(rd.Entries)))

	c.sendMessages(rd.Messages)

	if !etcdraft.IsEmptySnap(rd.Snapshot) {
		if err := c.applyLeaderSnapshot(rd.Snapshot); err != nil {
			return err
		}
	}

	c.handleReadStates(rd.ReadStates)

	if _, err := c.applyEntries(rd.CommittedEntries); err != nil {
		slog.Error("failed to apply entries", "node_id", c.node.ID(), "error", err)
	}

	c.node.Advance()

	if c.snapCount > 0 {
		if err := c.maybeTriggerSnapshot(c.LastApplied()); err != nil {
			slog.Warn("failed to trigger snapshot", "node_id", c.node.ID(), "error", err)
		}
	}

	return nil
}

func (c *Coordinator) sendMessages(msgs []raftpb.Message) {
	if len(msgs) == 0 {
		return
	}

	slog.Debug("sending raft messages", "count", len(msgs))
	for _, msg := range msgs {
		metrics.RaftMessagesTotal.WithLabelValues("sent", msg.Type.String()).Inc()
	}

	c.transport.SendMessages(msgs)
}

func (c *Coordinator) runMetricsCollector() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCtx.Done():
			return
		case <-ticker.C:
			c.UpdateMetrics()
		}
	}
}

func (c *Coordinator) runPromotionChecker() {
	ticker := time.NewTicker(c.promotionCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCtx.Done():
			return
		case <-ticker.C:
			c.maybePromoteLearners()
		}
	}
}

func (c *Coordinator) maybePromoteLearners() {
	if !c.IsLeader() {
		return
	}

	status := c.node.Status()
	confState := c.node.ConfState()

	if len(confState.Learners) == 0 {
		return
	}

	for _, learnerID := range confState.Learners {
		if learnerID == c.node.ID() {
			continue
		}

		progress, ok := status.Progress[learnerID]
		if !ok {
			slog.Debug("no progress info for learner", "learner_id", learnerID)
			continue
		}

		if c.isLearnerReady(progress, status.Commit) {
			slog.Info("promoting learner to voter",
				"learner_id", learnerID,
				"match", progress.Match,
				"commit", status.Commit,
				"lag", status.Commit-progress.Match,
			)

			raftAddr, clientAddr := c.transport.GetPeerAddrs(learnerID)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := c.PromoteToVoter(ctx, learnerID, raftAddr, clientAddr)
			cancel()

			if err != nil {
				slog.Warn("failed to promote learner",
					"learner_id", learnerID,
					"error", err,
				)
			}
		} else {
			slog.Debug("learner not ready for promotion",
				"learner_id", learnerID,
				"match", progress.Match,
				"commit", status.Commit,
				"state", progress.State.String(),
			)
		}
	}
}

func (c *Coordinator) isLearnerReady(progress tracker.Progress, commitIndex uint64) bool {
	if progress.State != tracker.StateReplicate {
		return false
	}
	if progress.IsPaused() {
		return false
	}
	if commitIndex <= c.promotionThreshold {
		return progress.Match >= commitIndex
	}
	return progress.Match >= (commitIndex - c.promotionThreshold)
}

func (c *Coordinator) PromoteToVoter(ctx context.Context, nodeID uint64, raftAddr, clientAddr string) error {
	if !c.IsLeader() {
		return ErrNotLeader
	}

	cc := ops.BuildPromoteChange(nodeID, raftAddr, clientAddr)
	return c.node.ProposeConfChange(ctx, cc)
}

func (c *Coordinator) requestJoin() {
	time.Sleep(2 * time.Second)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			confState := c.node.ConfState()
			if ops.IsInCluster(confState, c.node.ID()) {
				slog.Info("successfully joined cluster", "node_id", c.node.ID())
				return
			}

			if err := c.tryJoin(); err != nil {
				slog.Debug("join attempt failed", "error", err)
			}
		}
	}
}

func (c *Coordinator) tryJoin() error {
	peers := c.transport.Peers()

	for peerID := range peers {
		if peerID == c.node.ID() {
			continue
		}

		client, ok := c.transport.GetLeaderClient(peerID)
		if !ok {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		accepted, message, err := client.RequestJoinCluster(ctx, c.node.ID(), c.localRaftAddr, c.localClientAddr)
		cancel()

		if err != nil {
			slog.Debug("join request failed", "peer_id", peerID, "error", err)
			continue
		}

		if accepted {
			slog.Info("join request accepted", "by_peer", peerID, "message", message)
			return nil
		}
	}

	return ErrNoLeader
}
