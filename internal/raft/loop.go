package raft

import (
	"log/slog"
	"pulsardb/internal/metrics"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func (s *Service) startLoop() {
	s.stoppedWg.Add(1)
	go func() {
		defer s.stoppedWg.Done()
		s.runLoop()
	}()

	s.stoppedWg.Add(1)
	go func() {
		defer s.stoppedWg.Done()
		s.collectMetrics()
	}()

	s.stoppedWg.Add(1)
	go func() {
		defer s.stoppedWg.Done()
		s.runPromotionChecker()
	}()

	slog.Info("raft loop started", "node_id", s.Node.Id)
}

func (s *Service) runPromotionChecker() {
	ticker := time.NewTicker(s.promotionCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.maybePromoteLearners()
		}
	}
}

func (s *Service) collectMetrics() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.updateMetrics()
		}
	}
}

func (s *Service) updateMetrics() {
	status := s.Node.Status()

	if status.RaftState == etcdraft.StateLeader {
		metrics.RaftIsLeader.Set(1)
	} else {
		metrics.RaftIsLeader.Set(0)
	}

	metrics.RaftTerm.Set(float64(status.Term))
	metrics.RaftCommitIndex.Set(float64(status.Commit))
	metrics.RaftAppliedIndex.Set(float64(s.LastApplied()))
	metrics.RaftSnapshotIndex.Set(float64(s.Node.Storage().SnapshotIndex()))

	confState := s.Node.ConfState()
	metrics.RaftPeersTotal.Set(float64(len(confState.Voters) + len(confState.Learners)))

	metrics.StorageKeysTotal.Set(float64(s.StoreService.Len()))
}

func (s *Service) runLoop() {
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			slog.Debug("raft loop stopping", "node_id", s.Node.Id)
			return

		case <-ticker.C:
			s.Node.raftNode.Tick()

		case req := <-s.stepInbox:
			err := s.Node.raftNode.Step(req.ctx, req.msg)
			select {
			case req.resp <- err:
			default:
			}

		case rd, ok := <-s.Node.raftNode.Ready():
			if !ok {
				slog.Warn("raft ready channel closed", "node_id", s.Node.Id)
				return
			}
			if err := s.processReady(rd); err != nil {
				slog.Error("processReady failed", "node_id", s.Node.Id, "error", err)
				return
			}
		}
	}
}

func (s *Service) processReady(rd etcdraft.Ready) error {
	slog.Debug("processing ready",
		"node_id", s.Node.Id,
		"entries", len(rd.Entries),
		"committed", len(rd.CommittedEntries),
		"messages", len(rd.Messages),
		"hasSnapshot", !etcdraft.IsEmptySnap(rd.Snapshot),
	)

	start := time.Now()
	if err := s.Node.storage.SaveReady(rd); err != nil {
		return err
	}
	metrics.WALWriteDuration.Observe(time.Since(start).Seconds())
	metrics.WALWritesTotal.Add(float64(len(rd.Entries)))

	s.sendMessages(rd.Messages)

	if !etcdraft.IsEmptySnap(rd.Snapshot) {
		if err := s.applyLeaderSnapshotToApp(rd.Snapshot); err != nil {
			return err
		}
	}

	s.HandleReadStates(rd.ReadStates)

	s.applyCommitted(rd.CommittedEntries)

	s.Node.raftNode.Advance()

	if s.snapCount > 0 {
		s.maybeSnapshot()
	}

	return nil
}

func (s *Service) applyLeaderSnapshotToApp(snap raftpb.Snapshot) error {
	slog.Info("applying snapshot to application",
		"node_id", s.Node.Id,
		"index", snap.Metadata.Index,
		"term", snap.Metadata.Term,
		"data_size", len(snap.Data),
	)

	if len(snap.Data) > 0 {
		if err := s.StoreService.Restore(snap.Data); err != nil {
			slog.Error("failed to restore from snapshot", "node_id", s.Node.Id, "error", err)
			return err
		}
		slog.Info("application state restored from snapshot", "node_id", s.Node.Id)
	} else {
		slog.Warn("snapshot has no data, skipping application restore",
			"node_id", s.Node.Id,
			"index", snap.Metadata.Index,
		)
	}

	s.Node.SetConfState(snap.Metadata.ConfState)
	s.SetLastApplied(snap.Metadata.Index)

	return nil
}

func (s *Service) maybeSnapshot() {
	lastApplied := s.LastApplied()
	snapIndex := s.Node.Storage().SnapshotIndex()

	if lastApplied <= snapIndex {
		return
	}

	if (lastApplied - snapIndex) >= s.snapCount {
		slog.Debug("snapshot threshold reached",
			"lastApplied", lastApplied,
			"snapIndex", snapIndex,
			"snapCount", s.snapCount,
		)
		if err := s.TriggerSnapshot(s.snapCount); err != nil {
			slog.Warn("failed to trigger snapshot", "error", err)
		}
	}
}

func (s *Service) sendMessages(msgs []raftpb.Message) {
	if len(msgs) > 0 {
		slog.Debug("sending raft messages", "count", len(msgs))
		for _, msg := range msgs {
			metrics.RaftMessagesTotal.WithLabelValues("sent", msg.Type.String()).Inc()
		}
	}
	s.Node.sendMessages(msgs)
}
