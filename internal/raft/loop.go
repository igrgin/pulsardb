package raft

import (
	"log/slog"
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
}

func (s *Service) runLoop() {
	ticker := time.NewTicker(time.Duration(s.tickInterval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
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
				return
			}
			if err := s.processReady(rd); err != nil {
				slog.Error("processReady failed", "error", err)
				return
			}
		}
	}
}

func (s *Service) processReady(rd etcdraft.Ready) error {
	if err := s.Node.storage.SaveReady(rd); err != nil {
		return err
	}

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
	)

	if len(snap.Data) > 0 {
		if err := s.storeService.RestoreFromSnapshot(snap.Data); err != nil {
			return err
		}
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
		if err := s.TriggerSnapshot(s.snapCount); err != nil {
			slog.Warn("failed to trigger snapshot", "error", err)
		}
	}
}

func (s *Service) sendMessages(msgs []raftpb.Message) {
	s.Node.sendMessages(msgs)
}
