package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"pulsardb/internal/configuration"
	"pulsardb/internal/domain"
	"pulsardb/internal/metrics"
	"pulsardb/internal/storage"
	"pulsardb/internal/transport/gen/command"
	"pulsardb/internal/transport/gen/raft"
	"pulsardb/internal/types"
	"sync"
	"sync/atomic"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

var ErrShuttingDown = errors.New("shutdown in progress")

type StateMachine interface {
	Apply(data []byte) (response []byte, err error)
}

var ErrNotLeader = errors.New("not leader")

type readWaiter struct {
	index uint64
	ch    chan uint64
}

type raftStepReq struct {
	ctx  context.Context
	msg  raftpb.Message
	resp chan error
}

type readIndexReq struct {
	ctx  context.Context
	resp chan readIndexResp
}

type readIndexResp struct {
	index uint64
	err   error
}

type Service struct {
	Node                   *Node
	StoreService           *storage.Service
	StateMachine           domain.StateMachine
	nextReqID              uint64
	readWaiters            map[string]*readWaiter
	readMu                 sync.Mutex
	lastApplied            uint64
	appliedMu              sync.RWMutex
	stopCh                 chan int
	stoppedWg              sync.WaitGroup
	tickInterval           time.Duration
	snapCount              uint64
	stepInbox              chan raftStepReq
	readIndexInbox         chan readIndexReq
	sendPoolQueueSize      int
	promotionThreshold     uint64
	promotionCheckInterval time.Duration
	localRaftAddr          string
	shuttingDown           atomic.Bool
	drainTimeout           time.Duration
	inFlight               sync.WaitGroup
}

func NewService(node *Node,
	storageService *storage.Service,
	sm domain.StateMachine,
	cfg *configuration.RaftConfigurationProperties,
	localRaftAddr string) *Service {
	stepSize := 1024
	readIndexSize := 1024

	s := &Service{
		Node:                   node,
		StoreService:           storageService,
		StateMachine:           sm,
		readWaiters:            make(map[string]*readWaiter),
		lastApplied:            0,
		stopCh:                 make(chan int),
		tickInterval:           cfg.TickInterval,
		snapCount:              cfg.SnapCount,
		stepInbox:              make(chan raftStepReq, stepSize),
		readIndexInbox:         make(chan readIndexReq, readIndexSize),
		sendPoolQueueSize:      cfg.SendQueueSize,
		promotionCheckInterval: cfg.PromotionCheckInterval,
		promotionThreshold:     cfg.PromotionThreshold,
		localRaftAddr:          localRaftAddr,
		drainTimeout:           cfg.ServiceDrainTimeout,
	}

	slog.Info("raft service created",
		"node_id", node.Id,
		"tickInterval", cfg.TickInterval,
		"snapCount", cfg.SnapCount,
	)

	return s
}

func (s *Service) maybePromoteLearners() {
	if !s.IsLeader() {
		return
	}

	status := s.Node.Status()
	confState := s.Node.ConfState()

	if len(confState.Learners) == 0 {
		return
	}

	for _, learnerID := range confState.Learners {
		if learnerID == s.Node.Id {
			continue
		}

		progress, ok := status.Progress[learnerID]
		if !ok {
			slog.Debug("no progress info for learner", "learner_id", learnerID)
			continue
		}

		if s.isLearnerReady(progress, status.Commit) {
			slog.Info("promoting learner to voter",
				"learner_id", learnerID,
				"match", progress.Match,
				"commit", status.Commit,
				"lag", status.Commit-progress.Match,
			)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := s.promoteToVoter(ctx, learnerID)
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

func (s *Service) isLearnerReady(progress tracker.Progress, commitIndex uint64) bool {

	if progress.State != tracker.StateReplicate {
		return false
	}

	if progress.IsPaused() {
		return false
	}

	if commitIndex <= s.promotionThreshold {
		return progress.Match >= commitIndex
	}

	return progress.Match >= (commitIndex - s.promotionThreshold)
}

func (s *Service) promoteToVoter(ctx context.Context, nodeID uint64) error {
	s.Node.mu.RLock()
	raftAddr := s.Node.raftPeers[nodeID]
	s.Node.mu.RUnlock()

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeID,
		Context: encodePeerAddrs(raftAddr),
	}

	return s.Node.raftNode.ProposeConfChange(ctx, cc)
}

const peerAddrSeparator = "|"

func encodePeerAddrs(raftAddr string) []byte {
	return []byte(raftAddr)
}

func decodePeerAddrs(data []byte) (raftAddr string) {
	return string(data)
}

func (s *Service) RequestJoin(ctx context.Context, leaderID uint64, nodeID uint64, raftAddr string) error {
	client, ok := s.Node.GetLeaderClient(leaderID)
	if !ok {
		return fmt.Errorf("no client for leader %d", leaderID)
	}

	resp, err := client.RequestJoinCluster(ctx, &rafttransportpb.JoinRequest{
		NodeId:   nodeID,
		RaftAddr: raftAddr,
	})
	if err != nil {
		return fmt.Errorf("join request failed: %w", err)
	}
	if !resp.Accepted {
		return fmt.Errorf("join rejected: %s", resp.Message)
	}
	return nil
}

func (s *Service) HandleJoinRequest(ctx context.Context, nodeID uint64, raftAddr string) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	confState := s.Node.ConfState()
	for _, v := range confState.Voters {
		if v == nodeID {
			slog.Info("node already a voter", "node_id", nodeID)
			return nil
		}
	}

	for _, l := range confState.Learners {
		if l == nodeID {
			slog.Info("node already a learner, will be promoted when ready", "node_id", nodeID)
			return nil
		}
	}

	s.Node.AddPeer(nodeID, raftAddr)

	slog.Info("adding node as learner", "node_id", nodeID, "raft_addr", raftAddr)
	return s.ProposeAddLearnerNode(ctx, nodeID, raftAddr)
}

func (s *Service) CallRaftStep(ctx context.Context, m raftpb.Message) error {
	req := raftStepReq{ctx: ctx, msg: m, resp: make(chan error, 1)}

	select {
	case s.stepInbox <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopCh:
		return context.Canceled
	}

	select {
	case err := <-req.resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopCh:
		return context.Canceled
	}
}

func (s *Service) doReadIndex(ctx context.Context) (uint64, error) {
	start := time.Now()

	if s.Node.Status().Lead == 0 {
		slog.Debug("read index failed: no leader", "node_id", s.Node.Id)
		metrics.ReadIndexTotal.WithLabelValues("no_leader").Inc()
		return 0, ErrNotLeader
	}

	reqID := s.NextRequestID()
	reqCtx := []byte(fmt.Sprintf("read-%d", reqID))
	reqCtxKey := string(reqCtx)

	ch := make(chan uint64, 1)
	s.RegisterReadWaiter(reqCtxKey, ch)
	defer s.UnregisterReadWaiter(reqCtxKey)

	if err := s.Node.ReadIndex(ctx, reqCtx); err != nil {
		slog.Debug("read index request failed", "node_id", s.Node.Id, "error", err)
		metrics.ReadIndexTotal.WithLabelValues("error").Inc()
		return 0, fmt.Errorf("ReadIndex: %w", err)
	}

	select {
	case idx := <-ch:
		slog.Debug("read index received", "node_id", s.Node.Id, "index", idx)
		metrics.ReadIndexTotal.WithLabelValues("success").Inc()
		metrics.ReadIndexDuration.Observe(time.Since(start).Seconds())
		return idx, nil
	case <-ctx.Done():
		slog.Debug("read index timeout", "node_id", s.Node.Id)
		metrics.ReadIndexTotal.WithLabelValues("timeout").Inc()
		return 0, ctx.Err()
	case <-s.stopCh:
		metrics.ReadIndexTotal.WithLabelValues("cancelled").Inc()
		return 0, context.Canceled
	}
}

func (s *Service) acquireInflight() bool {
	if s.shuttingDown.Load() {
		return false
	}
	s.inFlight.Add(1)

	if s.shuttingDown.Load() {
		s.inFlight.Done()
		return false
	}
	return true
}

func (s *Service) releaseInflight() {
	s.inFlight.Done()
}

func (s *Service) Stop() {
	slog.Info("initiating graceful shutdown", "node_id", s.Node.Id)

	s.shuttingDown.Store(true)

	if s.IsLeader() {
		s.tryTransferLeadership()
	}

	s.waitForInflight()

	s.waitForPendingApplies()

	s.cancelAllReadWaiters()

	close(s.stopCh)
	s.stoppedWg.Wait()

	s.Node.Stop()

	slog.Info("raft service stopped", "node_id", s.Node.Id)
}

func (s *Service) waitForPendingApplies() {
	status := s.Node.Status()
	target := status.Commit

	if s.LastApplied() >= target {
		return
	}

	slog.Debug("waiting for pending applies",
		"node_id", s.Node.Id,
		"current", s.LastApplied(),
		"target", target,
	)

	deadline := time.Now().Add(s.drainTimeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		if s.LastApplied() >= target {
			slog.Debug("pending applies completed", "applied", s.LastApplied())
			return
		}
		<-ticker.C
	}

	slog.Warn("timed out waiting for pending applies",
		"applied", s.LastApplied(),
		"target", target,
	)
}

func (s *Service) waitForInflight() {
	done := make(chan struct{})
	go func() {
		s.inFlight.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Debug("all in-flight operations completed", "node_id", s.Node.Id)
	case <-time.After(s.drainTimeout):
		slog.Warn("timed out waiting for in-flight operations", "node_id", s.Node.Id)
	}
}

func (s *Service) tryTransferLeadership() {
	status := s.Node.Status()
	if status.RaftState != etcdraft.StateLeader {
		return
	}

	var targetID uint64
	var maxMatch uint64

	for id, pr := range status.Progress {
		if id == s.Node.Id {
			continue
		}
		if pr.State == tracker.StateReplicate && pr.Match > maxMatch {
			maxMatch = pr.Match
			targetID = id
		}
	}

	if targetID == 0 {
		slog.Warn("no suitable target for leadership transfer", "node_id", s.Node.Id)
		return
	}

	slog.Info("transferring leadership",
		"node_id", s.Node.Id,
		"target", targetID,
	)

	s.Node.raftNode.TransferLeadership(context.Background(), s.Node.Id, targetID)

	deadline := time.Now().Add(2 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		<-ticker.C
		if s.Node.Status().RaftState != etcdraft.StateLeader {
			slog.Info("leadership transferred", "new_leader", s.Node.Status().Lead)
			return
		}
	}

	slog.Warn("leadership transfer timed out", "node_id", s.Node.Id)
}

func (s *Service) cancelAllReadWaiters() {
	s.readMu.Lock()
	defer s.readMu.Unlock()

	for key, w := range s.readWaiters {
		if w != nil && w.ch != nil {
			close(w.ch)
		}
		delete(s.readWaiters, key)
	}
}

func (s *Service) drainMessageQueues() {
	deadline := time.Now().Add(s.drainTimeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		allEmpty := true
		s.Node.peerQueueMu.RLock()
		for _, ch := range s.Node.peerQueues {
			if len(ch) > 0 {
				allEmpty = false
				break
			}
		}
		s.Node.peerQueueMu.RUnlock()

		if allEmpty {
			return
		}
		<-ticker.C
	}
}

func (s *Service) LastApplied() uint64 {
	s.appliedMu.RLock()
	defer s.appliedMu.RUnlock()
	return s.lastApplied
}

func (s *Service) SetLastApplied(index uint64) {
	var newLA uint64

	s.appliedMu.Lock()
	if index > s.lastApplied {
		s.lastApplied = index
		slog.Debug("last applied updated", "node_id", s.Node.Id, "index", index)
	}
	newLA = s.lastApplied
	s.appliedMu.Unlock()

	s.completeReadWaiters(newLA)
}

func (s *Service) completeReadWaiters(lastApplied uint64) {
	s.readMu.Lock()
	defer s.readMu.Unlock()

	completed := 0
	for ctxKey, w := range s.readWaiters {
		if w == nil || w.index == 0 {
			continue
		}
		if lastApplied >= w.index {
			select {
			case w.ch <- w.index:
			default:
			}
			delete(s.readWaiters, ctxKey)
			completed++
		}
	}
	if completed > 0 {
		slog.Debug("completed read waiters", "count", completed, "lastApplied", lastApplied)
	}
}

func (s *Service) NextRequestID() uint64 {
	return atomic.AddUint64(&s.nextReqID, 1)
}

func (s *Service) getReadIndexFromLeader(ctx *context.Context, leaderID uint64) (uint64, error) {
	if leaderID == 0 {
		return 0, fmt.Errorf("no known leader")
	}

	client, ok := s.Node.GetLeaderClient(leaderID)
	if !ok {
		return 0, fmt.Errorf("no client for leader %d", leaderID)
	}

	slog.Debug("getting read index from leader", "node_id", s.Node.Id, "leader_id", leaderID)
	resp, err := client.GetReadIndex(*ctx, &rafttransportpb.GetReadIndexRequest{FromNode: s.Node.Id})
	if err != nil {
		slog.Debug("get read index from leader failed", "leader_id", leaderID, "error", err)
		return 0, fmt.Errorf("forward to leader %d: %w", leaderID, err)
	}

	return resp.ReadIndex, nil
}

func (s *Service) waitUntilApplied(ctx *context.Context, index uint64) error {
	if s.LastApplied() >= index {
		return nil
	}

	slog.Debug("waiting until applied", "node_id", s.Node.Id, "target_index", index, "current", s.LastApplied())

	key := fmt.Sprintf("applied-%d-%d", index, s.NextRequestID())
	ch := make(chan uint64, 1)

	s.readMu.Lock()
	s.readWaiters[key] = &readWaiter{index: index, ch: ch}
	s.readMu.Unlock()
	defer s.UnregisterReadWaiter(key)

	select {
	case <-ch:
		slog.Debug("applied index reached", "node_id", s.Node.Id, "index", index)
		return nil
	case <-(*ctx).Done():
		slog.Debug("wait for applied timed out", "node_id", s.Node.Id, "index", index)
		return (*ctx).Err()
	}
}

func (s *Service) HandleReadStates(readStates []etcdraft.ReadState) {
	if len(readStates) == 0 {
		return
	}

	slog.Debug("handling read states", "count", len(readStates))

	s.readMu.Lock()
	defer s.readMu.Unlock()

	lastApplied := s.LastApplied()

	for _, rs := range readStates {
		ctxKey := string(rs.RequestCtx)
		waiter, ok := s.readWaiters[ctxKey]
		if !ok {
			continue
		}

		if rs.Index > waiter.index {
			waiter.index = rs.Index
		}

		if lastApplied >= waiter.index {
			select {
			case waiter.ch <- waiter.index:
			default:
			}
			delete(s.readWaiters, ctxKey)
		}
	}
}

func (s *Service) RegisterReadWaiter(key string, ch chan uint64) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if s.readWaiters == nil {
		s.readWaiters = make(map[string]*readWaiter)
	}
	s.readWaiters[key] = &readWaiter{index: 0, ch: ch}
}

func (s *Service) UnregisterReadWaiter(key string) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	delete(s.readWaiters, key)
}

func (s *Service) applyCommitted(entries []raftpb.Entry) {
	if len(entries) > 0 {
		slog.Debug("applying committed entries", "count", len(entries))
	}

	for _, entry := range entries {
		switch entry.Type {
		case raftpb.EntryConfChange:
			s.applyConfChange(entry)

		case raftpb.EntryNormal:
			s.applyNormalEntry(entry)

		default:
			slog.Warn("ignoring unsupported raft entry type",
				"node_id", s.Node.Id,
				"index", entry.Index,
				"term", entry.Term,
				"type", entry.Type,
			)
		}

		s.SetLastApplied(entry.Index)
	}
}

func (s *Service) applyNormalEntry(entry raftpb.Entry) {
	slog.Debug("applying entry", "index", entry.Index, "term", entry.Term, "data_len", len(entry.Data))
	if len(entry.Data) == 0 {
		return
	}
	start := time.Now()
	err := s.StateMachine.Apply(entry.Data)
	metrics.CommandDuration.WithLabelValues("apply").Observe(time.Since(start).Seconds())

	if err != nil {
		metrics.CommandsTotal.WithLabelValues("apply", "error").Inc()
		slog.Error("state machine apply failed",
			"node_id", s.Node.Id,
			"index", entry.Index,
			"error", err,
		)
	} else {
		metrics.CommandsTotal.WithLabelValues("apply", "success").Inc()
	}
}

func (s *Service) TriggerSnapshot(snapCount uint64) error {
	start := time.Now()

	lastApplied := s.LastApplied()
	if lastApplied == 0 {
		slog.Debug("skip snapshot: no applied entries")
		return nil
	}

	slog.Debug("triggering snapshot", "lastApplied", lastApplied)

	data, err := s.StoreService.Snapshot()
	metrics.StorageSnapshotSize.Set(float64(len(data)))

	if err != nil {
		return fmt.Errorf("get snapshot data: %w", err)
	}

	if len(data) == 0 {
		slog.Debug("no application data to snapshot",
			"last_applied", lastApplied,
			"storage_len", s.StoreService.Len(),
		)
		return nil
	}

	metrics.StorageSnapshotSize.Set(float64(len(data)))

	confState := s.Node.ConfState()

	snap, err := s.Node.Storage().CreateSnapshot(lastApplied, &confState, data)
	if err != nil {
		if errors.Is(err, etcdraft.ErrSnapOutOfDate) {
			slog.Debug("snapshot already exists", "index", lastApplied)
			return nil
		}
		return fmt.Errorf("create snapshot: %w", err)
	}

	if err := s.Node.Storage().SaveSnapshot(snap); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}

	compactIndex := uint64(1)
	if lastApplied > snapCount {
		compactIndex = lastApplied - snapCount
	}

	if err := s.Node.Storage().Compact(compactIndex); err != nil {
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

func (s *Service) ReconcileConfiguredPeers() {
	time.Sleep(5 * time.Second)

	if !s.IsLeader() {
		slog.Debug("skipping peer reconciliation: not leader", "node_id", s.Node.Id)
		return
	}

	slog.Debug("reconciling configured peers", "node_id", s.Node.Id)

	confState := s.Node.ConfState()
	raftPeers, clientPeers := s.Node.AllPeers()

	configuredSet := make(map[uint64]bool)
	configuredSet[s.Node.Id] = true
	for nodeID := range raftPeers {
		configuredSet[nodeID] = true
	}

	voterSet := make(map[uint64]bool)
	for _, v := range confState.Voters {
		voterSet[v] = true
	}

	for nodeID, raftAddr := range raftPeers {
		if nodeID == s.Node.Id {
			continue
		}
		if !voterSet[nodeID] {
			clientAddr := clientPeers[nodeID]
			slog.Info("proposing new peer from config",
				"node_id", nodeID,
				"raftAddr", raftAddr,
				"clientAddr", clientAddr,
			)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := s.ProposeAddLearnerNode(ctx, nodeID, raftAddr); err != nil {
				slog.Warn("failed to propose new peer", "node_id", nodeID, "error", err)
			}
			cancel()
			time.Sleep(time.Second)
		}
	}

	for _, voterID := range confState.Voters {
		if voterID == s.Node.Id {
			continue
		}
		if !configuredSet[voterID] {
			slog.Info("proposing removal of peer not in config", "node_id", voterID)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := s.ProposeRemoveNode(ctx, voterID); err != nil {
				slog.Warn("failed to propose peer removal", "node_id", voterID, "error", err)
			}
			cancel()
			time.Sleep(time.Second)
		}
	}

	for _, learnerID := range confState.Learners {
		if learnerID == s.Node.Id {
			continue
		}
		if !configuredSet[learnerID] {
			slog.Info("proposing removal of learner not in config", "node_id", learnerID)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := s.ProposeRemoveNode(ctx, learnerID); err != nil {
				slog.Warn("failed to propose learner removal", "node_id", learnerID, "error", err)
			}
			cancel()
			time.Sleep(time.Second)
		}
	}

	slog.Debug("peer reconciliation complete", "node_id", s.Node.Id)
}

func (s *Service) RecoverState() error {
	slog.Info("recovering state", "node_id", s.Node.Id)

	snapIndex := s.Node.Storage().SnapshotIndex()
	snapData := s.Node.Storage().SnapshotData()

	if len(snapData) > 0 {
		if err := s.StoreService.Restore(snapData); err != nil {
			return fmt.Errorf("restore state machine from snapshot: %w", err)
		}
		slog.Info("restored application state from snapshot", "index", snapIndex)
	}

	entries, err := s.Node.Storage().EntriesAfter(snapIndex)
	if err != nil {
		return fmt.Errorf("get entries after snapshot: %w", err)
	}

	if len(entries) == 0 {
		if snapIndex > s.LastApplied() {
			s.SetLastApplied(snapIndex)
		}
		slog.Debug("no entries to replay", "snapIndex", snapIndex)
	} else {
		slog.Debug("replaying entries", "count", len(entries), "afterIndex", snapIndex)
		for _, entry := range entries {
			if err := s.replayEntry(entry); err != nil {
				slog.Error("failed to replay entry",
					"node_id", s.Node.Id,
					"index", entry.Index,
					"error", err,
				)
			}
		}

		lastIndex := entries[len(entries)-1].Index
		s.SetLastApplied(lastIndex)

		slog.Info("replayed entries to application",
			"count", len(entries),
			"last_index", lastIndex,
		)
	}

	return nil
}

func (s *Service) replayEntry(entry raftpb.Entry) error {
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
				if err := s.applyToStorage(req); err != nil {
					return err
				}
			}
			return nil
		}

		var req commandeventspb.CommandEventRequest
		if err := proto.Unmarshal(entry.Data, &req); err != nil {
			return err
		}
		return s.applyToStorage(&req)

	default:
		return nil
	}
}

func (s *Service) applyToStorage(req *commandeventspb.CommandEventRequest) error {
	switch req.GetType() {
	case commandeventspb.CommandEventType_SET:
		val := types.ValueFromProto(req.GetValue())
		s.StoreService.Set(req.GetKey(), val)
		slog.Debug("replayed SET", "key", req.GetKey())
	case commandeventspb.CommandEventType_DELETE:
		s.StoreService.Delete(req.GetKey())
		slog.Debug("replayed DELETE", "key", req.GetKey())
	}
	return nil
}

func (s *Service) applyConfChange(entry raftpb.Entry) {
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		slog.Error("failed to unmarshal conf change",
			"node_id", s.Node.Id,
			"index", entry.Index,
			"error", err,
		)
		return
	}

	slog.Debug("applying conf change",
		"node_id", s.Node.Id,
		"type", cc.Type,
		"target_node", cc.NodeID,
		"index", entry.Index,
	)

	confState := s.Node.ApplyConfChange(cc)
	if confState != nil {
		s.Node.SetConfState(*confState)
		if err := s.Node.storage.SaveConfState(*confState); err != nil {
			slog.Error("failed to persist confState",
				"node_id", s.Node.Id,
				"error", err,
			)
		}
	}

	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		s.handleAddNode(cc)
	case raftpb.ConfChangeRemoveNode:
		s.handleRemoveNode(cc)
	case raftpb.ConfChangeAddLearnerNode:
		s.handleAddLearner(cc)
	default:
		slog.Debug("unhandled conf change type",
			"node_id", s.Node.Id,
			"type", cc.Type,
		)
	}
}

func (s *Service) handleAddLearner(cc raftpb.ConfChange) {
	if cc.NodeID == s.Node.Id {
		slog.Info("this node added as learner", "node_id", s.Node.Id)
		return
	}

	raftAddr := decodePeerAddrs(cc.Context)

	s.Node.mu.Lock()
	s.Node.raftPeers[cc.NodeID] = raftAddr
	s.Node.mu.Unlock()

	if err := s.Node.initPeerClient(cc.NodeID, raftAddr); err != nil {
		slog.Error("failed to init client for learner",
			"learner_id", cc.NodeID,
			"error", err,
		)
	}

	s.Node.startPeerSender(cc.NodeID, s.sendPoolQueueSize)

	slog.Info("learner added to cluster",
		"node_id", s.Node.Id,
		"learner_id", cc.NodeID,
		"raftAddr", raftAddr,
	)

	if s.IsLeader() {
		if err := s.TriggerSnapshot(s.snapCount); err != nil {
			if !errors.Is(err, etcdraft.ErrSnapOutOfDate) {
				slog.Warn("failed to trigger snapshot after adding learner",
					"learner_id", cc.NodeID,
					"error", err,
				)
			}
		} else {
			slog.Info("triggered snapshot for new learner", "learner_id", cc.NodeID)
		}
	}
}

func (s *Service) handleAddNode(cc raftpb.ConfChange) {
	if cc.NodeID == s.Node.Id {
		slog.Info("this node added as voter", "node_id", s.Node.Id)
		return
	}

	raftAddr := decodePeerAddrs(cc.Context)

	if raftAddr == "" {
		s.Node.mu.RLock()
		raftAddr = s.Node.raftPeers[cc.NodeID]
		s.Node.mu.RUnlock()
	}

	if raftAddr == "" {
		slog.Warn("no address for new voter", "voter_id", cc.NodeID)
		return
	}

	s.Node.mu.Lock()
	s.Node.raftPeers[cc.NodeID] = raftAddr
	s.Node.mu.Unlock()

	s.Node.mu.RLock()
	_, hasClient := s.Node.clients[cc.NodeID]
	s.Node.mu.RUnlock()

	if !hasClient {
		if err := s.Node.initPeerClient(cc.NodeID, raftAddr); err != nil {
			slog.Error("failed to init client for voter",
				"voter_id", cc.NodeID,
				"error", err,
			)
			return
		}

		s.Node.peerQueueMu.Lock()
		if s.Node.peerQueues != nil {
			s.Node.peerQueueMu.Unlock()
			s.Node.startPeerSender(cc.NodeID, s.sendPoolQueueSize)
		} else {
			s.Node.peerQueueMu.Unlock()
		}
	}

	slog.Info("voter added to cluster",
		"node_id", s.Node.Id,
		"voter_id", cc.NodeID,
		"raftAddr", raftAddr,
	)

	if s.IsLeader() {
		go func() {
			if err := s.TriggerSnapshot(s.snapCount); err != nil {
				if !errors.Is(err, etcdraft.ErrSnapOutOfDate) {
					slog.Warn("failed to trigger snapshot after adding voter",
						"voter_id", cc.NodeID,
						"error", err,
					)
				}
			} else {
				slog.Info("triggered snapshot for new voter", "voter_id", cc.NodeID)
			}
		}()
	}
}

func (s *Service) handleRemoveNode(cc raftpb.ConfChange) {
	if cc.NodeID == s.Node.Id {
		slog.Warn("this node was removed from cluster", "node_id", s.Node.Id)
		return
	}

	slog.Info("removing node from cluster", "node_id", s.Node.Id, "removed_id", cc.NodeID)

	s.Node.stopPeerSender(cc.NodeID)

	s.Node.mu.Lock()
	client, clientExists := s.Node.clients[cc.NodeID]
	delete(s.Node.raftPeers, cc.NodeID)
	delete(s.Node.clients, cc.NodeID)
	s.Node.mu.Unlock()

	if clientExists {
		if err := client.Close(); err != nil {
			slog.Warn("failed to close removed peer client",
				"peer_id", cc.NodeID,
				"error", err,
			)
		}
	}

	slog.Info("node removed from cluster",
		"node_id", s.Node.Id,
		"removed_id", cc.NodeID,
	)
}

func (s *Service) WaitForQuorum(ctx context.Context) (leaderID uint64, readIndex uint64, err error) {
	slog.Debug("waiting for quorum", "node_id", s.Node.Id)

	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	for {
		st := s.Node.Status()

		if st.Lead == 0 {
			slog.Debug("no leader yet", "node_id", s.Node.Id)
		} else if st.RaftState == etcdraft.StateLeader {
			idx, err := s.GetReadIndex(ctx)
			if err == nil {
				slog.Info("quorum established as leader",
					"node_id", s.Node.Id,
					"read_index", idx,
				)
				return st.Lead, idx, nil
			}
			slog.Debug("leader read index failed",
				"node_id", s.Node.Id,
				"error", err,
			)
		} else {
			slog.Debug("checking quorum as follower",
				"node_id", s.Node.Id,
				"leader_id", st.Lead,
			)

			idx, err := s.getReadIndexFromLeader(&ctx, st.Lead)
			if err != nil {
				slog.Debug("get read index from leader failed",
					"node_id", s.Node.Id,
					"leader_id", st.Lead,
					"error", err,
				)
			} else {
				if err := s.waitUntilApplied(&ctx, idx); err == nil {
					slog.Info("quorum established as follower",
						"node_id", s.Node.Id,
						"leader_id", st.Lead,
						"read_index", idx,
					)
					return st.Lead, idx, nil
				}

				slog.Debug("wait until applied failed",
					"node_id", s.Node.Id,
					"index", idx,
					"error", err,
				)

			}
		}

		select {
		case <-ctx.Done():
			slog.Warn("wait for quorum timed out",
				"node_id", s.Node.Id,
				"last_leader", st.Lead,
				"raft_state", st.RaftState.String(),
			)
			return 0, 0, ctx.Err()
		case <-s.stopCh:
			return 0, 0, context.Canceled
		case <-t.C:
		}
	}
}

func (s *Service) GetReadIndex(ctx context.Context) (uint64, error) {
	return s.doReadIndex(ctx)
}

func (s *Service) GetReadIndexFromLeader(ctx context.Context, leaderID uint64) (uint64, error) {
	return s.getReadIndexFromLeader(&ctx, leaderID)
}

func (s *Service) WaitUntilApplied(ctx context.Context, index uint64) error {
	return s.waitUntilApplied(&ctx, index)
}

func (s *Service) ProposeAddNode(ctx context.Context, nodeID uint64, raftAddr string) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	slog.Info("proposing add node", "node_id", s.Node.Id, "target", nodeID, "raftAddr", raftAddr)

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeID,
		Context: encodePeerAddrs(raftAddr),
	}

	return s.Node.raftNode.ProposeConfChange(ctx, cc)
}

func (s *Service) ProposeAddLearnerNode(ctx context.Context, nodeID uint64, raftAddr string) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	slog.Info("proposing add Learner node", "node_id", s.Node.Id, "target", nodeID, "raftAddr", raftAddr)

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddLearnerNode,
		NodeID:  nodeID,
		Context: encodePeerAddrs(raftAddr),
	}

	return s.Node.raftNode.ProposeConfChange(ctx, cc)
}

func (s *Service) ProposeRemoveNode(ctx context.Context, nodeID uint64) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	slog.Info("proposing remove node", "node_id", s.Node.Id, "target", nodeID)

	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: nodeID,
	}

	return s.Node.raftNode.ProposeConfChange(ctx, cc)
}

func (s *Service) ProposeAddLearner(ctx context.Context, nodeID uint64, addr string) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	slog.Info("proposing add Learner", "node_id", s.Node.Id, "target", nodeID)

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddLearnerNode,
		NodeID:  nodeID,
		Context: []byte(addr),
	}

	return s.Node.raftNode.ProposeConfChange(ctx, cc)
}

func (s *Service) Step(ctx context.Context, msg raftpb.Message) error {
	return s.CallRaftStep(ctx, msg)
}

func (s *Service) Propose(ctx context.Context, data []byte) error {
	if s.shuttingDown.Load() {
		return errors.New("service is shutting down")
	}
	if s.Node.Status().Lead == 0 {
		return ErrNotLeader
	}

	return s.Node.Propose(ctx, data)
}

func (s *Service) ReadIndex(ctx context.Context) (uint64, error) {
	if s.shuttingDown.Load() {
		return 0, errors.New("service is shutting down")
	}
	return s.doReadIndex(ctx)
}

func (s *Service) IsLeader() bool {
	select {
	case <-s.stopCh:
		return false
	default:
	}
	return s.Node.Status().RaftState == etcdraft.StateLeader
}

func (s *Service) LeaderID() uint64 {
	return s.Node.Status().Lead
}

func (s *Service) NodeID() uint64 {
	return s.Node.Id
}

func (s *Service) Start() {
	slog.Info("starting raft service", "node_id", s.Node.Id)

	if err := s.RecoverState(); err != nil {
		slog.Error("failed to recover state", "error", err)
	}

	s.Node.restoreFromConfState()
	s.startLoop()

	if s.Node.IsJoining {
		go s.requestJoin()
	}

	slog.Info("raft service started", "node_id", s.Node.Id)
}

func (s *Service) requestJoin() {
	time.Sleep(2 * time.Second)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			confState := s.Node.ConfState()
			if s.isInCluster(confState) {
				slog.Info("successfully joined cluster", "node_id", s.Node.Id)
				return
			}

			if err := s.tryJoin(); err != nil {
				slog.Debug("join attempt failed, retrying", "error", err)
			}
		}
	}
}

func (s *Service) isInCluster(cs raftpb.ConfState) bool {
	for _, id := range cs.Voters {
		if id == s.Node.Id {
			return true
		}
	}
	for _, id := range cs.Learners {
		if id == s.Node.Id {
			return true
		}
	}
	return false
}

func (s *Service) tryJoin() error {
	peers := s.Node.Peers()

	for peerID := range peers {
		if peerID == s.Node.Id {
			continue
		}

		client, ok := s.Node.GetLeaderClient(peerID)
		if !ok {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.RequestJoinCluster(ctx, &rafttransportpb.JoinRequest{
			NodeId:   s.Node.Id,
			RaftAddr: s.localRaftAddr,
		})
		cancel()

		if err != nil {
			slog.Debug("join request failed", "peer_id", peerID, "error", err)
			continue
		}

		if resp.Accepted {
			slog.Info("join request accepted", "by_peer", peerID)
			return nil
		}

	}

	return fmt.Errorf("no peer accepted join request")
}
