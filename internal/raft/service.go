package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"pulsardb/internal/configuration"
	"pulsardb/internal/domain"
	"pulsardb/internal/store"
	"pulsardb/internal/transport/gen/commandevents"
	"pulsardb/internal/transport/gen/raft"
	"pulsardb/internal/types"
	"sync"
	"sync/atomic"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

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
	Node              *Node
	StoreService      *store.Service
	StateMachine      domain.StateMachine
	nextReqID         uint64
	readWaiters       map[string]*readWaiter
	readMu            sync.Mutex
	lastApplied       uint64
	appliedMu         sync.RWMutex
	stopCh            chan int
	stoppedWg         sync.WaitGroup
	tickInterval      time.Duration
	snapCount         uint64
	stepInbox         chan raftStepReq
	readIndexInbox    chan readIndexReq
	sendPoolQueueSize int
}

func NewService(node *Node,
	storageService *store.Service,
	sm domain.StateMachine,
	cfg *configuration.RaftConfigurationProperties) *Service {
	stepSize := 1024
	readIndexSize := 1024

	s := &Service{
		Node:              node,
		StoreService:      storageService,
		StateMachine:      sm,
		readWaiters:       make(map[string]*readWaiter),
		lastApplied:       0,
		stopCh:            make(chan int),
		tickInterval:      cfg.TickInterval,
		snapCount:         cfg.SnapCount,
		stepInbox:         make(chan raftStepReq, stepSize),
		readIndexInbox:    make(chan readIndexReq, readIndexSize),
		sendPoolQueueSize: cfg.SendQueueSize,
	}

	slog.Info("raft service created",
		"node_id", node.Id,
		"tickInterval", cfg.TickInterval,
		"snapCount", cfg.SnapCount,
	)

	return s
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
	if s.Node.Status().Lead == 0 {
		slog.Debug("read index failed: no leader", "node_id", s.Node.Id)
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
		return 0, fmt.Errorf("ReadIndex: %w", err)
	}

	select {
	case idx := <-ch:
		slog.Debug("read index received", "node_id", s.Node.Id, "index", idx)
		return idx, nil
	case <-ctx.Done():
		slog.Debug("read index timeout", "node_id", s.Node.Id)
		return 0, ctx.Err()
	case <-s.stopCh:
		return 0, context.Canceled
	}
}

func (s *Service) Stop() {
	slog.Info("stopping raft service", "node_id", s.Node.Id)
	close(s.stopCh)
	s.stoppedWg.Wait()

	s.Node.stopSendPool()
	s.Node.StopClients()

	if err := s.Node.storage.Close(); err != nil {
		slog.Error("failed to close raft storage", "error", err)
	}

	slog.Info("raft service stopped", "node_id", s.Node.Id)
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

func (s *Service) forwardModifyToLeader(
	ctx *context.Context,
	req *commandeventspb.CommandEventRequest,
	leaderID uint64,
) (*commandeventspb.CommandEventResponse, error) {
	if leaderID == 0 {
		return nil, fmt.Errorf("no known leader")
	}

	client, ok := s.Node.GetLeaderClient(leaderID)
	if !ok {
		return nil, fmt.Errorf("no client for leader %d", leaderID)
	}

	slog.Debug("forwarding command to leader", "event_id", req.EventId, "leader_id", leaderID)
	resp, err := client.ProcessCommandEvent(*ctx, req)
	if err != nil {
		slog.Debug("forward to leader failed", "event_id", req.EventId, "leader_id", leaderID, "error", err)
		return nil, fmt.Errorf("forward to leader %d: %w", leaderID, err)
	}
	return resp, nil
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
	resp, err := client.GetReadIndex(ctx, &rafttransportpb.GetReadIndexRequest{FromNode: s.Node.Id})
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

	err := s.StateMachine.Apply(entry.Data)
	if err != nil {
		slog.Error("state machine apply failed",
			"node_id", s.Node.Id,
			"index", entry.Index,
			"error", err,
		)
	}
}

func (s *Service) TriggerSnapshot(snapCount uint64) error {
	lastApplied := s.LastApplied()
	if lastApplied == 0 {
		slog.Debug("skip snapshot: no applied entries")
		return nil
	}

	slog.Debug("triggering snapshot", "lastApplied", lastApplied)

	data, err := s.StoreService.Snapshot()
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

	slog.Info("triggered snapshot",
		"index", snap.Metadata.Index,
		"term", snap.Metadata.Term,
		"compact_index", compactIndex,
		"data_size", len(data),
	)

	return nil
}

func (s *Service) Start() {
	slog.Info("starting raft service", "node_id", s.Node.Id)

	if err := s.RecoverState(); err != nil {
		slog.Error("failed to recover state", "node_id", s.Node.Id, "error", err)
	}

	s.Node.restoreFromConfState()
	s.startLoop()

	slog.Info("raft service started", "node_id", s.Node.Id)
}

func (s *Service) ReconcileConfiguredPeers() {
	time.Sleep(5 * time.Second)

	if !s.IsLeader() {
		slog.Debug("skipping peer reconciliation: not leader", "node_id", s.Node.Id)
		return
	}

	slog.Debug("reconciling configured peers", "node_id", s.Node.Id)

	confState := s.Node.ConfState()
	configuredPeers := s.Node.Peers()

	configuredSet := make(map[uint64]bool)
	configuredSet[s.Node.Id] = true
	for nodeID := range configuredPeers {
		configuredSet[nodeID] = true
	}

	voterSet := make(map[uint64]bool)
	for _, v := range confState.Voters {
		voterSet[v] = true
	}

	for nodeID, addr := range configuredPeers {
		if nodeID == s.Node.Id {
			continue
		}
		if !voterSet[nodeID] {
			slog.Info("proposing new peer from config", "node_id", nodeID, "addr", addr)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := s.ProposeAddNode(ctx, nodeID, addr); err != nil {
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

	addr := string(cc.Context)
	if addr == "" {
		s.Node.mu.RLock()
		addr = s.Node.raftPeers[cc.NodeID]
		s.Node.mu.RUnlock()
	}

	if addr == "" {
		slog.Warn("no address for new learner", "learner_id", cc.NodeID)
		return
	}

	s.Node.mu.Lock()
	s.Node.raftPeers[cc.NodeID] = addr
	s.Node.mu.Unlock()

	if err := s.Node.initPeerClient(cc.NodeID, addr); err != nil {
		slog.Error("failed to init client for learner",
			"learner_id", cc.NodeID,
			"error", err,
		)
	}

	s.Node.startPeerSender(cc.NodeID, s.sendPoolQueueSize)

	slog.Info("learner added to cluster",
		"node_id", s.Node.Id,
		"learner_id", cc.NodeID,
		"addr", addr,
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

	addr := string(cc.Context)
	if addr == "" {
		s.Node.mu.RLock()
		addr = s.Node.raftPeers[cc.NodeID]
		s.Node.mu.RUnlock()
	}

	if addr == "" {
		slog.Warn("no address for new voter", "voter_id", cc.NodeID)
		return
	}

	s.Node.mu.Lock()
	s.Node.raftPeers[cc.NodeID] = addr
	s.Node.mu.Unlock()

	s.Node.mu.RLock()
	_, hasClient := s.Node.clients[cc.NodeID]
	s.Node.mu.RUnlock()

	if !hasClient {
		if err := s.Node.initPeerClient(cc.NodeID, addr); err != nil {
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
		"addr", addr,
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
	client := s.Node.clients[cc.NodeID]
	delete(s.Node.raftPeers, cc.NodeID)
	delete(s.Node.clients, cc.NodeID)
	s.Node.mu.Unlock()

	if client != nil {
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

func (s *Service) ForwardToLeader(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (*commandeventspb.CommandEventResponse, error) {
	leaderID := s.LeaderID()
	if leaderID == 0 {
		slog.Debug("forward failed: no leader", "event_id", req.EventId)
		return nil, fmt.Errorf("no known leader")
	}

	client, ok := s.Node.GetLeaderClient(leaderID)
	if !ok {
		slog.Warn("forward failed: no client for leader", "event_id", req.EventId, "leader_id", leaderID)
		return nil, fmt.Errorf("no client for leader %d", leaderID)
	}

	slog.Debug("forwarding to leader", "event_id", req.EventId, "leader_id", leaderID)
	return client.ProcessCommandEvent(ctx, req)
}

func (s *Service) ProposeAddNode(ctx context.Context, nodeID uint64, addr string) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	slog.Info("proposing add node", "node_id", s.Node.Id, "target", nodeID, "addr", addr)

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeID,
		Context: []byte(addr),
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

// Propose implements command.Proposer
func (s *Service) Propose(ctx context.Context, data []byte) error {
	if s.Node.Status().Lead == 0 {
		return ErrNotLeader
	}
	return s.Node.Propose(ctx, data)
}

// ReadIndex implements command.Proposer
func (s *Service) ReadIndex(ctx context.Context) (uint64, error) {
	return s.doReadIndex(ctx)
}

func (s *Service) IsLeader() bool {
	return s.Node.Status().RaftState == etcdraft.StateLeader
}

func (s *Service) LeaderID() uint64 {
	return s.Node.Status().Lead
}

func (s *Service) NodeID() uint64 {
	return s.Node.Id
}
