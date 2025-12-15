package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/storage"
	"pulsardb/internal/transport/gen/commandevents"
	rafttransportpb "pulsardb/internal/transport/gen/raft"
	"pulsardb/internal/types"
	"runtime"
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
	Node           *Node
	storeService   *storage.Service
	stateMachine   StateMachine
	nextReqID      uint64
	readWaiters    map[string]*readWaiter
	readMu         sync.Mutex
	lastApplied    uint64
	appliedMu      sync.RWMutex
	stopCh         chan int
	stoppedWg      sync.WaitGroup
	tickInterval   uint64
	snapCount      uint64
	stepInbox      chan raftStepReq
	readIndexInbox chan readIndexReq
}

func NewService(node *Node, storeSvc *storage.Service, rc *properties.RaftConfigProperties, sm StateMachine) *Service {
	stepSize := 1024
	readIndexSize := 1024

	return &Service{
		Node:           node,
		storeService:   storeSvc,
		readWaiters:    make(map[string]*readWaiter),
		lastApplied:    0,
		stopCh:         make(chan int),
		tickInterval:   rc.TickInterval,
		snapCount:      rc.SnapCount,
		stepInbox:      make(chan raftStepReq, stepSize),
		readIndexInbox: make(chan readIndexReq, readIndexSize),
		stateMachine:   sm,
	}
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

func (s *Service) startReadIndexWorkers() {
	workers := runtime.GOMAXPROCS(0)
	if workers > 4 {
		workers = 4
	}
	if workers < 1 {
		workers = 1
	}

	for i := 0; i < workers; i++ {
		s.stoppedWg.Add(1)
		go func() {
			defer s.stoppedWg.Done()
			for {
				select {
				case <-s.stopCh:
					return
				case req := <-s.readIndexInbox:
					idx, err := s.doReadIndex(req.ctx)
					select {
					case req.resp <- readIndexResp{index: idx, err: err}:
					default:
					}
				}
			}
		}()
	}
}

func (s *Service) doReadIndex(ctx context.Context) (uint64, error) {

	reqID := s.NextRequestID()
	reqCtx := []byte(fmt.Sprintf("read-%d", reqID))
	reqCtxKey := string(reqCtx)

	ch := make(chan uint64, 1)
	s.RegisterReadWaiter(reqCtxKey, ch)
	defer s.UnregisterReadWaiter(reqCtxKey)

	if err := s.Node.ReadIndex(ctx, reqCtx); err != nil {
		return 0, fmt.Errorf("ReadIndex: %w", err)
	}

	select {
	case idx := <-ch:
		return idx, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.stopCh:
		return 0, context.Canceled
	}
}

func (s *Service) Stop() {
	close(s.stopCh)
	s.stoppedWg.Wait()

	s.Node.stopSendPool()
	s.Node.StopClients()

	if err := s.Node.storage.Close(); err != nil {
		slog.Error("failed to close raft storage", "error", err)
	}

	slog.Info("raft Node stopped", "id", s.Node.Id)
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
	}
	newLA = s.lastApplied
	s.appliedMu.Unlock()

	s.completeReadWaiters(newLA)
}

func (s *Service) completeReadWaiters(lastApplied uint64) {
	s.readMu.Lock()
	defer s.readMu.Unlock()

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
		}
	}
}

func (s *Service) NextRequestID() uint64 {
	return atomic.AddUint64(&s.nextReqID, 1)
}

func (s *Service) forwardModifyToLeader(ctx *context.Context, req *commandeventspb.CommandEventRequest, leaderID uint64) (*commandeventspb.CommandEventResponse, error) {
	if leaderID == 0 {
		return nil, fmt.Errorf("no known leader")
	}

	client, ok := s.Node.GetLeaderClient(leaderID)
	if !ok {
		return nil, fmt.Errorf("no client for leader %d", leaderID)
	}

	resp, err := client.ProcessCommandEvent(*ctx, req)
	if err != nil {
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

	resp, err := client.GetReadIndex(ctx, &rafttransportpb.GetReadIndexRequest{FromNode: s.Node.Id})
	if err != nil {
		return 0, fmt.Errorf("forward to leader %d: %w", leaderID, err)
	}

	return resp.ReadIndex, nil
}

func (s *Service) waitUntilApplied(ctx *context.Context, index uint64) error {
	if s.LastApplied() >= index {
		return nil
	}

	key := fmt.Sprintf("applied-%d-%d", index, s.NextRequestID())
	ch := make(chan uint64, 1)

	s.readMu.Lock()
	s.readWaiters[key] = &readWaiter{index: index, ch: ch}
	s.readMu.Unlock()
	defer s.UnregisterReadWaiter(key)

	select {
	case <-ch:
		return nil
	case <-(*ctx).Done():
		return (*ctx).Err()
	}
}

func (s *Service) HandleReadStates(readStates []etcdraft.ReadState) {
	if len(readStates) == 0 {
		return
	}

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
	slog.Debug("applying entry", "index", entry.Index, "data_len", len(entry.Data))
	if len(entry.Data) == 0 {
		return
	}

	_, err := s.stateMachine.Apply(entry.Data)
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
		return nil
	}

	data, err := s.storeService.GetSnapshot()
	if err != nil {
		return fmt.Errorf("get snapshot data: %w", err)
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
	if err := s.RecoverState(); err != nil {
		slog.Error("failed to recover state", "node_id", s.Node.Id, "error", err)
	}
	s.Node.restoreFromConfState()
	s.startReadIndexWorkers()
	s.startLoop()
}

func (s *Service) RecoverState() error {
	snapIndex := s.Node.Storage().SnapshotIndex()
	snapData := s.Node.Storage().SnapshotData()

	if len(snapData) > 0 {
		if err := s.storeService.RestoreFromSnapshot(snapData); err != nil {
			return fmt.Errorf("restore from snapshot: %w", err)
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
		return nil
	}

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
		s.storeService.Set(req.GetKey(), val)
	case commandeventspb.CommandEventType_DELETE:
		s.storeService.Delete(req.GetKey())
	}
	return nil
}

func (s *Service) applyConfChange(entry raftpb.Entry) {
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		slog.Error("failed to unmarshal conf change", "node_id", s.Node.Id, "index", entry.Index, "error", err)
		return
	}

	confState := s.Node.ApplyConfChange(cc)
	if confState != nil {
		s.Node.SetConfState(*confState)
		if err := s.Node.storage.SaveConfState(*confState); err != nil {
			slog.Error("failed to persist confState", "node_id", s.Node.Id, "error", err)
		}
	}

	s.updatePeersFromConfChange(cc)
}

func (s *Service) updatePeersFromConfChange(cc raftpb.ConfChange) {
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if len(cc.Context) == 0 || cc.NodeID == s.Node.Id {
			return
		}
		addr := string(cc.Context)
		s.Node.mu.Lock()
		s.Node.raftPeers[cc.NodeID] = addr
		s.Node.mu.Unlock()
		if err := s.Node.initPeerClient(cc.NodeID, addr); err != nil {
			slog.Error("failed to init client for new peer", "peer_id", cc.NodeID, "error", err)
		}

	case raftpb.ConfChangeRemoveNode:
		if cc.NodeID == s.Node.Id {
			return
		}
		s.Node.mu.Lock()
		c := s.Node.clients[cc.NodeID]
		delete(s.Node.raftPeers, cc.NodeID)
		delete(s.Node.clients, cc.NodeID)
		s.Node.mu.Unlock()
		if c != nil {
			if err := c.Close(); err != nil {
				slog.Warn("failed to close removed peer client", "peer_id", cc.NodeID, "error", err)
			}
		}
	}
}

func (s *Service) WaitForQuorum(ctx context.Context) (leaderID uint64, readIndex uint64, err error) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()

	for {
		st := s.Node.Status()
		if st.Lead == 0 {
		} else if st.RaftState == etcdraft.StateLeader {
			idx, e := s.GetReadIndex(ctx)
			if e == nil {
				return st.Lead, idx, nil
			}
		} else {
			idx, e := s.getReadIndexFromLeader(&ctx, st.Lead)
			if e == nil {
				if e2 := s.waitUntilApplied(&ctx, idx); e2 == nil {
					return st.Lead, idx, nil
				}
			}
		}

		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-s.stopCh:
			return 0, 0, context.Canceled
		case <-t.C:
		}
	}
}

func (s *Service) Propose(ctx context.Context, data []byte) error {
	return s.Node.Propose(ctx, data)
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

func (s *Service) IsLeader() bool {
	return s.Node.Status().RaftState == etcdraft.StateLeader
}

func (s *Service) LeaderID() uint64 {
	return s.Node.Status().Lead
}

func (s *Service) NodeID() uint64 {
	return s.Node.Id
}

func (s *Service) ForwardToLeader(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (*commandeventspb.CommandEventResponse, error) {
	leaderID := s.LeaderID()
	if leaderID == 0 {
		return nil, fmt.Errorf("no known leader")
	}

	client, ok := s.Node.GetLeaderClient(leaderID)
	if !ok {
		return nil, fmt.Errorf("no client for leader %d", leaderID)
	}

	return client.ProcessCommandEvent(ctx, req)
}
