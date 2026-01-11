package command

import (
	"context"
	"fmt"
	"log/slog"
	"pulsardb/convert"
	"pulsardb/internal/domain"
	"pulsardb/internal/metrics"
	"pulsardb/internal/raft/coordinator"
	"pulsardb/internal/transport/gen/command"
	"sync"
	"time"
)

type BatchConfig struct {
	MaxSize             int
	MaxWait             time.Duration
	CleanupTickInterval time.Duration
}

type Processor struct {
	storageService domain.Store
	proposer       domain.Consensus
	Batcher        *Batcher
	Pending        map[uint64]chan *commandeventspb.CommandEventResponse
	Mu             sync.RWMutex
}

func NewProcessor(storageService domain.Store, proposer domain.Consensus, batchCfg BatchConfig) *Processor {
	s := &Processor{
		storageService: storageService,
		proposer:       proposer,
		Pending:        make(map[uint64]chan *commandeventspb.CommandEventResponse),
	}
	s.Batcher = NewBatcher(proposer, s, batchCfg)
	slog.Info("command service initialized")
	return s
}

func (s *Processor) Stop() {
	s.Batcher.Stop()
	slog.Info("command service stopped")
}

func (s *Processor) ProcessCommand(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (*commandeventspb.CommandEventResponse, error) {
	start := time.Now()
	cmdType := req.Type.String()

	metrics.CommandsInFlight.Inc()
	defer metrics.CommandsInFlight.Dec()

	slog.Debug("processing command", "eventId", req.EventId, "type", req.Type, "key", req.Key)

	if err := s.validate(req); err != nil {
		slog.Warn("command validation failed", "eventId", req.EventId, "error", err)
		metrics.CommandsTotal.WithLabelValues(cmdType, "invalid").Inc()
		return nil, fmt.Errorf("command validation failed with error: %w", err)
	}

	var resp *commandeventspb.CommandEventResponse
	var err error

	switch req.Type {
	case commandeventspb.CommandEventType_GET:
		resp, err = s.read(ctx, req)
	case commandeventspb.CommandEventType_SET, commandeventspb.CommandEventType_DELETE:
		resp, err = s.write(ctx, req)
	default:
		slog.Warn("unknown command type", "eventId", req.EventId, "type", req.Type)
		metrics.CommandsTotal.WithLabelValues(cmdType, "invalid").Inc()
		return nil, fmt.Errorf("%w: unknown type %s", ErrInvalidCommand, req.Type)
	}

	duration := time.Since(start).Seconds()
	metrics.CommandDuration.WithLabelValues(cmdType).Observe(duration)

	if err != nil {
		metrics.CommandsTotal.WithLabelValues(cmdType, "error").Inc()
	} else {
		metrics.CommandsTotal.WithLabelValues(cmdType, "success").Inc()
	}

	return resp, err
}

func (s *Processor) GetLeaderInfo() (uint64, string) {
	leaderID := s.proposer.LeaderID()
	if leaderID == 0 {
		return 0, ""
	}

	addr := s.proposer.GetPeerAddr(leaderID)
	return leaderID, addr
}

func (s *Processor) read(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (*commandeventspb.CommandEventResponse, error) {
	slog.Debug("read operation", "eventId", req.EventId, "key", req.Key)

	if _, err := s.proposer.ReadIndex(ctx); err != nil {
		slog.Warn("read index failed", "eventId", req.EventId, "error", err)
		return nil, err
	}

	val, ok := s.storageService.Get(req.Key)
	if !ok {
		slog.Debug("key not found", "eventId", req.EventId, "key", req.Key)
		return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, req.Key)
	}

	slog.Debug("read success", "eventId", req.EventId, "key", req.Key)
	return &commandeventspb.CommandEventResponse{
		EventId: req.EventId,
		Success: true,
		Value:   convert.ToCommandProto(val),
	}, nil
}

func (s *Processor) write(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (*commandeventspb.CommandEventResponse, error) {
	slog.Debug("write operation", "eventId", req.EventId, "type", req.Type, "key", req.Key)

	if s.proposer.LeaderID() == uint64(0) {
		return nil, coordinator.ErrNoLeader
	}

	if !s.proposer.IsLeader() {
		return nil, coordinator.ErrNotLeader
	}

	respCh, err := s.Batcher.Submit(ctx, req)
	if err != nil {
		slog.Warn("submit to batcher failed", "eventId", req.EventId, "error", err)
		return nil, err
	}

	select {
	case resp := <-respCh:
		if resp.Success {
			slog.Debug("write success", "eventId", req.EventId)
		} else {
			slog.Debug("write failed", "eventId", req.EventId, "error", resp.Error)
		}
		return resp, nil
	case <-ctx.Done():
		slog.Warn("write timed out", "eventId", req.EventId, "error", ctx.Err())
		return nil, ctx.Err()
	}
}

func (s *Processor) validate(req *commandeventspb.CommandEventRequest) error {
	if req.Key == "" {
		return fmt.Errorf("%w: key is required", ErrInvalidCommand)
	}

	switch req.Type {
	case commandeventspb.CommandEventType_GET, commandeventspb.CommandEventType_DELETE:
		if req.Value != nil {
			return fmt.Errorf("%w: %s does not accept a value", ErrInvalidCommand, req.Type)
		}
	case commandeventspb.CommandEventType_SET:
		if req.Value == nil {
			return fmt.Errorf("%w: SET requires a value", ErrInvalidCommand)
		}
	default:
		return fmt.Errorf("%w: unknown type %s", ErrInvalidCommand, req.Type)
	}

	return nil
}

func (s *Processor) HandleApplied(eventID uint64, resp *commandeventspb.CommandEventResponse) {
	s.Mu.RLock()
	ch, ok := s.Pending[eventID]
	s.Mu.RUnlock()

	if ok {
		slog.Debug("command applied", "eventId", eventID, "success", resp.Success)
		select {
		case ch <- resp:
		default:
			slog.Warn("failed to deliver response, channel full", "eventId", eventID)
		}
	} else {
		slog.Debug("no pending handler for applied command", "eventId", eventID)
	}
}

func (s *Processor) Register(eventID uint64, ch chan *commandeventspb.CommandEventResponse) {
	s.Mu.Lock()
	s.Pending[eventID] = ch
	s.Mu.Unlock()
	slog.Debug("registered pending command", "eventId", eventID)
}

func (s *Processor) Unregister(eventID uint64) {
	s.Mu.Lock()
	delete(s.Pending, eventID)
	s.Mu.Unlock()
	slog.Debug("unregistered pending command", "eventId", eventID)
}

func (s *Processor) NodeID() uint64 {
	return s.proposer.NodeID()
}
