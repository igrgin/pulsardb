package command

import (
	"context"
	"fmt"
	"log/slog"
	"pulsardb/convert"
	"sync"
	"time"

	"pulsardb/internal/domain"
	"pulsardb/internal/transport/gen/commandevents"
)

type BatchConfig struct {
	MaxSize int
	MaxWait time.Duration // milliseconds
}

type Service struct {
	store    domain.Store
	proposer domain.Consensus
	batcher  *Batcher
	pending  map[uint64]chan *commandeventspb.CommandEventResponse
	mu       sync.RWMutex
}

func NewService(store domain.Store, proposer domain.Consensus, batchCfg BatchConfig) *Service {
	s := &Service{
		store:    store,
		proposer: proposer,
		pending:  make(map[uint64]chan *commandeventspb.CommandEventResponse),
	}
	s.batcher = NewBatcher(proposer, s, batchCfg)
	slog.Info("command service initialized")
	return s
}

func (s *Service) Stop() {
	slog.Info("command service stopping")
	s.batcher.Stop()
	slog.Info("command service stopped")
}

func (s *Service) ProcessCommand(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (*commandeventspb.CommandEventResponse, error) {
	slog.Debug("processing command", "eventId", req.EventId, "type", req.Type, "key", req.Key)

	if err := s.validate(req); err != nil {
		slog.Warn("command validation failed", "eventId", req.EventId, "error", err)
		return nil, err
	}

	switch req.Type {
	case commandeventspb.CommandEventType_GET:
		return s.read(ctx, req)
	case commandeventspb.CommandEventType_SET, commandeventspb.CommandEventType_DELETE:
		return s.write(ctx, req)
	default:
		slog.Warn("unknown command type", "eventId", req.EventId, "type", req.Type)
		return nil, fmt.Errorf("%w: unknown type %s", ErrInvalidCommand, req.Type)
	}
}

func (s *Service) read(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (*commandeventspb.CommandEventResponse, error) {
	slog.Debug("read operation", "eventId", req.EventId, "key", req.Key)

	if _, err := s.proposer.ReadIndex(ctx); err != nil {
		slog.Warn("read index failed", "eventId", req.EventId, "error", err)
		return nil, err
	}

	val, ok := s.store.Get(req.Key)
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

func (s *Service) write(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (*commandeventspb.CommandEventResponse, error) {
	slog.Debug("write operation", "eventId", req.EventId, "type", req.Type, "key", req.Key)

	if s.proposer.LeaderID() == 0 {
		slog.Warn("no leader available", "eventId", req.EventId)
		return nil, ErrNoLeader
	}

	respCh, err := s.batcher.Submit(ctx, req)
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

func (s *Service) validate(req *commandeventspb.CommandEventRequest) error {
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

func (s *Service) HandleApplied(eventID uint64, resp *commandeventspb.CommandEventResponse) {
	s.mu.RLock()
	ch, ok := s.pending[eventID]
	s.mu.RUnlock()

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

func (s *Service) Register(eventID uint64, ch chan *commandeventspb.CommandEventResponse) {
	s.mu.Lock()
	s.pending[eventID] = ch
	s.mu.Unlock()
	slog.Debug("registered pending command", "eventId", eventID)
}

func (s *Service) Unregister(eventID uint64) {
	s.mu.Lock()
	delete(s.pending, eventID)
	s.mu.Unlock()
	slog.Debug("unregistered pending command", "eventId", eventID)
}

func (s *Service) NodeID() uint64 {
	return s.proposer.NodeID()
}
