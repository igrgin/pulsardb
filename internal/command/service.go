package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"pulsardb/internal/transport/util"
	"sync"

	"pulsardb/internal/raft"
	"pulsardb/internal/storage"
	"pulsardb/internal/transport/gen/commandevents"
	"pulsardb/internal/types"

	"google.golang.org/protobuf/proto"
)

type RaftProposer interface {
	Propose(ctx context.Context, data []byte) error
	GetReadIndex(ctx context.Context) (uint64, error)
	GetReadIndexFromLeader(ctx context.Context, leaderID uint64) (uint64, error)
	WaitUntilApplied(ctx context.Context, index uint64) error
	IsLeader() bool
	LeaderID() uint64
	NodeID() uint64
	ForwardToLeader(ctx context.Context, req *commandeventspb.CommandEventRequest) (*commandeventspb.CommandEventResponse, error)
}

type Service struct {
	storageService *storage.Service
	raft           RaftProposer
	pending        map[uint64]chan *commandeventspb.CommandEventResponse
	pendingMu      sync.RWMutex
	batcher        *raft.Batcher
}

func NewCommandService(
	storageSvc *storage.Service,
	raft RaftProposer,
	batcher *raft.Batcher,
) *Service {

	return &Service{
		storageService: storageSvc,
		pending:        make(map[uint64]chan *commandeventspb.CommandEventResponse),
		raft:           raft,
		batcher:        batcher,
	}
}

func (s *Service) Apply(data []byte) ([]byte, error) {
	var batch commandeventspb.BatchedCommands
	if err := proto.Unmarshal(data, &batch); err == nil && len(batch.Commands) > 0 {
		for _, req := range batch.Commands {
			resp := s.executeToStorage(req)
			s.notifyPending(req.EventId, resp)
		}
		return nil, nil
	}

	var req commandeventspb.CommandEventRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("unmarshal command: %w", err)
	}

	resp := s.executeToStorage(&req)
	s.notifyPending(req.EventId, resp)
	return nil, nil
}

func (s *Service) executeToStorage(req *commandeventspb.CommandEventRequest) *commandeventspb.CommandEventResponse {

	switch req.Type {
	case commandeventspb.CommandEventType_SET:
		val := types.ValueFromProto(req.Value)
		s.storageService.Set(req.Key, val)
		return transport.SuccessModifyResponse(req.EventId)

	case commandeventspb.CommandEventType_DELETE:
		s.storageService.Delete(req.Key)
		return transport.SuccessModifyResponse(req.EventId)

	case commandeventspb.CommandEventType_GET:
		stored, ok := s.storageService.Get(req.Key)
		if !ok {
			return transport.ErrorResponse(req.EventId, commandeventspb.ErrorCode_KEY_NOT_FOUND,
				fmt.Sprintf("key %q not found", req.Key))
		}
		return transport.SuccessReadResponse(req.EventId, types.ValueToProto(stored))

	default:
		return transport.ErrorResponse(req.EventId, commandeventspb.ErrorCode_INVALID_REQUEST,
			fmt.Sprintf("unknown type: %s", req.Type))
	}
}

func (s *Service) ProcessCommand(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (*commandeventspb.CommandEventResponse, error) {
	if err := s.validateCommand(req); err != nil {
		return transport.ErrorResponse(req.EventId, commandeventspb.ErrorCode_INVALID_REQUEST, err.Error()), err
	}

	switch req.Type {
	case commandeventspb.CommandEventType_GET:
		resp, err := s.processRead(ctx, req)
		return resp, err

	case commandeventspb.CommandEventType_SET, commandeventspb.CommandEventType_DELETE:
		resp, err := s.processModify(ctx, req)
		return resp, err

	default:
		err := fmt.Errorf("unknown command type: %s", req.Type)
		return transport.ErrorResponse(req.EventId, commandeventspb.ErrorCode_INVALID_REQUEST, err.Error()), err
	}
}

func (s *Service) processRead(ctx context.Context, req *commandeventspb.CommandEventRequest) (*commandeventspb.CommandEventResponse, error) {
	readIndex, err := s.raft.GetReadIndex(ctx)
	if err != nil {
		return nil, fmt.Errorf("get read index: %w", err)
	}

	if err := s.raft.WaitUntilApplied(ctx, readIndex); err != nil {
		return nil, fmt.Errorf("wait for applied: %w", err)
	}

	return s.ExecuteRead(req.Key, req.EventId), nil
}

func (s *Service) ExecuteRead(key string, eventID uint64) *commandeventspb.CommandEventResponse {
	stored, ok := s.storageService.Get(key)
	if !ok {
		slog.Error("Key not found", "key", key)
		return transport.ErrorResponse(eventID, commandeventspb.ErrorCode_KEY_NOT_FOUND,
			fmt.Sprintf("key `%s` not found", key))
	}

	return transport.SuccessReadResponse(eventID, types.ValueToProto(stored))
}

func (s *Service) processModify(ctx context.Context, req *commandeventspb.CommandEventRequest) (*commandeventspb.CommandEventResponse, error) {
	respCh, err := s.batcher.Propose(ctx, req)
	if err != nil {
		return nil, err
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Service) RegisterPending(eventID uint64, ch chan *commandeventspb.CommandEventResponse) {
	s.pendingMu.Lock()
	s.pending[eventID] = ch
	s.pendingMu.Unlock()
}

func (s *Service) UnregisterPending(eventID uint64) {
	s.pendingMu.Lock()
	delete(s.pending, eventID)
	s.pendingMu.Unlock()
}

func (s *Service) notifyPending(eventID uint64, resp *commandeventspb.CommandEventResponse) {
	s.pendingMu.RLock()
	ch, ok := s.pending[eventID]
	s.pendingMu.RUnlock()

	if ok {
		select {
		case ch <- resp:
		default:
		}
	}
}

func (s *Service) NodeID() uint64 {
	return s.raft.NodeID()
}

func (s *Service) validateCommand(req *commandeventspb.CommandEventRequest) error {
	if req.Key == "" {
		return errors.New("key is required")
	}

	switch req.Type {
	case commandeventspb.CommandEventType_GET, commandeventspb.CommandEventType_DELETE:
		if req.Value != nil {
			return fmt.Errorf("%s does not accept a value", req.Type)
		}
	case commandeventspb.CommandEventType_SET:
		if req.Value == nil {
			return errors.New("SET requires a value")
		}
	default:
		return fmt.Errorf("unknown command type: %s", req.Type)
	}

	return nil
}
