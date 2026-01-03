package statemachine

import (
	"fmt"
	"pulsardb/convert"
	"sync"

	"pulsardb/internal/domain"
	"pulsardb/internal/transport/gen/command"

	"google.golang.org/protobuf/proto"
)

type ApplyCallback func(eventID uint64, resp *commandeventspb.CommandEventResponse)

type StateMachine struct {
	storage   domain.Store
	callbacks []ApplyCallback
	mu        sync.RWMutex
}

func New(storage domain.Store) *StateMachine {
	return &StateMachine{
		storage:   storage,
		callbacks: make([]ApplyCallback, 0),
	}
}

func (sm *StateMachine) OnApply(cb ApplyCallback) {
	sm.mu.Lock()
	sm.callbacks = append(sm.callbacks, cb)
	sm.mu.Unlock()
}

func (sm *StateMachine) Apply(data []byte) error {

	var batch commandeventspb.BatchedCommands
	if err := proto.Unmarshal(data, &batch); err == nil && len(batch.Commands) > 0 {
		for _, cmd := range batch.Commands {
			resp := sm.execute(cmd)
			sm.notify(cmd.EventId, resp)
		}
		return nil
	}

	var cmd commandeventspb.CommandEventRequest
	if err := proto.Unmarshal(data, &cmd); err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}

	resp := sm.execute(&cmd)
	sm.notify(cmd.EventId, resp)
	return nil
}

func (sm *StateMachine) execute(cmd *commandeventspb.CommandEventRequest) *commandeventspb.CommandEventResponse {
	switch cmd.Type {
	case commandeventspb.CommandEventType_SET:
		val := convert.FromCommandProto(cmd.Value)
		sm.storage.Set(cmd.Key, val)
		return successResponse(cmd.EventId)

	case commandeventspb.CommandEventType_DELETE:
		sm.storage.Delete(cmd.Key)
		return successResponse(cmd.EventId)

	case commandeventspb.CommandEventType_GET:
		val, ok := sm.storage.Get(cmd.Key)
		if !ok {
			return errorResponse(cmd.EventId, commandeventspb.ErrorCode_KEY_NOT_FOUND,
				fmt.Sprintf("key %q not found", cmd.Key))
		}
		return readResponse(cmd.EventId, convert.ToCommandProto(val))

	default:
		return errorResponse(cmd.EventId, commandeventspb.ErrorCode_INVALID_REQUEST,
			fmt.Sprintf("unknown type: %s", cmd.Type))
	}
}

func (sm *StateMachine) notify(eventID uint64, resp *commandeventspb.CommandEventResponse) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, cb := range sm.callbacks {
		cb(eventID, resp)
	}
}

func (sm *StateMachine) ApplyReplay(data []byte) error {
	var batch commandeventspb.BatchedCommands
	if err := proto.Unmarshal(data, &batch); err == nil && len(batch.Commands) > 0 {
		for _, cmd := range batch.Commands {
			sm.applyToStorage(cmd)
		}
		return nil
	}

	var cmd commandeventspb.CommandEventRequest
	if err := proto.Unmarshal(data, &cmd); err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}
	sm.applyToStorage(&cmd)
	return nil
}

func (sm *StateMachine) applyToStorage(cmd *commandeventspb.CommandEventRequest) {
	switch cmd.Type {
	case commandeventspb.CommandEventType_SET:
		val := convert.FromCommandProto(cmd.Value)
		sm.storage.Set(cmd.Key, val)
	case commandeventspb.CommandEventType_DELETE:
		sm.storage.Delete(cmd.Key)
	}
}

func successResponse(eventID uint64) *commandeventspb.CommandEventResponse {
	return &commandeventspb.CommandEventResponse{
		EventId: eventID,
		Success: true,
	}
}

func readResponse(eventID uint64, val *commandeventspb.CommandEventValue) *commandeventspb.CommandEventResponse {
	return &commandeventspb.CommandEventResponse{
		EventId: eventID,
		Success: true,
		Value:   val,
	}
}

func errorResponse(eventID uint64, code commandeventspb.ErrorCode, msg string) *commandeventspb.CommandEventResponse {
	return &commandeventspb.CommandEventResponse{
		EventId: eventID,
		Success: false,
		Error: &commandeventspb.CommandError{
			Code:    code,
			Message: msg,
		},
	}
}
