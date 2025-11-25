package command

import (
	"fmt"
	"pulsardb/internal/storage"
	"pulsardb/internal/transport/gen"
)

type Handler interface {
	Handle(task *CmdTask) (*CmdTask, error)
}

type SetHandler struct {
	storageService *storage.Service
}

type GetHandler struct {
	storageService *storage.Service
}

type DeleteHandler struct {
	storageService *storage.Service
}

func NewSetHandler(s *storage.Service) *SetHandler {
	return &SetHandler{storageService: s}
}

func NewGetHandler(s *storage.Service) *GetHandler {
	return &GetHandler{storageService: s}
}

func NewDeleteHandler(s *storage.Service) *DeleteHandler {
	return &DeleteHandler{storageService: s}
}

// --- helpers ---

func valueFromProto(v *command_events.CommandEventValue) any {
	if v == nil {
		return nil
	}
	switch val := v.GetValue().(type) {
	case *command_events.CommandEventValue_StringValue:
		return val.StringValue
	case *command_events.CommandEventValue_IntValue:
		return val.IntValue
	case *command_events.CommandEventValue_DoubleValue:
		return val.DoubleValue
	case *command_events.CommandEventValue_BoolValue:
		return val.BoolValue
	case *command_events.CommandEventValue_BytesValue:
		return val.BytesValue
	default:
		return nil
	}
}

func valueToProto(a any) *command_events.CommandEventValue {
	switch v := a.(type) {
	case string:
		return &command_events.CommandEventValue{
			Value: &command_events.CommandEventValue_StringValue{StringValue: v},
		}
	case int:
		return &command_events.CommandEventValue{
			Value: &command_events.CommandEventValue_IntValue{IntValue: int64(v)},
		}
	case int64:
		return &command_events.CommandEventValue{
			Value: &command_events.CommandEventValue_IntValue{IntValue: v},
		}
	case float64:
		return &command_events.CommandEventValue{
			Value: &command_events.CommandEventValue_DoubleValue{DoubleValue: v},
		}
	case bool:
		return &command_events.CommandEventValue{
			Value: &command_events.CommandEventValue_BoolValue{BoolValue: v},
		}
	case []byte:
		return &command_events.CommandEventValue{
			Value: &command_events.CommandEventValue_BytesValue{BytesValue: v},
		}
	default:
		return nil
	}
}

// --- handlers ---

func (h *SetHandler) Handle(task *CmdTask) (*CmdTask, error) {
	req := task.event
	val := valueFromProto(req.GetCmdValue())

	h.storageService.Set(req.GetKey(), val)

	task.status = Completed
	return task, nil
}

func (h *GetHandler) Handle(task *CmdTask) (*CmdTask, error) {
	req := task.event
	stored, ok := h.storageService.Get(req.GetKey())
	if !ok {
		task.status = Failed
		return task, fmt.Errorf("key `%s` not found", req.GetKey())
	}

	req.CmdValue = valueToProto(stored)
	task.status = Completed
	return task, nil
}

func (h *DeleteHandler) Handle(task *CmdTask) (*CmdTask, error) {
	req := task.event

	h.storageService.Delete(req.GetKey())
	task.status = Completed
	return task, nil
}
