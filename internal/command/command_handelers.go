package command

import (
	"fmt"
	"pulsardb/internal/storage"
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

func (h *SetHandler) Handle(task *CmdTask) (*CmdTask, error) {
	req := task.event
	val := ValueFromProto(req.GetCmdValue()) // Use exported function

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

	req.CmdValue = ValueToProto(stored) // Use exported function
	task.status = Completed
	return task, nil
}

func (h *DeleteHandler) Handle(task *CmdTask) (*CmdTask, error) {
	req := task.event

	h.storageService.Delete(req.GetKey())
	task.status = Completed
	return task, nil
}
