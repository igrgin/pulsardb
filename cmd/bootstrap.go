package main

import (
	"pulsardb/internal/command"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/raft"
	"pulsardb/internal/storage"
	"pulsardb/internal/transport/gen/commandevents"
	"time"
)

type Services struct {
	Storage *storage.Service
	Command *command.Service
	Raft    *raft.Service
	Batcher *raft.Batcher
}

func NewServices(
	raftNode *raft.Node,
	raftConfig *properties.RaftConfigProperties,
) (*Services, error) {
	storageSvc := storage.NewStorageService()

	cmdHolder := &commandHolder{}

	raftSvc := raft.NewService(raftNode, storageSvc, raftConfig, cmdHolder)

	batcher := raft.NewBatcher(
		raftSvc,
		cmdHolder,
		raftConfig.BatchSize,
		time.Duration(raftConfig.BatchMaxWait)*time.Millisecond,
	)

	cmdSvc := command.NewCommandService(storageSvc, raftSvc, batcher)

	cmdHolder.service = cmdSvc

	return &Services{
		Storage: storageSvc,
		Command: cmdSvc,
		Raft:    raftSvc,
		Batcher: batcher,
	}, nil
}

type commandHolder struct {
	service *command.Service
}

func (h *commandHolder) Apply(data []byte) ([]byte, error) {
	return h.service.Apply(data)
}

func (h *commandHolder) RegisterPending(eventID uint64, ch chan *commandeventspb.CommandEventResponse) {
	h.service.RegisterPending(eventID, ch)
}

func (h *commandHolder) UnregisterPending(eventID uint64) {
	h.service.UnregisterPending(eventID)
}
