package command

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/transport/gen"
)

type CmdTask struct {
	taskId          uint64
	event           *command_events.CommandEventRequest
	status          TaskStatus
	responseChannel chan *command_events.CommandEventResponse
}

type Service struct {
	CmdTaskQueue chan CmdTask
}

func NewCommandService(commandConfig *properties.CommandConfigProperties) *Service {
	if commandConfig.QueueSize <= 0 {
		slog.Warn("Queue can't be smaller then 1. Setting queue size to 1.")
		commandConfig.QueueSize = 1
	}

	s := &Service{
		CmdTaskQueue: make(chan CmdTask, commandConfig.QueueSize),
	}

	slog.Info(fmt.Sprintf("Created DB queue with size: %d", commandConfig.QueueSize))

	return s
}

func (s *Service) Enqueue(ev *command_events.CommandEventRequest, respChan chan *command_events.CommandEventResponse) error {
	task := toCmdTask(ev, respChan)
	select {
	case s.CmdTaskQueue <- task:
		slog.Debug("enqueued command task",
			"task_id", task.taskId,
			"type", ev.GetType().String(),
			"key", ev.GetKey(),
			"event_id", ev.EventId,
		)
		return nil
	default:
		slog.Error("command queue is full",
			"type", ev.GetType().String(),
			"key", ev.GetKey(),
			"event_id", ev.EventId,
		)
		return fmt.Errorf("command queue is full")
	}
}

func toCmdTask(ev *command_events.CommandEventRequest, rc chan *command_events.CommandEventResponse) CmdTask {
	return CmdTask{event: ev, status: Pending, responseChannel: rc, taskId: rand.Uint64()}
}
