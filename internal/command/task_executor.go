package command

import (
	"fmt"
	"log/slog"
	"pulsardb/internal/transport/gen"
	"sync"
)

type TaskExecutor struct {
	commandService  *Service
	commandHandlers map[command_events.CommandEventType]Handler
	wg              *sync.WaitGroup
}

func NewTaskExecutor(cmdService *Service, commandHandlers map[command_events.CommandEventType]Handler) *TaskExecutor {
	return &TaskExecutor{
		commandService:  cmdService,
		commandHandlers: commandHandlers,
		wg:              &sync.WaitGroup{},
	}
}

func (te *TaskExecutor) Execute() {
	te.wg.Add(1)
	defer te.wg.Done()

	slog.Info("task executor started")
	for task := range te.commandService.CmdTaskQueue {
		task.status = Active
		slog.Debug("task Activated",
			"type", task.event.GetType().String(),
			"key", task.event.GetKey(),
		)

		handler, ok := te.commandHandlers[task.event.Type]
		if !ok {
			task.status = Failed
			resp := &command_events.CommandEventResponse{
				Type:         task.event.Type,
				Success:      false,
				ErrorMessage: "type missing handler",
			}
			slog.Error("missing handler for command type",
				"type", task.event.GetType().String(),
				"key", task.event.GetKey(),
			)
			task.responseChannel <- resp
			continue
		}

		handlerResponse, err := handler.Handle(&task)
		if err != nil && handlerResponse.status != Failed {
			slog.Error("handler error",
				"type", task.event.GetType().String(),
				"key", task.event.GetKey(),
				"error", err,
			)
			handlerResponse.status = Failed
		} else {
			slog.Debug("handler completed",
				"type", task.event.GetType().String(),
				"key", task.event.GetKey(),
				"status", handlerResponse.status.String(),
			)
		}

		response := toCmdTaskResponse(handlerResponse, err)
		task.responseChannel <- response
	}

	slog.Debug("task executor stopped")
}

// Stop closes the queue and waits until all queued tasks are processed.
func (te *TaskExecutor) Stop() {
	// Closing the channel signals "no more tasks"
	close(te.commandService.CmdTaskQueue)
	// Wait until Execute returns after draining the queue
	te.wg.Wait()
}

func toCmdTaskResponse(ct *CmdTask, err error) *command_events.CommandEventResponse {
	var success bool
	switch ct.status {
	case Completed:
		success = true
	case Failed, Pending, Active:
		success = false
	}

	errMsg := ""
	if err != nil {
		errMsg = fmt.Sprintf("%v", err)
	}

	return &command_events.CommandEventResponse{
		Type:         ct.event.Type,
		Success:      success,
		CmdValue:     ct.event.CmdValue,
		ErrorMessage: errMsg,
	}
}
