package queue

import (
	"fmt"
	"log/slog"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/core/util"
	"pulsardb/internal/transport/gen"
	"sync"
)

type CmdTask struct {
	event           *command_event.CommandEventRequest
	status          util.StorageEventStatus
	responseChannel chan *command_event.CommandEventResponse
}

type TaskQueue struct {
	Queue     chan CmdTask
	wg        sync.WaitGroup
	closeOnce sync.Once
	mu        sync.Mutex
	closed    bool
}

func CreateTaskQueue(commandConfig *properties.CommandConfigProperties) (*TaskQueue, error) {
	if commandConfig.QueueSize <= 0 {
		slog.Warn("Queue can't be smaller then 1. Setting queue size to 1.")
		commandConfig.QueueSize = 1
	}

	q := &TaskQueue{
		Queue:  make(chan CmdTask, commandConfig.QueueSize),
		mu:     sync.Mutex{},
		wg:     sync.WaitGroup{},
		closed: false,
	}

	slog.Info(fmt.Sprintf("Created DB queue with size: %d", commandConfig.QueueSize))

	return q, nil
}

func (q *TaskQueue) Start() {
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()

		for task := range q.Queue {
			task.status = util.Pending

			switch task.event.Type {
			case command_event.CommandEventType_SET:
				// handle SET
			case command_event.CommandEventType_GET:
				// handle GET
			case command_event.CommandEventType_DELETE:
				// handle DELETE
			}
		}
	}()
}

func (q *TaskQueue) Enqueue(ev *command_event.CommandEventRequest, respChan chan *command_event.CommandEventResponse) error {
	event := toCmdTask(ev, respChan)

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return fmt.Errorf("enqueue failed: queue closed")
	}

	select {
	case q.Queue <- event:
		return nil
	default:
		return fmt.Errorf("enqueue failed: queue is full")
	}
}

func toCmdTask(ev *command_event.CommandEventRequest, rc chan *command_event.CommandEventResponse) CmdTask {
	return CmdTask{event: ev, status: util.Pending, responseChannel: rc}
}

func (q *TaskQueue) Close() {
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		close(q.Queue)
	})
	q.wg.Wait()
}
