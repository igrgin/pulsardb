package event_queue

import (
	"fmt"
	"log/slog"
	"pulsardb/database/util"
	db_events "pulsardb/server/gen"
	"sync"
)

type DBTask struct {
	event           *db_events.DBEventRequest
	status          util.DBEventStatus
	responseChannel chan *db_events.DBEventResponse
}

type DBQueue struct {
	Queue     chan DBTask
	wg        sync.WaitGroup
	closeOnce sync.Once
	mu        sync.Mutex
	closed    bool
}

func CreateDBQueue(queueSize int) (*DBQueue, error) {
	if queueSize <= 0 {
		slog.Warn("Queue can't be smaller then 1. Setting queue size to 1.")
		queueSize = 1
	}

	q := &DBQueue{
		Queue:  make(chan DBTask, queueSize),
		mu:     sync.Mutex{},
		wg:     sync.WaitGroup{},
		closed: false,
	}

	slog.Info(fmt.Sprintf("Created DB queue with size: %d", queueSize))

	return q, nil
}

func (q *DBQueue) Start() {
	q.wg.Add(1)
	for task := range q.Queue {
		task.status = util.Pending
		go func() {
			defer q.wg.Done()
			for ev := range q.Queue {
				switch ev.event.Type {
				case db_events.DBEventType_SET:
				case db_events.DBEventType_GET:
				case db_events.DBEventType_DELETE:
				}
			}
		}()
	}
}

func (q *DBQueue) Enqueue(ev *db_events.DBEventRequest, respChan chan *db_events.DBEventResponse) error {
	event := toDBTask(ev, respChan)

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

func toDBTask(ev *db_events.DBEventRequest, rc chan *db_events.DBEventResponse) DBTask {
	return DBTask{event: ev, status: util.Pending, responseChannel: rc}
}

func (q *DBQueue) Close() {
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		close(q.Queue)
	})
	q.wg.Wait()
}
