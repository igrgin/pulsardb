package endpoint

import (
	"context"
	"fmt"
	"log/slog"
	"pulsardb/database/event_queue"
	"pulsardb/server/gen"
)

type DBEventServer struct {
	db_events.UnimplementedDBEventServiceServer
	DbQueue *event_queue.DBQueue
}

func (dbs *DBEventServer) EnqueueDBEvent(context context.Context, request *db_events.DBEventRequest) (*db_events.DBEventResponse, error) {

	responseChannel := make(chan *db_events.DBEventResponse, 1)
	err := dbs.DbQueue.Enqueue(request, responseChannel)
	if err != nil {
		close(responseChannel)
		slog.Error(fmt.Sprintf("Event failed to be added to queue %v", err.Error()))
		return &db_events.DBEventResponse{Success: false, ErrorMessage: "Error occurred during processing of your command. Try Again."}, nil
	}

	select {
	case resp := <-responseChannel:
		return resp, nil
	case <-context.Done():
		return &db_events.DBEventResponse{Success: false, ErrorMessage: "Error occurred during processing of your command. Try Again."}, context.Err()
	}

}
