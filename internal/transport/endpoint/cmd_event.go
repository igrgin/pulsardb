package endpoint

import (
	"context"
	"log/slog"
	"pulsardb/internal/core/queue"
	"pulsardb/internal/transport/gen"
)

type GRPCServer struct {
	command_event.UnimplementedCommandEventServiceServer
	TaskQueue *queue.TaskQueue
}

func (dbs *GRPCServer) ProcessCommandEvent(context context.Context, request *command_event.CommandEventRequest) (*command_event.CommandEventResponse, error) {

	responseChannel := make(chan *command_event.CommandEventResponse, 1)
	err := dbs.TaskQueue.Enqueue(request, responseChannel)
	if err != nil {
		close(responseChannel)
		slog.Error("Event failed to be added to queue", "Error", err.Error())
		return &command_event.CommandEventResponse{Success: false, ErrorMessage: "Error occurred during processing of your core. Try Again."}, nil
	}

	select {
	case resp := <-responseChannel:
		return resp, nil
	case <-context.Done():
		return &command_event.CommandEventResponse{Success: false, ErrorMessage: "Error occurred during processing of your core. Try Again."}, context.Err()
	}

}
