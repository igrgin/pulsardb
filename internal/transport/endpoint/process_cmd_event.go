package endpoint

import (
	"context"
	"log/slog"
	"pulsardb/internal/command"
	"pulsardb/internal/transport/gen"
)

type GRPCServer struct {
	command_events.UnimplementedCommandEventServiceServer
	*command.Service
}

func (grpcs *GRPCServer) ProcessCommandEvent(ctx context.Context, request *command_events.CommandEventRequest) (*command_events.CommandEventResponse, error) {
	slog.Debug("received gRPC command",
		"type", request.GetType().String(),
		"key", request.GetKey(),
	)

	responseChannel := make(chan *command_events.CommandEventResponse, 1)

	if err := grpcs.Service.Enqueue(request, responseChannel); err != nil {
		close(responseChannel)
		slog.Error("failed to enqueue command",
			"error", err.Error(),
			"type", request.GetType().String(),
			"key", request.GetKey(),
		)
		return &command_events.CommandEventResponse{
			Type:         request.GetType(),
			Success:      false,
			ErrorMessage: "Error occurred during processing of your command. Try Again.",
		}, nil
	}

	resp := <-responseChannel
	close(responseChannel)

	if resp.ErrorMessage == "" {
		slog.Debug("sending gRPC response",
			"type", resp.GetType().String(),
			"key", request.GetKey(),
			"success", resp.GetSuccess(),
		)
	} else {
		slog.Debug("sending gRPC response",
			"type", resp.GetType().String(),
			"key", request.GetKey(),
			"success", resp.GetSuccess(),
			"error_message", resp.GetErrorMessage(),
		)
	}

	return resp, nil
}
