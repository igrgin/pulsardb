package endpoint

import (
	"context"
	"log/slog"
	"pulsardb/internal/raft"
	commandevents "pulsardb/internal/transport/gen"
)

type GRPCServer struct {
	commandevents.UnimplementedCommandEventServiceServer
	RaftNode *raft.Node
}

func (grpcs *GRPCServer) ProcessCommandEvent(
	ctx context.Context,
	request *commandevents.CommandEventRequest,
) (*commandevents.CommandEventResponse, error) {
	slog.Debug("received gRPC command",
		"type", request.GetType().String(),
		"key", request.GetKey(),
	)

	resp, err := grpcs.RaftNode.ProcessCommand(ctx, request)
	if err != nil {
		slog.Error("raft node failed to process command",
			"error", err,
			"type", request.GetType().String(),
			"key", request.GetKey(),
		)
		return &commandevents.CommandEventResponse{
			Type:         request.GetType(),
			Success:      false,
			ErrorMessage: "Error occurred during consensus. Try Again.",
		}, nil
	}

	return resp, nil
}
