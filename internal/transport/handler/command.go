package handler

import (
	"context"
	"errors"
	"log/slog"

	"pulsardb/internal/command"
	"pulsardb/internal/transport/gen/commandevents"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CommandProcessor interface {
	ProcessCommand(ctx context.Context, req *commandeventspb.CommandEventRequest) (*commandeventspb.CommandEventResponse, error)
}

type CommandHandler struct {
	commandeventspb.UnimplementedCommandEventClientServiceServer
	processor CommandProcessor
}

func NewCommandHandler(p CommandProcessor) *CommandHandler {
	return &CommandHandler{processor: p}
}

func (h *CommandHandler) ProcessCommandEvent(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (*commandeventspb.CommandEventResponse, error) {
	slog.Debug("received command", "type", req.GetType(), "key", req.GetKey())

	resp, err := h.processor.ProcessCommand(ctx, req)
	if err != nil {
		slog.Error("command failed",
			"error", err,
			"event_id", req.GetEventId(),
			"key", req.GetKey(),
		)
		return nil, ToGRPCError(err)
	}

	return resp, nil
}

func ToGRPCError(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request timed out")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, command.ErrInvalidCommand):
		return status.Errorf(codes.InvalidArgument, "invalid command: %v", err)
	case errors.Is(err, command.ErrKeyNotFound):
		return status.Errorf(codes.NotFound, "%v", err)
	case errors.Is(err, command.ErrNotLeader), errors.Is(err, command.ErrNoLeader):
		return status.Error(codes.Unavailable, "not leader")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
