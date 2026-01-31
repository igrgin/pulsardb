package handler

import (
	"context"
	"errors"
	"log/slog"
	internalerrors "pulsardb/internal/command"
	"pulsardb/internal/raft/coordinator"
	commandeventspb "pulsardb/internal/transport/gen/command"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CommandProcessor interface {
	ProcessCommand(ctx context.Context, req *commandeventspb.CommandEventRequest) (*commandeventspb.CommandEventResponse, error)
	GetLeaderInfo() (uint64, string)
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
		return nil, h.toGRPCError(err, req)
	}

	return resp, nil
}

func (h *CommandHandler) toGRPCError(err error, req *commandeventspb.CommandEventRequest) error {
	if errors.Is(err, coordinator.ErrNotLeader) {
		leaderID, leaderAddr := h.processor.GetLeaderInfo()

		st := status.New(codes.FailedPrecondition, "operation must be performed on the leader")
		ds, detailErr := st.WithDetails(&commandeventspb.NotLeaderDetails{
			LeaderId:   leaderID,
			LeaderAddr: leaderAddr,
		})

		if detailErr != nil {
			return st.Err()
		}
		return ds.Err()
	}

	if errors.Is(err, internalerrors.ErrKeyNotFound) {
		st := status.New(codes.NotFound, "key not found")
		ds, detailErr := st.WithDetails(&commandeventspb.KeyNotFoundDetails{
			Key:       req.GetKey(),
			Operation: req.GetType().String(),
		})

		if detailErr != nil {
			return st.Err()
		}
		return ds.Err()
	}

	if errors.Is(err, internalerrors.ErrInvalidCommand) {
		st := status.New(codes.InvalidArgument, "invalid command")
		ds, detailErr := st.WithDetails(&commandeventspb.InvalidCommandDetails{
			Reason: err.Error(),
		})

		if detailErr != nil {
			return st.Err()
		}
		return ds.Err()
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request timed out")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, coordinator.ErrShuttingDown):
		return status.Error(codes.Unavailable, "server is shutting down")
	default:
		return status.Errorf(codes.Internal, "internal error: %v", err)
	}
}
