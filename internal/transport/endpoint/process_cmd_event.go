package endpoint

import (
	"context"
	"errors"
	"log/slog"
	"pulsardb/internal/command"
	"pulsardb/internal/transport/gen/commandevents"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCServer struct {
	commandeventspb.UnsafeCommandEventClientServiceServer
	CommandService *command.Service
}

func (s *GRPCServer) ProcessCommandEvent(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (*commandeventspb.CommandEventResponse, error) {
	slog.Debug("received gRPC command", "type", req.GetType(), "key", req.GetKey())

	resp, err := s.CommandService.ProcessCommand(ctx, req)
	if err != nil {
		slog.Error("Database failed to process command",
			"error", err,
			"event_id", req.GetEventId(),
			"key", req.GetKey(),
		)

		grpcErr := s.resolveError(err)

		return nil, grpcErr
	}

	return resp, nil
}

func (s *GRPCServer) resolveError(err error) error {
	msg := "Internal error occurred"
	grpcErr := status.Error(codes.Internal, msg)

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		msg = "Request timed out"
		grpcErr = status.Error(codes.DeadlineExceeded, msg)
	case errors.Is(err, context.Canceled):
		msg = "Request was canceled"
		grpcErr = status.Error(codes.Canceled, msg)

	case errors.Is(err, command.ErrInvalidCommand):
		grpcErr = status.Errorf(codes.InvalidArgument, "Invalid command: %s", err)

	case errors.Is(err, command.ErrKeyNotFound):
		grpcErr = status.Errorf(codes.NotFound, "%s", err)
	}

	return grpcErr
}
