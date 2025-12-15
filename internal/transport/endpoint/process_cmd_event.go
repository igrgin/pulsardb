package endpoint

import (
	"context"
	"errors"
	"log/slog"
	"pulsardb/internal/command"
	"pulsardb/internal/transport/gen/commandevents"
	"pulsardb/internal/transport/util"
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
	slog.Debug("response", "resp", resp)
	if err != nil {
		slog.Error("Database failed to process command",
			"error", err,
			"event_id", req.GetEventId(),
			"key", req.GetKey(),
		)

		code := commandeventspb.ErrorCode_UNKNOWN
		if errors.Is(err, context.DeadlineExceeded) {
			code = commandeventspb.ErrorCode_TIMEOUT
		}

		return transport.ErrorResponse(req.EventId, code, "Error occurred during consensus. Try Again."), nil
	}

	slog.Debug("ProcessCommandEvent response", "Response", resp)
	return resp, nil
}
