package handler

import (
	"context"
	"errors"
	"log/slog"
	"pulsardb/internal/raft"

	rafttransportpb "pulsardb/internal/transport/gen/raft"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RaftHandler interface {
	Step(ctx context.Context, msg raftpb.Message) error
	GetReadIndex(ctx context.Context) (uint64, error)
}

type RaftTransportHandler struct {
	rafttransportpb.UnimplementedRaftTransportServiceServer
	handler RaftHandler
}

func NewRaftTransportHandler(h RaftHandler) *RaftTransportHandler {
	return &RaftTransportHandler{handler: h}
}

func (h *RaftTransportHandler) SendRaftMessage(
	ctx context.Context,
	req *rafttransportpb.RaftMessage,
) (*rafttransportpb.RaftMessageResponse, error) {
	var msg raftpb.Message
	if err := msg.Unmarshal(req.GetData()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal: %v", err)
	}

	if err := h.handler.Step(ctx, msg); err != nil {
		return nil, raftError(err)
	}

	return &rafttransportpb.RaftMessageResponse{Ok: true}, nil
}

func (h *RaftTransportHandler) GetReadIndex(
	ctx context.Context,
	req *rafttransportpb.GetReadIndexRequest,
) (*rafttransportpb.GetReadIndexResponse, error) {
	slog.Debug("read index request", "from", req.GetFromNode())

	idx, err := h.handler.GetReadIndex(ctx)
	if err != nil {
		return nil, raftError(err)
	}

	return &rafttransportpb.GetReadIndexResponse{ReadIndex: idx}, nil
}

func raftError(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return status.FromContextError(err).Err()
	case errors.Is(err, raft.ErrNotLeader):
		return status.Error(codes.FailedPrecondition, "not leader")
	default:
		return status.Errorf(codes.Internal, "raft: %v", err)
	}
}
