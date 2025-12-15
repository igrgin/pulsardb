package endpoint

import (
	"context"
	"errors"
	"log/slog"
	"pulsardb/internal/raft"
	rafttransportpb "pulsardb/internal/transport/gen/raft"

	etcdraftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RaftTransportServer struct {
	rafttransportpb.UnimplementedRaftTransportServiceServer
	raftService *raft.Service
}

func NewRaftTransportServer(rs *raft.Service) *RaftTransportServer {
	return &RaftTransportServer{raftService: rs}
}

func (s *RaftTransportServer) SendRaftMessage(ctx context.Context, req *rafttransportpb.RaftMessage) (*rafttransportpb.RaftMessageResponse, error) {
	var m etcdraftpb.Message
	if err := m.Unmarshal(req.GetData()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal raft message: %v", err)
	}

	if err := s.raftService.CallRaftStep(ctx, m); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Errorf(codes.DeadlineExceeded, "raft step: %v", err)
		}
		return nil, status.Errorf(codes.FailedPrecondition, "raft step: %v", err)
	}

	return &rafttransportpb.RaftMessageResponse{Ok: true}, nil
}

func (s *RaftTransportServer) GetReadIndex(
	ctx context.Context,
	req *rafttransportpb.GetReadIndexRequest,
) (*rafttransportpb.GetReadIndexResponse, error) {

	slog.Debug("Received ReadIndex request", "NodeSource", req.FromNode)
	idx, err := s.raftService.GetReadIndex(ctx)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return nil, status.Error(codes.FailedPrecondition, "not leader")
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.Internal, "GetReadIndex failed: %v", err)
	}

	return &rafttransportpb.GetReadIndexResponse{ReadIndex: idx}, nil
}
