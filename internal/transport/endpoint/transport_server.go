package endpoint

import (
	"context"
	"encoding/json"
	"log/slog"
	"pulsardb/internal/raft"
	raftevents "pulsardb/internal/raft/gen"

	"go.etcd.io/raft/v3/raftpb"
)

type RaftTransportServer struct {
	raftevents.UnimplementedRaftTransportServiceServer
	RaftNode *raft.Node
}

func NewRaftTransportServer(rn *raft.Node) *RaftTransportServer {
	return &RaftTransportServer{RaftNode: rn}
}

func (s *RaftTransportServer) SendRaftMessage(ctx context.Context, req *raftevents.RaftMessage) (*raftevents.RaftMessageResponse, error) {
	var m raftpb.Message
	if err := json.Unmarshal(req.GetData(), &m); err != nil {
		slog.Error("failed to unmarshal raft message", "error", err)
		return &raftevents.RaftMessageResponse{
			Ok: false,
		}, nil
	}

	raw := s.RaftNode.RawNode()
	if err := (*raw).Step(ctx, m); err != nil {
		slog.Error("failed to step raft message", "error", err)
		return &raftevents.RaftMessageResponse{
			Ok: false,
		}, nil
	}

	return &raftevents.RaftMessageResponse{
		Ok: true,
	}, nil
}
