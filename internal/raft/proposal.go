package raft

import (
	"context"
	"fmt"
	"log/slog"

	commandevents "pulsardb/internal/transport/gen"

	etcdraft "go.etcd.io/raft/v3"
	"google.golang.org/protobuf/proto"
)

// ProcessCommand handles incoming commands, routing to leader or processing locally.
func (n *Node) ProcessCommand(ctx context.Context, req *commandevents.CommandEventRequest) (*commandevents.CommandEventResponse, error) {
	status := n.raftNode.Status()

	// Follower: forward to leader
	if status.RaftState != etcdraft.StateLeader {
		return n.forwardToLeader(ctx, req, status.Lead)
	}

	// Leader: handle based on command type
	switch req.GetType() {
	case commandevents.CommandEventType_GET:
		return n.LinearizableGet(ctx, req.GetKey())
	case commandevents.CommandEventType_SET, commandevents.CommandEventType_DELETE:
		return n.ProposeCommand(ctx, req)
	default:
		return &commandevents.CommandEventResponse{
			EventId:      req.EventId,
			Type:         req.GetType(),
			Success:      false,
			ErrorMessage: "unknown command type",
		}, nil
	}
}

// ProposeCommand proposes a write command through raft consensus.
func (n *Node) ProposeCommand(ctx context.Context, req *commandevents.CommandEventRequest) (*commandevents.CommandEventResponse, error) {
	status := n.raftNode.Status()

	// Non-leader: forward to leader
	if status.RaftState != etcdraft.StateLeader {
		return n.forwardToLeader(ctx, req, status.Lead)
	}

	// Assign event ID if not set
	if req.EventId == 0 {
		req.EventId = n.nextRequestID()
	}

	slog.Debug("proposing command",
		"node_id", n.Id,
		"type", req.GetType().String(),
		"key", req.GetKey(),
		"event_id", req.EventId,
	)

	// Register response channel
	respCh := make(chan *commandevents.CommandEventResponse, 1)
	n.registerPending(req.EventId, respCh)
	defer n.unregisterPending(req.EventId)

	// Marshal and propose
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}

	if err := n.raftNode.Propose(ctx, data); err != nil {
		return nil, fmt.Errorf("raft propose: %w", err)
	}

	// Wait for response
	select {
	case resp := <-respCh:
		return resp, nil
	case <-ctx.Done():
		slog.Warn("propose timed out",
			"node_id", n.Id,
			"event_id", req.EventId,
		)
		return nil, ctx.Err()
	}
}

// forwardToLeader forwards a command to the current leader.
func (n *Node) forwardToLeader(ctx context.Context, req *commandevents.CommandEventRequest, leaderID uint64) (*commandevents.CommandEventResponse, error) {
	if leaderID == 0 {
		slog.Warn("no known leader",
			"node_id", n.Id,
			"type", req.GetType().String(),
		)
		return nil, fmt.Errorf("no known leader")
	}

	client, ok := n.getLeaderClient(leaderID)
	if !ok {
		return nil, fmt.Errorf("no client for leader %d", leaderID)
	}

	slog.Debug("forwarding to leader",
		"node_id", n.Id,
		"leader_id", leaderID,
		"type", req.GetType().String(),
	)

	resp, err := client.ProcessCommandEvent(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("forward to leader %d: %w", leaderID, err)
	}

	return resp, nil
}

// registerPending registers a channel to receive a proposal response.
func (n *Node) registerPending(eventID uint64, ch chan *commandevents.CommandEventResponse) {
	n.mu.Lock()
	n.pending[eventID] = ch
	n.mu.Unlock()
}

// unregisterPending removes a pending response channel.
func (n *Node) unregisterPending(eventID uint64) {
	n.mu.Lock()
	delete(n.pending, eventID)
	n.mu.Unlock()
}
