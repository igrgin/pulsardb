package raft

import (
	"log/slog"

	commandevents "pulsardb/internal/transport/gen"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// applyCommitted processes committed raft entries.
func (n *Node) applyCommitted(entries []raftpb.Entry) {
	for _, entry := range entries {
		switch entry.Type {
		case raftpb.EntryConfChange:
			n.applyConfChange(entry)
		case raftpb.EntryNormal:
			n.applyNormalEntry(entry)
		default:
			slog.Warn("ignoring unsupported raft entry type",
				"node_id", n.Id,
				"index", entry.Index,
				"term", entry.Term,
				"type", entry.Type.String(),
			)
		}

		n.updateLastApplied(entry.Index)
	}
}

// applyConfChange handles cluster membership changes.
func (n *Node) applyConfChange(entry raftpb.Entry) {
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		slog.Error("failed to unmarshal conf change",
			"node_id", n.Id,
			"index", entry.Index,
			"error", err,
		)
		return
	}

	slog.Info("applying conf change",
		"node_id", n.Id,
		"index", entry.Index,
		"term", entry.Term,
		"type", cc.Type.String(),
		"target_node", cc.NodeID,
	)

	// Apply to raft and get new confState
	confState := n.raftNode.ApplyConfChange(cc)
	n.confState = *confState

	// IMPORTANT:  Persist the confState to storage!
	if err := n.storage.SaveConfState(*confState); err != nil {
		slog.Error("failed to persist confState",
			"node_id", n.Id,
			"error", err,
		)
	}

	slog.Info("confState updated",
		"node_id", n.Id,
		"voters", confState.Voters,
		"learners", confState.Learners,
	)

	// Update peer tracking
	n.updatePeersFromConfChange(cc)
}

// updatePeersFromConfChange updates local peer tracking after a conf change.
func (n *Node) updatePeersFromConfChange(cc raftpb.ConfChange) {
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if len(cc.Context) > 0 && cc.NodeID != n.Id {
			addr := string(cc.Context)
			n.mu.Lock()
			n.peers[cc.NodeID] = addr
			n.mu.Unlock()

			if err := n.initPeerClient(cc.NodeID, addr); err != nil {
				slog.Error("failed to init client for new peer",
					"peer_id", cc.NodeID,
					"error", err,
				)
			}
		}

	case raftpb.ConfChangeRemoveNode:
		if cc.NodeID != n.Id {
			n.mu.Lock()
			delete(n.peers, cc.NodeID)
			delete(n.clients, cc.NodeID)
			n.mu.Unlock()
			slog.Info("removed peer", "peer_id", cc.NodeID)
		}
	}
}

// applyNormalEntry handles regular log entries (commands).
func (n *Node) applyNormalEntry(entry raftpb.Entry) {
	if len(entry.Data) == 0 {
		return
	}

	var req commandevents.CommandEventRequest
	if err := proto.Unmarshal(entry.Data, &req); err != nil {
		slog.Error("failed to unmarshal command",
			"node_id", n.Id,
			"index", entry.Index,
			"error", err,
		)
		return
	}

	n.executeCommand(&req, entry.Index, entry.Term)
}

// executeCommand runs a command through the command service asynchronously.
func (n *Node) executeCommand(req *commandevents.CommandEventRequest, index, term uint64) {
	slog.Debug("executing committed command",
		"node_id", n.Id,
		"index", index,
		"term", term,
		"type", req.GetType().String(),
		"key", req.GetKey(),
		"event_id", req.EventId,
	)

	go func() {
		respCh := make(chan *commandevents.CommandEventResponse, 1)
		if err := n.commandSvc.Enqueue(req, respCh); err != nil {
			slog.Error("failed to enqueue command",
				"node_id", n.Id,
				"event_id", req.EventId,
				"error", err,
			)
			return
		}

		resp := <-respCh
		if resp.EventId == 0 {
			resp.EventId = req.EventId
		}

		n.notifyWaiter(resp)
	}()
}

// notifyWaiter sends a response to any waiting proposer.
func (n *Node) notifyWaiter(resp *commandevents.CommandEventResponse) {
	n.mu.RLock()
	waiterCh, ok := n.pending[resp.EventId]
	n.mu.RUnlock()

	if !ok {
		slog.Debug("no waiter for response", "event_id", resp.EventId)
		return
	}

	select {
	case waiterCh <- resp:
		slog.Debug("delivered response to waiter", "event_id", resp.EventId)
	default:
		slog.Debug("waiter channel full", "event_id", resp.EventId)
	}
}

// updateLastApplied updates the last applied index.
func (n *Node) updateLastApplied(index uint64) {
	if index > n.lastApplied {
		n.lastApplied = index
	}
}
