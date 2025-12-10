package raft

import (
	"context"
	"fmt"
	"log/slog"

	commandevents "pulsardb/internal/transport/gen"

	etcdraft "go.etcd.io/raft/v3"
)

// LinearizableGet performs a linearizable read using ReadIndex.
func (n *Node) LinearizableGet(ctx context.Context, key string) (*commandevents.CommandEventResponse, error) {
	status := n.raftNode.Status()
	if status.RaftState != etcdraft.StateLeader {
		return nil, fmt.Errorf("not leader")
	}

	// Issue ReadIndex with unique context
	reqID := n.nextRequestID()
	reqCtx := []byte(fmt.Sprintf("read-%d", reqID))
	reqCtxKey := string(reqCtx)

	// Register waiter
	ch := make(chan uint64, 1)
	n.registerReadWaiter(reqCtxKey, ch)

	slog.Debug("issuing ReadIndex",
		"node_id", n.Id,
		"key", key,
		"req_id", reqID,
	)

	if err := n.raftNode.ReadIndex(ctx, reqCtx); err != nil {
		n.unregisterReadWaiter(reqCtxKey)
		return nil, fmt.Errorf("ReadIndex: %w", err)
	}

	// Wait for read index to be applied locally
	select {
	case <-ch:
		// Safe to read
	case <-ctx.Done():
		n.unregisterReadWaiter(reqCtxKey)
		return nil, ctx.Err()
	}

	// Execute GET through command service
	return n.executeGet(ctx, key)
}

// executeGet executes a GET command through the command service.
func (n *Node) executeGet(ctx context.Context, key string) (*commandevents.CommandEventResponse, error) {
	req := &commandevents.CommandEventRequest{
		EventId: n.nextRequestID(),
		Type:    commandevents.CommandEventType_GET,
		Key:     key,
	}

	respCh := make(chan *commandevents.CommandEventResponse, 1)
	if err := n.commandSvc.Enqueue(req, respCh); err != nil {
		return nil, fmt.Errorf("enqueue GET: %w", err)
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// handleReadStates processes ReadIndex responses.
func (n *Node) handleReadStates(readStates []etcdraft.ReadState) {
	if len(readStates) == 0 {
		return
	}

	n.readMu.Lock()
	defer n.readMu.Unlock()

	for _, rs := range readStates {
		ctxKey := string(rs.RequestCtx)
		ch, ok := n.readWaiters[ctxKey]
		if !ok {
			continue
		}

		// Check if local state has caught up
		if n.lastApplied >= rs.Index {
			select {
			case ch <- rs.Index:
				slog.Debug("completed ReadIndex waiter",
					"node_id", n.Id,
					"read_index", rs.Index,
				)
			default:
			}
			delete(n.readWaiters, ctxKey)
		} else {
			slog.Debug("read not yet applied locally",
				"node_id", n.Id,
				"read_index", rs.Index,
				"last_applied", n.lastApplied,
			)
		}
	}
}

// registerReadWaiter registers a channel to wait for a read index.
func (n *Node) registerReadWaiter(key string, ch chan uint64) {
	n.readMu.Lock()
	defer n.readMu.Unlock()

	if n.readWaiters == nil {
		n.readWaiters = make(map[string]chan uint64)
	}
	n.readWaiters[key] = ch
}

// unregisterReadWaiter removes a read waiter.
func (n *Node) unregisterReadWaiter(key string) {
	n.readMu.Lock()
	defer n.readMu.Unlock()
	delete(n.readWaiters, key)
}
