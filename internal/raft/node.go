package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"pulsardb/internal/command"
	"pulsardb/internal/configuration/properties"
	raftevents "pulsardb/internal/raft/gen"
	"pulsardb/internal/storage"
	commandevents "pulsardb/internal/transport/gen"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type Node struct {
	Id           uint64
	Node         etcdraft.Node
	storage      *Storage
	storeService *storage.Service
	commandSvc   *command.Service
	peers        map[uint64]string
	mu           sync.RWMutex
	localAddress string
	clients      map[uint64]Client
	pending      map[uint64]chan *commandevents.CommandEventResponse
	isLeader     bool
	leaderID     uint64

	stopCh    chan struct{}
	stoppedWg sync.WaitGroup

	nextReqID uint64

	readMu      sync.Mutex
	readWaiters map[string]chan uint64

	lastApplied uint64
}

func NewRaftNode(rc *properties.RaftConfigProperties, store *storage.Service, commandSvc *command.Service, localAddr string) (*Node, error) {
	raftStore, lastIndex, err := OpenStorage(rc.StorageBaseDir)
	if err != nil {
		return nil, fmt.Errorf("failure to create raft storage with error: %v", err)
	}
	slog.Debug("Created raft storage")

	c := &etcdraft.Config{
		ID:              rc.NodeId,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         raftStore.RaftStorage(),
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
		Logger:          NewSlogRaftLogger(),
		Applied:         lastIndex,
	}

	peersList := buildPeers(rc.NodeId, localAddr, rc.Peers)

	rawNode, err := StartRestartNode(c, raftStore, peersList)
	if err != nil {
		return nil, fmt.Errorf("failed to start Node with error: %v", err)
	}
	slog.Info("Raft Node started", "Id", rc.NodeId)

	rn := &Node{
		Id:           rc.NodeId,
		Node:         rawNode,
		storage:      raftStore,
		storeService: store,
		commandSvc:   commandSvc,
		peers:        rc.Peers,
		clients:      make(map[uint64]Client),
		pending:      make(map[uint64]chan *commandevents.CommandEventResponse),
		stopCh:       make(chan struct{}),
		localAddress: localAddr,
		readWaiters:  make(map[string]chan uint64),
		lastApplied:  lastIndex,
	}

	if err := rn.initPeerClients(); err != nil {
		return nil, err
	}

	return rn, nil
}

func (n *Node) handleReadStates(readStates []etcdraft.ReadState, commitIndex uint64) {
	if len(readStates) == 0 {
		return
	}

	n.readMu.Lock()
	defer n.readMu.Unlock()

	for _, rs := range readStates {
		ch, ok := n.readWaiters[string(rs.RequestCtx)]
		if !ok {
			slog.Debug("no read waiter for context",
				"node_id", n.Id,
				"ctx", string(rs.RequestCtx),
			)
			continue
		}
		// ensure local commitIndex >= rs.Index before signaling
		if n.lastApplied >= rs.Index {
			select {
			case ch <- rs.Index:
				slog.Debug("completed ReadIndex waiter",
					"node_id", n.Id,
					"ctx", string(rs.RequestCtx),
					"read_index", rs.Index,
				)
			default:
				slog.Debug("read waiter channel full",
					"node_id", n.Id,
					"ctx", string(rs.RequestCtx),
				)
			}
			delete(n.readWaiters, string(rs.RequestCtx))
		} else {
			slog.Debug("read not yet applied locally",
				"node_id", n.Id,
				"ctx", string(rs.RequestCtx),
				"read_index", rs.Index,
				"last_applied", n.lastApplied,
			)
		}
	}
}

func (n *Node) ProposeCommand(ctx context.Context, req *commandevents.CommandEventRequest) (*commandevents.CommandEventResponse, error) {
	status := n.Node.Status()

	if status.RaftState != etcdraft.StateLeader {
		leaderID := status.Lead
		if leaderID == 0 {
			slog.Warn("propose rejected: no known leader",
				"node_id", n.Id,
				"type", req.GetType().String(),
				"key", req.GetKey(),
				"event_id", req.EventId,
			)
			return nil, fmt.Errorf("no known leader")
		}

		slog.Debug("forwarding command to leader",
			"node_id", n.Id,
			"leader_id", leaderID,
			"type", req.GetType().String(),
			"key", req.GetKey(),
			"event_id", req.EventId,
		)

		n.mu.RLock()
		client, ok := n.clients[leaderID]
		n.mu.RUnlock()
		if !ok {
			slog.Error("no gRPC client for leader",
				"node_id", n.Id,
				"leader_id", leaderID,
			)
			return nil, fmt.Errorf("no client for leader %d", leaderID)
		}

		leaderResp, err := client.ProcessCommandEvent(ctx, req)
		if err != nil {
			slog.Error("forward to leader failed",
				"node_id", n.Id,
				"leader_id", leaderID,
				"type", req.GetType().String(),
				"key", req.GetKey(),
				"event_id", req.EventId,
				"error", err,
			)
			return nil, fmt.Errorf("forward to leader %d failed: %w", leaderID, err)
		}

		slog.Debug("forwarded command completed",
			"node_id", n.Id,
			"leader_id", leaderID,
			"type", req.GetType().String(),
			"key", req.GetKey(),
			"event_id", leaderResp.EventId,
			"success", leaderResp.Success,
		)
		return leaderResp, nil
	}

	// leader path
	if req.EventId == 0 {
		req.EventId = atomic.AddUint64(&n.nextReqID, 1)
	}

	slog.Debug("proposing command",
		"node_id", n.Id,
		"type", req.GetType().String(),
		"key", req.GetKey(),
		"event_id", req.EventId,
	)

	respCh := make(chan *commandevents.CommandEventResponse, 1)

	n.mu.Lock()
	n.pending[req.EventId] = respCh
	n.mu.Unlock()
	defer func(id uint64) {
		n.mu.Lock()
		delete(n.pending, id)
		n.mu.Unlock()
	}(req.EventId)

	data, err := proto.Marshal(req)
	if err != nil {
		slog.Error("failed to marshal command for raft",
			"node_id", n.Id,
			"type", req.GetType().String(),
			"key", req.GetKey(),
			"event_id", req.EventId,
			"error", err,
		)
		return nil, fmt.Errorf("marshal command: %w", err)
	}

	if err := n.Node.Propose(ctx, data); err != nil {
		slog.Error("raft propose failed",
			"node_id", n.Id,
			"type", req.GetType().String(),
			"key", req.GetKey(),
			"event_id", req.EventId,
			"error", err,
		)
		return nil, fmt.Errorf("raft propose: %w", err)
	}

	select {
	case resp := <-respCh:
		slog.Debug("leader received command result",
			"node_id", n.Id,
			"type", resp.GetType().String(),
			"key", req.GetKey(),
			"event_id", resp.EventId,
			"success", resp.Success,
		)
		return resp, nil
	case <-ctx.Done():
		slog.Warn("propose command timed out",
			"node_id", n.Id,
			"type", req.GetType().String(),
			"key", req.GetKey(),
			"event_id", req.EventId,
			"error", ctx.Err(),
		)
		return nil, ctx.Err()
	}
}

// LinearizableGet:
// \- ensures leader & linearizability via ReadIndex
// \- then enqueues a synthetic GET CommandEventRequest into commandSvc
// \- waits for the handler response and returns it
func (n *Node) LinearizableGet(ctx context.Context, key string) (*commandevents.CommandEventResponse, error) {
	status := n.Node.Status()
	if status.RaftState != etcdraft.StateLeader {
		return nil, fmt.Errorf("not leader")
	}

	// 1) Issue ReadIndex with a unique context
	reqID := atomic.AddUint64(&n.nextReqID, 1)
	reqCtx := []byte(fmt.Sprintf("read-%d", reqID))
	reqCtxKey := string(reqCtx)

	ch := make(chan uint64, 1)

	n.readMu.Lock()
	if n.readWaiters == nil {
		n.readWaiters = make(map[string]chan uint64)
	}
	n.readWaiters[reqCtxKey] = ch
	n.readMu.Unlock()

	slog.Debug("issuing ReadIndex",
		"node_id", n.Id,
		"key", key,
		"req_id", reqID,
	)

	if err := n.Node.ReadIndex(ctx, reqCtx); err != nil {
		slog.Error("ReadIndex failed",
			"node_id", n.Id,
			"key", key,
			"req_id", reqID,
			"error", err,
		)
		// cleanup waiter
		n.readMu.Lock()
		delete(n.readWaiters, reqCtxKey)
		n.readMu.Unlock()
		return nil, fmt.Errorf("ReadIndex: %w", err)
	}

	// 2) Wait until local commit index has reached the read index
	select {
	case <-ch:
		// safe to read
	case <-ctx.Done():
		// cleanup waiter on timeout/cancel
		n.readMu.Lock()
		delete(n.readWaiters, reqCtxKey)
		n.readMu.Unlock()
		return nil, ctx.Err()
	}

	// 3) Build synthetic GET command and enqueue through command queue
	ev := &commandevents.CommandEventRequest{
		EventId: atomic.AddUint64(&n.nextReqID, 1),
		Type:    commandevents.CommandEventType_GET,
		Key:     key,
	}

	respCh := make(chan *commandevents.CommandEventResponse, 1)
	if err := n.commandSvc.Enqueue(ev, respCh); err != nil {
		slog.Error("failed to enqueue linearizable GET",
			"node_id", n.Id,
			"key", key,
			"event_id", ev.EventId,
			"error", err,
		)
		return nil, fmt.Errorf("enqueue GET: %w", err)
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ProcessCommand centralizes leader/follower logic and GET fast path.
func (n *Node) ProcessCommand(ctx context.Context, req *commandevents.CommandEventRequest) (*commandevents.CommandEventResponse, error) {
	status := n.Node.Status()

	// follower: forward everything to leader
	if status.RaftState != etcdraft.StateLeader {
		leaderID := status.Lead
		if leaderID == 0 {
			slog.Warn("process rejected: no known leader",
				"node_id", n.Id,
				"type", req.GetType().String(),
				"key", req.GetKey(),
				"event_id", req.EventId,
			)
			return nil, fmt.Errorf("no known leader")
		}

		n.mu.RLock()
		client, ok := n.clients[leaderID]
		n.mu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("no client for leader %d", leaderID)
		}
		return client.ProcessCommandEvent(ctx, req)
	}

	// leader path
	switch req.GetType() {
	case commandevents.CommandEventType_GET:
		return n.LinearizableGet(ctx, req.GetKey())
	case commandevents.CommandEventType_SET,
		commandevents.CommandEventType_DELETE:
		// normal write path via raft log
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

// applyCommitted: decode entries and push them into command queue.
func (n *Node) applyCommitted(ents []raftpb.Entry) {
	for _, e := range ents {
		switch e.Type {
		case raftpb.EntryConfChange:
			// 1) Handle configuration changes
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(e.Data); err != nil {
				slog.Error("failed to unmarshal conf change",
					"node_id", n.Id,
					"index", e.Index,
					"term", e.Term,
					"error", err,
				)
				if e.Index > n.lastApplied {
					n.lastApplied = e.Index
				}
				continue
			}

			slog.Info("applying conf change",
				"node_id", n.Id,
				"index", e.Index,
				"term", e.Term,
				"type", cc.Type.String(),
				"node_id_change", cc.NodeID,
			)

			// Update raft's internal config state.
			n.Node.ApplyConfChange(cc)

			// \[Optional\]: if you keep your own `n.peers` map in sync with membership,
			// update it here based on cc.Type and cc.NodeId.

			if e.Index > n.lastApplied {
				n.lastApplied = e.Index
			}

		case raftpb.EntryNormal:
			// 2) Handle normal log entries (commands)
			if len(e.Data) == 0 {
				// Empty entries are common (heartbeat, leader confirmation, etc.).
				if e.Index > n.lastApplied {
					n.lastApplied = e.Index
				}
				continue
			}

			var req commandevents.CommandEventRequest
			if err := proto.Unmarshal(e.Data, &req); err != nil {
				slog.Error("failed to unmarshal command from raft entry",
					"node_id", n.Id,
					"index", e.Index,
					"term", e.Term,
					"error", err,
				)
				if e.Index > n.lastApplied {
					n.lastApplied = e.Index
				}
				continue
			}

			n.applyCommandEntry(&req, e.Index, e.Term)

		default:
			// Unknown or unsupported entry type; just advance lastApplied.
			slog.Warn("ignoring unsupported raft entry type",
				"node_id", n.Id,
				"index", e.Index,
				"term", e.Term,
				"type", e.Type.String(),
			)
			if e.Index > n.lastApplied {
				n.lastApplied = e.Index
			}
		}
	}
}

// applyCommandEntry contains your existing "normal entry" logic that was
// previously inline in applyCommitted.
func (n *Node) applyCommandEntry(req *commandevents.CommandEventRequest, index, term uint64) {
	slog.Debug("applying committed command",
		"node_id", n.Id,
		"index", index,
		"term", term,
		"type", req.GetType().String(),
		"key", req.GetKey(),
		"event_id", req.EventId,
	)

	localRespCh := make(chan *commandevents.CommandEventResponse, 1)
	if err := n.commandSvc.Enqueue(req, localRespCh); err != nil {
		slog.Error("failed to enqueue command from raft",
			"node_id", n.Id,
			"type", req.GetType().String(),
			"key", req.GetKey(),
			"event_id", req.EventId,
			"error", err,
		)
		if index > n.lastApplied {
			n.lastApplied = index
		}
		return
	}

	resp := <-localRespCh
	close(localRespCh)

	if resp.EventId == 0 {
		resp.EventId = req.EventId
	}

	slog.Debug("command executed",
		"node_id", n.Id,
		"type", resp.GetType().String(),
		"key", req.GetKey(),
		"event_id", resp.EventId,
		"success", resp.Success,
	)

	n.mu.RLock()
	waiterCh, ok := n.pending[resp.EventId]
	n.mu.RUnlock()
	if ok {
		select {
		case waiterCh <- resp:
			// delivered to proposer
		default:
			slog.Debug("waiter channel full; dropping response",
				"node_id", n.Id,
				"event_id", resp.EventId,
			)
		}
	} else {
		slog.Debug("no waiter for response",
			"node_id", n.Id,
			"event_id", resp.EventId,
		)
	}

	if index > n.lastApplied {
		n.lastApplied = index
	}
}

func (n *Node) StartLoop() {
	n.stoppedWg.Add(1)

	go func() {
		defer n.stoppedWg.Done()

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-n.stopCh:
				slog.Info("raft loop stopping", "node_id", n.Id)
				return

			case <-ticker.C:
				// drive elections and heartbeats
				n.Node.Tick()

			case rd, ok := <-n.Node.Ready():
				if !ok {
					slog.Info("raft Ready channel closed", "node_id", n.Id)
					return
				}

				// 1\) persist snapshot, entries, hardstate (WAL + MemoryStorage)
				if err := n.storage.SaveReady(rd); err != nil {
					slog.Error("failed to save raft Ready",
						"node_id", n.Id,
						"error", err,
					)
					close(n.stopCh)
					return
				}

				// 2\) apply committed entries to the state machine
				if len(rd.CommittedEntries) > 0 {
					n.applyCommitted(rd.CommittedEntries)
				}

				// 3\) compute commit index used for ReadIndex waiters
				commitIdx := rd.HardState.Commit
				if len(rd.CommittedEntries) > 0 {
					last := rd.CommittedEntries[len(rd.CommittedEntries)-1]
					if last.Index > commitIdx {
						commitIdx = last.Index
					}
				}

				// 4\) wake up any ReadIndex waiters
				n.handleReadStates(rd.ReadStates, commitIdx)

				// 5\) send outgoing raft messages
				n.sendMessages(rd.Messages)

				// 6\) tell raft we are done with this Ready batch
				n.Node.Advance()
			}
		}
	}()
}

func (n *Node) send(msg raftpb.Message) {
	if msg.To == 0 {
		return
	}

	n.mu.RLock()
	client, ok := n.clients[msg.To]
	n.mu.RUnlock()
	if !ok {
		slog.Error("missing gRPC client for peer",
			"to", msg.To,
			"type", msg.Type.String(),
		)
		return
	}

	data, err := json.Marshal(msg)
	if err != nil {
		slog.Error("failed to marshal raft message",
			"to", msg.To,
			"type", msg.Type.String(),
			"error", err,
		)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.SendRaftMessage(ctx, &raftevents.RaftMessage{Data: data}); err != nil {
		slog.Error("failed to send raft message",
			"to", msg.To,
			"type", msg.Type.String(),
			"error", err,
		)
	}
}

func StartRestartNode(c *etcdraft.Config, rs *Storage, peersList []etcdraft.Peer) (etcdraft.Node, error) {
	isEmpty, err := rs.IsStorageEmpty()
	if err != nil {
		return nil, fmt.Errorf("failed to check if raft storage is empty with error: %v", err)
	}

	if isEmpty {
		// Fresh bootstrap
		slog.Debug("Starting New Node")
		peerStrs := make([]string, 0, len(peersList))
		for _, p := range peersList {
			if len(p.Context) > 0 {
				peerStrs = append(peerStrs, fmt.Sprintf("%d=%s", p.ID, string(p.Context)))
			} else {
				peerStrs = append(peerStrs, fmt.Sprintf("%d", p.ID))
			}
		}
		slog.Debug(fmt.Sprintf("peers are: %s", strings.Join(peerStrs, ",")))
		return etcdraft.StartNode(c, peersList), nil
	} else {
		slog.Debug("Restarting Node from saved state")
		return etcdraft.RestartNode(c), nil
	}

}

// RawNode returns the underlying etcd/raft Node (for transport server Step calls).
func (n *Node) RawNode() *etcdraft.Node {
	return &n.Node
}

func (n *Node) StopLoop() {
	close(n.stopCh)
	n.stoppedWg.Wait()
	if err := n.storage.Close(); err != nil {
		slog.Error("failed to close raft storage", "error", err)
	}
}

// Client is the subset of generated client used by Node.
type Client interface {
	SendRaftMessage(ctx context.Context, in *raftevents.RaftMessage, opts ...grpc.CallOption) (*raftevents.RaftMessageResponse, error)
	ProcessCommandEvent(ctx context.Context, in *commandevents.CommandEventRequest, opts ...grpc.CallOption) (*commandevents.CommandEventResponse, error)
}

// combinedClient implements Client by delegating to two underlying gRPC clients.
type combinedClient struct {
	raftClient    raftevents.RaftTransportServiceClient
	commandClient commandevents.CommandEventServiceClient
}

func (c *combinedClient) SendRaftMessage(
	ctx context.Context,
	in *raftevents.RaftMessage,
	opts ...grpc.CallOption,
) (*raftevents.RaftMessageResponse, error) {
	return c.raftClient.SendRaftMessage(ctx, in, opts...)
}

func (c *combinedClient) ProcessCommandEvent(
	ctx context.Context,
	in *commandevents.CommandEventRequest,
	opts ...grpc.CallOption,
) (*commandevents.CommandEventResponse, error) {
	return c.commandClient.ProcessCommandEvent(ctx, in, opts...)
}

// buildPeers converts `map\[uint64\]string` to `\[]etcdraft.Peer`.
//
// For a single Node, you can call NewRaftNode with an empty `peers` map;
// this helper will still include the local Node as the single peer.
func buildPeers(localID uint64, localAddr string, peers map[uint64]string) []etcdraft.Peer {
	peerList := make([]etcdraft.Peer, 0, len(peers)+1)

	// Always include the local Node as a peer for bootstrapping a 1-Node cluster.
	peerList = append(peerList, etcdraft.Peer{ID: localID, Context: []byte(localAddr)})

	for id, addr := range peers {
		if id == localID {
			continue
		}
		peerList = append(peerList, etcdraft.Peer{
			ID:      id,
			Context: []byte(addr), // optionally encode address in Context
		})
	}

	return peerList
}
