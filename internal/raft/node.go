package raft

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"pulsardb/internal/command"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/storage"
	commandevents "pulsardb/internal/transport/gen"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Node represents a raft consensus node.
type Node struct {
	Id           uint64
	raftNode     etcdraft.Node
	localAddress string

	// Services
	storage    *Storage
	storeSvc   *storage.Service
	commandSvc *command.Service

	// Cluster membership
	peers     map[uint64]string
	clients   map[uint64]Client
	confState raftpb.ConfState

	// Leader tracking
	isLeader bool
	leaderID uint64

	// Proposal handling
	pending   map[uint64]chan *commandevents.CommandEventResponse
	nextReqID uint64

	// Linearizable reads
	readWaiters map[string]chan uint64
	readMu      sync.Mutex

	// State tracking
	lastApplied uint64

	// Synchronization
	mu sync.RWMutex

	// Lifecycle
	stopCh    chan struct{}
	stoppedWg sync.WaitGroup

	// Snapshot function - provided by storage service
	getSnapshot func() ([]byte, error)
}

// NewNode creates a new raft node with the given configuration.
func NewNode(rc *properties.RaftConfigProperties, storeSvc *storage.Service, commandSvc *command.Service, localAddr string) (*Node, error) {
	cfg, err := newNodeConfig(rc, localAddr)
	if err != nil {
		return nil, err
	}

	n := &Node{
		Id:           rc.NodeId,
		localAddress: localAddr,
		storage:      cfg.storage,
		storeSvc:     storeSvc,
		commandSvc:   commandSvc,
		peers:        rc.Peers,
		clients:      make(map[uint64]Client),
		pending:      make(map[uint64]chan *commandevents.CommandEventResponse),
		stopCh:       make(chan struct{}),
		readWaiters:  make(map[string]chan uint64),
		lastApplied:  cfg.lastIndex,
		confState:    cfg.storage.ConfState(),
	}

	n.raftNode = cfg.raftNode

	if err := n.initAllPeerClients(); err != nil {
		return nil, err
	}

	slog.Info("raft node created", "id", rc.NodeId)
	return n, nil
}

// RawNode returns the underlying etcd/raft Node.
func (n *Node) RawNode() *etcdraft.Node {
	return &n.raftNode
}

// Status returns the current raft status.
func (n *Node) Status() etcdraft.Status {
	return n.raftNode.Status()
}

// Start begins the raft event loop.
func (n *Node) Start() {
	// First, restore state from snapshot and WAL
	if err := n.recoverState(); err != nil {
		slog.Error("failed to recover state", "node_id", n.Id, "error", err)
	}

	n.restoreFromConfState()
	n.startLoop()
}

// Stop gracefully shuts down the raft node.
func (n *Node) Stop() {
	close(n.stopCh)
	n.stoppedWg.Wait()

	if err := n.storage.Close(); err != nil {
		slog.Error("failed to close raft storage", "error", err)
	}

	slog.Info("raft node stopped", "id", n.Id)
}

// recoverState rebuilds in-memory state from snapshot and WAL entries.
func (n *Node) recoverState() error {
	slog.Info("recovering state from storage", "node_id", n.Id)

	// 1) Load and apply snapshot data to storage service
	snap, err := n.storage.RaftStorage().Snapshot()
	if err != nil {
		return err
	}

	snapIndex := snap.Metadata.Index
	if len(snap.Data) > 0 {
		slog.Info("restoring from snapshot",
			"node_id", n.Id,
			"index", snapIndex,
			"term", snap.Metadata.Term,
		)
		if err := n.storeSvc.RestoreFromSnapshot(snap.Data); err != nil {
			return err
		}
	}

	entries, err := n.storage.EntriesAfter(snapIndex)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		slog.Info("no entries to replay", "node_id", n.Id)
		return nil
	}

	slog.Info("replaying WAL entries",
		"node_id", n.Id,
		"count", len(entries),
		"from_index", entries[0].Index,
		"to_index", entries[len(entries)-1].Index,
	)

	// 3) Replay each entry synchronously (blocking, not through command queue)
	for _, entry := range entries {
		if err := n.replayEntry(entry); err != nil {
			slog.Error("failed to replay entry",
				"node_id", n.Id,
				"index", entry.Index,
				"error", err,
			)
			// Continue replaying other entries
		}
	}

	n.lastApplied = entries[len(entries)-1].Index
	slog.Info("state recovery complete",
		"node_id", n.Id,
		"last_applied", n.lastApplied,
	)

	return nil
}

// replayEntry applies a single entry during recovery (synchronous, no response channel).
func (n *Node) replayEntry(entry raftpb.Entry) error {
	switch entry.Type {
	case raftpb.EntryConfChange:
		// Conf changes were already applied to raft during storage load
		// Just update our local confState tracking
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			return err
		}
		slog.Debug("replayed conf change",
			"node_id", n.Id,
			"index", entry.Index,
			"type", cc.Type.String(),
			"target_node", cc.NodeID,
		)

	case raftpb.EntryNormal:
		if len(entry.Data) == 0 {
			return nil // Empty entry (leader election, etc.)
		}

		var req commandevents.CommandEventRequest
		if err := proto.Unmarshal(entry.Data, &req); err != nil {
			return err
		}

		// Apply directly to storage (bypass command queue during replay)
		if err := n.applyToStorage(&req); err != nil {
			slog.Error("failed to apply entry to storage",
				"node_id", n.Id,
				"index", entry.Index,
				"type", req.GetType().String(),
				"key", req.GetKey(),
				"error", err,
			)
			return err
		}

		slog.Debug("replayed entry",
			"node_id", n.Id,
			"index", entry.Index,
			"type", req.GetType().String(),
			"key", req.GetKey(),
		)
	}

	return nil
}

// applyToStorage applies a command directly to storage (used during replay).
func (n *Node) applyToStorage(req *commandevents.CommandEventRequest) error {
	switch req.GetType() {
	case commandevents.CommandEventType_SET:
		val := command.ValueFromProto(req.GetCmdValue())
		n.storeSvc.Set(req.GetKey(), val)
		slog.Debug("applied SET to storage during replay",
			"key", req.GetKey(),
			"value", val,
			"storage_len", n.storeSvc.Len(),
		)
	case commandevents.CommandEventType_DELETE:
		n.storeSvc.Delete(req.GetKey())
		slog.Debug("applied DELETE to storage during replay",
			"key", req.GetKey(),
		)
	case commandevents.CommandEventType_GET:
		slog.Warn("GET command's shouldn't be replayed")
	}
	return nil
}

// nextRequestID generates a unique request ID.
func (n *Node) nextRequestID() uint64 {
	return atomic.AddUint64(&n.nextReqID, 1)
}

// restoreFromConfState restores peer connections from persisted confState.
func (n *Node) restoreFromConfState() {
	slog.Debug("restoring from confState",
		"node_id", n.Id,
		"voters", n.confState.Voters,
		"learners", n.confState.Learners,
	)

	if len(n.confState.Voters) == 0 {
		slog.Debug("confState empty, using configured peers for bootstrap",
			"node_id", n.Id,
			"peers", n.peers,
		)
		return
	}

	for _, voterID := range n.confState.Voters {
		if voterID == n.Id {
			continue
		}

		addr, ok := n.peers[voterID]
		if !ok {
			slog.Warn("no address configured for voter",
				"node_id", n.Id,
				"voter_id", voterID,
			)
			continue
		}

		n.mu.RLock()
		_, exists := n.clients[voterID]
		n.mu.RUnlock()

		if !exists {
			if err := n.initPeerClient(voterID, addr); err != nil {
				slog.Warn("failed to restore peer client",
					"peer_id", voterID,
					"addr", addr,
					"error", err,
				)
			} else {
				slog.Info("restored peer client from confState",
					"node_id", n.Id,
					"peer_id", voterID,
					"addr", addr,
				)
			}
		}
	}
}
