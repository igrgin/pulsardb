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

	tickInterval uint64
	snapCount    uint64
	Timeout      uint64
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
		tickInterval: rc.TickInterval,
		snapCount:    rc.SnapCount,
		Timeout:      rc.Timeout,
	}

	n.raftNode = cfg.raftNode

	if err := n.initAllPeerClients(); err != nil {
		return nil, err
	}

	slog.Info("raft node created", "id", rc.NodeId)
	return n, nil
}

func (n *Node) RawNode() *etcdraft.Node {
	return &n.raftNode
}

func (n *Node) Status() etcdraft.Status {
	return n.raftNode.Status()
}

func (n *Node) Start() {
	// CRITICAL:  Recover application state BEFORE starting the raft loop
	if err := n.recoverState(); err != nil {
		slog.Error("failed to recover state", "node_id", n.Id, "error", err)
	}

	n.restoreFromConfState()
	n.startLoop()
}

func (n *Node) Stop() {
	close(n.stopCh)
	n.stoppedWg.Wait()

	if err := n.storage.Close(); err != nil {
		slog.Error("failed to close raft storage", "error", err)
	}

	slog.Info("raft node stopped", "id", n.Id)
}

// recoverState rebuilds the application's KV store from snapshot and WAL.
func (n *Node) recoverState() error {
	slog.Info("recovering application state", "node_id", n.Id)

	// 1) Get the actual snapshot index from the storage (NOT from MemoryStorage)
	//    This is the index of the real data snapshot file, or 0 if none exists
	snapIndex := n.storage.SnapshotIndex()
	snapData := n.storage.SnapshotData()

	slog.Debug("recovery info",
		"node_id", n.Id,
		"snapshot_index", snapIndex,
		"has_snapshot_data", len(snapData) > 0,
	)

	// 2) Restore from snapshot data if present
	if len(snapData) > 0 {
		slog.Info("restoring KV store from snapshot",
			"node_id", n.Id,
			"index", snapIndex,
			"data_size", len(snapData),
		)
		if err := n.storeSvc.RestoreFromSnapshot(snapData); err != nil {
			return err
		}
		slog.Info("restored from snapshot",
			"node_id", n.Id,
			"keys", n.storeSvc.Len(),
		)
	}

	// 3) Get committed entries AFTER the snapshot index
	entries, err := n.storage.EntriesAfter(snapIndex)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		slog.Info("no WAL entries to replay",
			"node_id", n.Id,
			"snapshot_index", snapIndex,
		)
		// Still set lastApplied from hardstate if we have it
		if snapIndex > n.lastApplied {
			n.lastApplied = snapIndex
		}
		return nil
	}

	slog.Info("replaying WAL entries to KV store",
		"node_id", n.Id,
		"count", len(entries),
		"from_index", entries[0].Index,
		"to_index", entries[len(entries)-1].Index,
	)

	// 4) Replay each committed entry to the application store
	replayedCount := 0
	for _, entry := range entries {
		if err := n.replayEntry(entry); err != nil {
			slog.Error("failed to replay entry",
				"node_id", n.Id,
				"index", entry.Index,
				"error", err,
			)
			// Continue with other entries
		} else {
			replayedCount++
		}
	}

	n.lastApplied = entries[len(entries)-1].Index

	slog.Info("application state recovery complete",
		"node_id", n.Id,
		"last_applied", n.lastApplied,
		"entries_replayed", replayedCount,
		"storage_keys", n.storeSvc.Len(),
	)

	return nil
}

// replayEntry applies a single entry to the application store during recovery.
func (n *Node) replayEntry(entry raftpb.Entry) error {
	switch entry.Type {
	case raftpb.EntryConfChange:
		// Conf changes don't affect KV store, just log them
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			return err
		}
		slog.Debug("skipped conf change during KV replay",
			"node_id", n.Id,
			"index", entry.Index,
			"type", cc.Type.String(),
			"target_node", cc.NodeID,
		)
		return nil

	case raftpb.EntryNormal:
		if len(entry.Data) == 0 {
			// Empty entry (leader election, etc.)
			return nil
		}

		var req commandevents.CommandEventRequest
		if err := proto.Unmarshal(entry.Data, &req); err != nil {
			return err
		}

		// Apply directly to storage (bypass command queue during replay)
		if err := n.applyToStorage(&req); err != nil {
			return err
		}

		slog.Debug("replayed entry to KV store",
			"node_id", n.Id,
			"index", entry.Index,
			"type", req.GetType().String(),
			"key", req.GetKey(),
		)
		return nil

	default:
		slog.Warn("unknown entry type during replay",
			"node_id", n.Id,
			"index", entry.Index,
			"type", entry.Type.String(),
		)
		return nil
	}
}

// applyToStorage applies a command directly to the KV store.
func (n *Node) applyToStorage(req *commandevents.CommandEventRequest) error {
	switch req.GetType() {
	case commandevents.CommandEventType_SET:
		val := command.ValueFromProto(req.GetCmdValue())
		n.storeSvc.Set(req.GetKey(), val)
	case commandevents.CommandEventType_DELETE:
		n.storeSvc.Delete(req.GetKey())
	case commandevents.CommandEventType_GET:
		// GET doesn't modify state
	}
	return nil
}

func (n *Node) nextRequestID() uint64 {
	return atomic.AddUint64(&n.nextReqID, 1)
}

func (n *Node) restoreFromConfState() {
	slog.Debug("restoring peer connections from confState",
		"node_id", n.Id,
		"voters", n.confState.Voters,
		"learners", n.confState.Learners,
	)

	if len(n.confState.Voters) == 0 {
		slog.Debug("confState empty, using configured peers",
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
			slog.Warn("no address for voter",
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
				slog.Warn("failed to init peer client",
					"peer_id", voterID,
					"addr", addr,
					"error", err,
				)
			} else {
				slog.Debug("restored peer client",
					"node_id", n.Id,
					"peer_id", voterID,
				)
			}
		}
	}
}
