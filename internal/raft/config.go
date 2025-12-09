package raft

import (
	"fmt"
	"log/slog"
	"strings"

	"pulsardb/internal/configuration/properties"

	etcdraft "go.etcd.io/raft/v3"
)

// nodeConfig holds initialization configuration for a Node.
type nodeConfig struct {
	storage   *Storage
	raftNode  etcdraft.Node
	lastIndex uint64
}

// newNodeConfig creates the raft storage and node based on configuration.
func newNodeConfig(rc *properties.RaftConfigProperties, localAddr string) (*nodeConfig, error) {
	raftStore, lastIndex, err := OpenStorage(rc.StorageBaseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft storage: %w", err)
	}
	slog.Debug("created raft storage", "dir", rc.StorageBaseDir)

	c := &etcdraft.Config{
		ID:                        rc.NodeId,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   raftStore.RaftStorage(),
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
		Logger:                    NewSlogRaftLogger(),
		Applied:                   lastIndex,
	}

	peersList := buildPeersList(rc.NodeId, localAddr, rc.Peers)
	raftNode, err := startOrRestartNode(c, raftStore, peersList)
	if err != nil {
		return nil, fmt.Errorf("failed to start raft node:  %w", err)
	}

	return &nodeConfig{
		storage:   raftStore,
		raftNode:  raftNode,
		lastIndex: lastIndex,
	}, nil
}

// startOrRestartNode creates a new raft node or restarts from existing state.
func startOrRestartNode(c *etcdraft.Config, rs *Storage, peersList []etcdraft.Peer) (etcdraft.Node, error) {
	isEmpty, err := rs.IsStorageEmpty()
	if err != nil {
		return nil, fmt.Errorf("failed to check storage state: %w", err)
	}

	if isEmpty {
		slog.Debug("starting new raft node", "peers", formatPeers(peersList))
		return etcdraft.StartNode(c, peersList), nil
	}

	slog.Debug("restarting raft node from saved state")
	return etcdraft.RestartNode(c), nil
}

// buildPeersList converts peer map to raft. Peer slice.
func buildPeersList(localID uint64, localAddr string, peers map[uint64]string) []etcdraft.Peer {
	peerList := make([]etcdraft.Peer, 0, len(peers)+1)

	// Always include local node for bootstrapping
	peerList = append(peerList, etcdraft.Peer{
		ID:      localID,
		Context: []byte(localAddr),
	})

	for id, addr := range peers {
		if id == localID {
			continue
		}
		peerList = append(peerList, etcdraft.Peer{
			ID:      id,
			Context: []byte(addr),
		})
	}

	return peerList
}

// formatPeers returns a human-readable string of peers for logging.
func formatPeers(peers []etcdraft.Peer) string {
	strs := make([]string, 0, len(peers))
	for _, p := range peers {
		if len(p.Context) > 0 {
			strs = append(strs, fmt.Sprintf("%d=%s", p.ID, string(p.Context)))
		} else {
			strs = append(strs, fmt.Sprintf("%d", p.ID))
		}
	}
	return strings.Join(strs, ",")
}
