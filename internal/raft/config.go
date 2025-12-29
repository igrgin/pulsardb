package raft

import (
	"fmt"
	"log/slog"
	"pulsardb/internal/configuration"
	"strings"

	etcdraft "go.etcd.io/raft/v3"
)

type nodeConfig struct {
	storage      *Storage
	raftNode     etcdraft.Node
	AppliedIndex uint64
}

func newNodeConfig(rc *configuration.RaftConfigurationProperties, localAddr string) (*nodeConfig, error) {
	raftStore, appliedIndex, err := OpenStorage(rc.StorageDir, rc.Wal.NoSync)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft storageService: %w", err)
	}
	slog.Debug("created raft storageService", "dir", rc.StorageDir)

	etcdCfg := rc.Etcd
	electionTick := 10
	if etcdCfg.ElectionTick != 0 {
		electionTick = etcdCfg.ElectionTick
	}
	heartbeatTick := 1
	if etcdCfg.HeartbeatTick != 0 {
		heartbeatTick = etcdCfg.HeartbeatTick
	}
	maxSizePerMsg := uint64(1024 * 1024)
	if etcdCfg.MaxSizePerMsg != 0 {
		maxSizePerMsg = etcdCfg.MaxSizePerMsg
	}
	maxInflight := 256
	if etcdCfg.MaxInflightMsgs != 0 {
		maxInflight = etcdCfg.MaxInflightMsgs
	}
	maxUncommitted := uint64(1 << 30)
	if etcdCfg.MaxUncommittedEntriesSize != 0 {
		maxUncommitted = etcdCfg.MaxUncommittedEntriesSize
	}

	c := &etcdraft.Config{
		ID:                        rc.NodeID,
		ElectionTick:              electionTick,
		HeartbeatTick:             heartbeatTick,
		Storage:                   raftStore.RaftStorage(),
		MaxSizePerMsg:             maxSizePerMsg,
		MaxInflightMsgs:           maxInflight,
		MaxUncommittedEntriesSize: maxUncommitted,
		Logger:                    NewSlogRaftLogger(),
		Applied:                   appliedIndex,
	}

	peersList := buildPeersList(rc.NodeID, localAddr, rc.RaftPeers)
	raftNode, err := startOrRestartNode(c, raftStore, peersList, rc.Join)
	if err != nil {
		return nil, fmt.Errorf("failed to start raft node: %w", err)
	}

	return &nodeConfig{
		storage:      raftStore,
		raftNode:     raftNode,
		AppliedIndex: appliedIndex,
	}, nil
}

func startOrRestartNode(c *etcdraft.Config, rs *Storage, peersList []etcdraft.Peer, join bool) (etcdraft.Node, error) {
	isEmpty, err := rs.IsStorageEmpty()
	if err != nil {
		return nil, fmt.Errorf("failed to check storageService state: %w", err)
	}

	if !isEmpty {
		slog.Debug("restarting raft node from saved state", "node_id", c.ID)

		return etcdraft.RestartNode(c), nil
	}

	if join {
		slog.Info("joining cluster - starting with empty state, waiting for snapshot",
			"node_id", c.ID,
		)

		if err := rs.MarkClusterInitialized(); err != nil {
			slog.Warn("failed to write cluster marker", "error", err)
		}

		return etcdraft.RestartNode(c), nil
	}

	if rs.WasPreviouslyInitialized() {

		slog.Info("rejoining cluster - starting with empty state",
			"node_id", c.ID,
		)
		return etcdraft.RestartNode(c), nil
	}

	slog.Info("starting cluster",
		"node_id", c.ID,
		"peers", formatPeers(peersList),
	)

	if err := rs.MarkClusterInitialized(); err != nil {
		slog.Warn("failed to write cluster marker", "error", err)
	}

	return etcdraft.StartNode(c, peersList), nil
}

func buildPeersList(localID uint64, localAddr string, peers map[uint64]string) []etcdraft.Peer {
	peerList := make([]etcdraft.Peer, 0, len(peers)+1)

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
