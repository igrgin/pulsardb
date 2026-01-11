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
	appliedIndex uint64
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
		CheckQuorum:               rc.CheckQuorum || rc.LeaseBasedRead,
		PreVote:                   rc.PreVote,
		ReadOnlyOption:            readOnlyOption(rc.LeaseBasedRead),
	}

	var peersList []etcdraft.Peer
	if !rc.Join {
		peersList = buildPeersList(rc.NodeID, localAddr, rc.RaftPeers)
	}

	slog.Info("raft config",
		"checkQuorum", c.CheckQuorum,
		"readOnlyOption", c.ReadOnlyOption,
		"leaseBasedRead", rc.LeaseBasedRead,
		"electionTick", electionTick,
		"heartbeatTick", heartbeatTick,
		"tickInterval", rc.TickInterval,
	)

	raftNode, err := startOrRestartNode(c, raftStore, peersList, rc.Join)
	if err != nil {
		return nil, fmt.Errorf("failed to start raft node: %w", err)
	}

	return &nodeConfig{
		storage:      raftStore,
		raftNode:     raftNode,
		appliedIndex: appliedIndex,
	}, nil
}

func readOnlyOption(leaseBasedRead bool) etcdraft.ReadOnlyOption {
	if leaseBasedRead {
		return etcdraft.ReadOnlyLeaseBased
	}
	return etcdraft.ReadOnlySafe
}

func startOrRestartNode(c *etcdraft.Config, rs *Storage, peersList []etcdraft.Peer, isJoining bool) (etcdraft.Node, error) {
	isEmpty, err := rs.IsStorageEmpty()
	if err != nil {
		return nil, fmt.Errorf("failed to check storageService state: %w", err)
	}

	if !isEmpty {
		slog.Debug("restarting raft node from saved state", "node_id", c.ID)

		return etcdraft.RestartNode(c), nil
	}

	if len(peersList) > 0 {
		slog.Info("bootstrapping cluster",
			"node_id", c.ID,
			"peers", formatPeers(peersList),
		)
		return etcdraft.StartNode(c, peersList), nil
	}

	if isJoining {
		slog.Info("New node joining existing cluster",
			"node_id", c.ID,
		)
		return etcdraft.RestartNode(c), nil
	}

	slog.Info("starting single node cluster", "node_id", c.ID)
	return etcdraft.StartNode(c, []etcdraft.Peer{{ID: c.ID}}), nil
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
