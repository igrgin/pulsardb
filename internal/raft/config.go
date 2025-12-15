package raft

import (
	"fmt"
	"log/slog"
	"strings"

	"pulsardb/internal/configuration/properties"

	etcdraft "go.etcd.io/raft/v3"
)

type nodeConfig struct {
	storage      *Storage
	raftNode     etcdraft.Node
	AppliedIndex uint64
}

func newNodeConfig(rc *properties.RaftConfigProperties, localAddr string) (*nodeConfig, error) {
	raftStore, appliedIndex, err := OpenStorage(rc.StorageBaseDir, rc.Wal.NoSync)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft storage: %w", err)
	}
	slog.Debug("created raft storage", "dir", rc.StorageBaseDir)

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
		ID:                        rc.NodeId,
		ElectionTick:              electionTick,
		HeartbeatTick:             heartbeatTick,
		Storage:                   raftStore.RaftStorage(),
		MaxSizePerMsg:             maxSizePerMsg,
		MaxInflightMsgs:           maxInflight,
		MaxUncommittedEntriesSize: maxUncommitted,
		Logger:                    NewSlogRaftLogger(),
		Applied:                   appliedIndex,
	}

	peersList := buildPeersList(rc.NodeId, localAddr, rc.RaftPeers)
	raftNode, err := startOrRestartNode(c, raftStore, peersList)
	if err != nil {
		return nil, fmt.Errorf("failed to start raft Node:  %w", err)
	}

	return &nodeConfig{
		storage:      raftStore,
		raftNode:     raftNode,
		AppliedIndex: appliedIndex,
	}, nil
}

func startOrRestartNode(c *etcdraft.Config, rs *Storage, peersList []etcdraft.Peer) (etcdraft.Node, error) {
	isEmpty, err := rs.IsStorageEmpty()
	if err != nil {
		return nil, fmt.Errorf("failed to check storage state: %w", err)
	}

	if isEmpty {
		slog.Debug("starting new raft Node", "raftPeers", formatPeers(peersList))
		return etcdraft.StartNode(c, peersList), nil
	}

	slog.Debug("restarting raft Node from saved state")
	return etcdraft.RestartNode(c), nil
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
