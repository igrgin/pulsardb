package ops

import (
	"strings"

	"go.etcd.io/raft/v3/raftpb"
)

const metadataSeparator = "|"

func EncodePeerMetadata(raftAddr, clientAddr string) []byte {
	if clientAddr == "" {
		return []byte(raftAddr)
	}
	return []byte(raftAddr + metadataSeparator + clientAddr)
}

func DecodePeerMetadata(data []byte) (raftAddr, clientAddr string) {
	s := string(data)
	if s == "" {
		return "", ""
	}

	parts := strings.Split(s, metadataSeparator)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}

	return parts[0], ""
}

func BuildAddNodeChange(nodeID uint64, raftAddr, clientAddr string) raftpb.ConfChange {
	return raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeID,
		Context: EncodePeerMetadata(raftAddr, clientAddr),
	}
}

func BuildAddLearnerChange(nodeID uint64, raftAddr, clientAddr string) raftpb.ConfChange {
	return raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddLearnerNode,
		NodeID:  nodeID,
		Context: EncodePeerMetadata(raftAddr, clientAddr),
	}
}

func BuildRemoveNodeChange(nodeID uint64) raftpb.ConfChange {
	return raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: nodeID,
	}
}

func BuildPromoteChange(nodeID uint64, raftAddr, clientAddr string) raftpb.ConfChange {
	return raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeID,
		Context: EncodePeerMetadata(raftAddr, clientAddr),
	}
}

func IsVoter(confState raftpb.ConfState, nodeID uint64) bool {
	for _, v := range confState.Voters {
		if v == nodeID {
			return true
		}
	}
	return false
}

func IsLearner(confState raftpb.ConfState, nodeID uint64) bool {
	for _, l := range confState.Learners {
		if l == nodeID {
			return true
		}
	}
	return false
}

func IsInCluster(confState raftpb.ConfState, nodeID uint64) bool {
	return IsVoter(confState, nodeID) || IsLearner(confState, nodeID)
}
