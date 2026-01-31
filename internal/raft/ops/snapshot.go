package ops

import (
	"fmt"

	"go.etcd.io/raft/v3/raftpb"
)

type SnapshotData struct {
	Index     uint64
	Term      uint64
	Data      []byte
	ConfState raftpb.ConfState
}

func SerializeSnapshot(index uint64, term uint64, data []byte, confState raftpb.ConfState) raftpb.Snapshot {
	return raftpb.Snapshot{
		Data: data,
		Metadata: raftpb.SnapshotMetadata{
			Index:     index,
			Term:      term,
			ConfState: confState,
		},
	}
}

func ValidateSnapshot(snap raftpb.Snapshot) error {
	if snap.Metadata.Index == 0 {
		return fmt.Errorf("snapshot index is zero")
	}
	if snap.Metadata.Term == 0 {
		return fmt.Errorf("snapshot term is zero")
	}
	return nil
}

func IsEmptySnapshot(snap raftpb.Snapshot) bool {
	return len(snap.Data) == 0
}

func ExtractSnapshotData(snap raftpb.Snapshot) SnapshotData {
	return SnapshotData{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		Data:      snap.Data,
		ConfState: snap.Metadata.ConfState,
	}
}
