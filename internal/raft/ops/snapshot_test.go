package ops

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestSerializeSnapshot(t *testing.T) {
	data := []byte("test snapshot data")
	confState := raftpb.ConfState{
		Voters:   []uint64{1, 2, 3},
		Learners: []uint64{4},
	}

	snap := SerializeSnapshot(100, 5, data, confState)

	if snap.Metadata.Index != 100 {
		t.Errorf("Snapshot index = %d, want 100", snap.Metadata.Index)
	}
	if snap.Metadata.Term != 5 {
		t.Errorf("Snapshot term = %d, want 5", snap.Metadata.Term)
	}
	if string(snap.Data) != "test snapshot data" {
		t.Errorf("Snapshot data = %q, want %q", snap.Data, "test snapshot data")
	}
	if len(snap.Metadata.ConfState.Voters) != 3 {
		t.Errorf("Snapshot voters count = %d, want 3", len(snap.Metadata.ConfState.Voters))
	}
}

func TestValidateSnapshot(t *testing.T) {
	tests := []struct {
		name    string
		snap    raftpb.Snapshot
		wantErr bool
	}{
		{
			name: "valid snapshot",
			snap: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 100,
					Term:  5,
				},
			},
			wantErr: false,
		},
		{
			name: "zero index",
			snap: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 0,
					Term:  5,
				},
			},
			wantErr: true,
		},
		{
			name: "zero term",
			snap: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 100,
					Term:  0,
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSnapshot(tt.snap)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateSnapshot() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIsEmptySnapshot(t *testing.T) {
	tests := []struct {
		name string
		snap raftpb.Snapshot
		want bool
	}{
		{
			name: "empty data",
			snap: raftpb.Snapshot{Data: nil},
			want: true,
		},
		{
			name: "empty slice",
			snap: raftpb.Snapshot{Data: []byte{}},
			want: true,
		},
		{
			name: "with data",
			snap: raftpb.Snapshot{Data: []byte("data")},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsEmptySnapshot(tt.snap)
			if got != tt.want {
				t.Errorf("IsEmptySnapshot() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractSnapshotData(t *testing.T) {
	snap := raftpb.Snapshot{
		Data: []byte("snapshot data"),
		Metadata: raftpb.SnapshotMetadata{
			Index: 150,
			Term:  10,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1, 2},
			},
		},
	}

	extracted := ExtractSnapshotData(snap)

	if extracted.Index != 150 {
		t.Errorf("Index = %d, want 150", extracted.Index)
	}
	if extracted.Term != 10 {
		t.Errorf("Term = %d, want 10", extracted.Term)
	}
	if string(extracted.Data) != "snapshot data" {
		t.Errorf("Data = %q, want %q", extracted.Data, "snapshot data")
	}
	if len(extracted.ConfState.Voters) != 2 {
		t.Errorf("Voters count = %d, want 2", len(extracted.ConfState.Voters))
	}
}
