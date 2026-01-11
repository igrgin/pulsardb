package ops

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestEncodePeerMetadata(t *testing.T) {
	tests := []struct {
		name       string
		raftAddr   string
		clientAddr string
		want       string
	}{
		{
			name:       "both addresses",
			raftAddr:   "localhost:9001",
			clientAddr: "localhost:9000",
			want:       "localhost:9001|localhost:9000",
		},
		{
			name:       "only raft address",
			raftAddr:   "localhost:9001",
			clientAddr: "",
			want:       "localhost:9001",
		},
		{
			name:       "empty addresses",
			raftAddr:   "",
			clientAddr: "",
			want:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EncodePeerMetadata(tt.raftAddr, tt.clientAddr)
			if string(got) != tt.want {
				t.Errorf("EncodePeerMetadata() = %q, want %q", string(got), tt.want)
			}
		})
	}
}

func TestDecodePeerMetadata(t *testing.T) {
	tests := []struct {
		name           string
		data           []byte
		wantRaftAddr   string
		wantClientAddr string
	}{
		{
			name:           "both addresses",
			data:           []byte("localhost:9001|localhost:9000"),
			wantRaftAddr:   "localhost:9001",
			wantClientAddr: "localhost:9000",
		},
		{
			name:           "only raft address",
			data:           []byte("localhost:9001"),
			wantRaftAddr:   "localhost:9001",
			wantClientAddr: "",
		},
		{
			name:           "empty data",
			data:           []byte(""),
			wantRaftAddr:   "",
			wantClientAddr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRaft, gotClient := DecodePeerMetadata(tt.data)
			if gotRaft != tt.wantRaftAddr {
				t.Errorf("DecodePeerMetadata() raftAddr = %q, want %q", gotRaft, tt.wantRaftAddr)
			}
			if gotClient != tt.wantClientAddr {
				t.Errorf("DecodePeerMetadata() clientAddr = %q, want %q", gotClient, tt.wantClientAddr)
			}
		})
	}
}

func TestRoundTripPeerMetadata(t *testing.T) {
	raftAddr := "192.168.1.1:5001"
	clientAddr := "192.168.1.1:5000"

	encoded := EncodePeerMetadata(raftAddr, clientAddr)
	gotRaft, gotClient := DecodePeerMetadata(encoded)

	if gotRaft != raftAddr {
		t.Errorf("Round trip raftAddr = %q, want %q", gotRaft, raftAddr)
	}
	if gotClient != clientAddr {
		t.Errorf("Round trip clientAddr = %q, want %q", gotClient, clientAddr)
	}
}

func TestBuildAddNodeChange(t *testing.T) {
	cc := BuildAddNodeChange(1, "localhost:9001", "localhost:9000")

	if cc.Type != raftpb.ConfChangeAddNode {
		t.Errorf("BuildAddNodeChange() Type = %v, want %v", cc.Type, raftpb.ConfChangeAddNode)
	}
	if cc.NodeID != 1 {
		t.Errorf("BuildAddNodeChange() NodeID = %d, want %d", cc.NodeID, 1)
	}

	raftAddr, clientAddr := DecodePeerMetadata(cc.Context)
	if raftAddr != "localhost:9001" {
		t.Errorf("BuildAddNodeChange() raftAddr = %q, want %q", raftAddr, "localhost:9001")
	}
	if clientAddr != "localhost:9000" {
		t.Errorf("BuildAddNodeChange() clientAddr = %q, want %q", clientAddr, "localhost:9000")
	}
}

func TestBuildAddLearnerChange(t *testing.T) {
	cc := BuildAddLearnerChange(2, "localhost:9003", "localhost:9002")

	if cc.Type != raftpb.ConfChangeAddLearnerNode {
		t.Errorf("BuildAddLearnerChange() Type = %v, want %v", cc.Type, raftpb.ConfChangeAddLearnerNode)
	}
	if cc.NodeID != 2 {
		t.Errorf("BuildAddLearnerChange() NodeID = %d, want %d", cc.NodeID, 2)
	}
}

func TestBuildRemoveNodeChange(t *testing.T) {
	cc := BuildRemoveNodeChange(3)

	if cc.Type != raftpb.ConfChangeRemoveNode {
		t.Errorf("BuildRemoveNodeChange() Type = %v, want %v", cc.Type, raftpb.ConfChangeRemoveNode)
	}
	if cc.NodeID != 3 {
		t.Errorf("BuildRemoveNodeChange() NodeID = %d, want %d", cc.NodeID, 3)
	}
}

func TestIsVoter(t *testing.T) {
	cs := raftpb.ConfState{
		Voters:   []uint64{1, 2, 3},
		Learners: []uint64{4, 5},
	}

	tests := []struct {
		nodeID uint64
		want   bool
	}{
		{1, true},
		{2, true},
		{3, true},
		{4, false},
		{5, false},
		{6, false},
	}

	for _, tt := range tests {
		got := IsVoter(cs, tt.nodeID)
		if got != tt.want {
			t.Errorf("IsVoter(%d) = %v, want %v", tt.nodeID, got, tt.want)
		}
	}
}

func TestIsLearner(t *testing.T) {
	cs := raftpb.ConfState{
		Voters:   []uint64{1, 2, 3},
		Learners: []uint64{4, 5},
	}

	tests := []struct {
		nodeID uint64
		want   bool
	}{
		{1, false},
		{2, false},
		{3, false},
		{4, true},
		{5, true},
		{6, false},
	}

	for _, tt := range tests {
		got := IsLearner(cs, tt.nodeID)
		if got != tt.want {
			t.Errorf("IsLearner(%d) = %v, want %v", tt.nodeID, got, tt.want)
		}
	}
}

func TestIsInCluster(t *testing.T) {
	cs := raftpb.ConfState{
		Voters:   []uint64{1, 2, 3},
		Learners: []uint64{4, 5},
	}

	tests := []struct {
		nodeID uint64
		want   bool
	}{
		{1, true},
		{2, true},
		{3, true},
		{4, true},
		{5, true},
		{6, false},
		{0, false},
	}

	for _, tt := range tests {
		got := IsInCluster(cs, tt.nodeID)
		if got != tt.want {
			t.Errorf("IsInCluster(%d) = %v, want %v", tt.nodeID, got, tt.want)
		}
	}
}
