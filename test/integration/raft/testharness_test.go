package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/raft"
	snapshotpb "pulsardb/internal/raft/gen"
	"pulsardb/internal/store"
	"pulsardb/internal/transport/gen/commandevents"
	"pulsardb/internal/transport/gen/raft"
	"pulsardb/internal/transport/util"
	"pulsardb/internal/types"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type TestCluster struct {
	t        *testing.T
	nodes    map[uint64]*TestNode
	baseDir  string
	mu       sync.RWMutex
	nextPort int
}

type TestNode struct {
	ID             uint64
	Node           *raft.Node
	Service        *raft.Service
	Storage        *store.Service
	StateMachine   *TestStateMachine
	Batcher        *raft.Batcher
	RaftServer     *grpc.Server
	ClientServer   *grpc.Server
	RaftAddr       string
	ClientAddr     string
	RaftListener   net.Listener
	ClientListener net.Listener
	stopped        bool
	mu             sync.Mutex
}

type TestStateMachine struct {
	mu        sync.RWMutex
	data      map[string][]byte
	applied   [][]byte
	pending   map[uint64]chan *commandeventspb.CommandEventResponse
	pendingMu sync.RWMutex
}

func NewTestStateMachine() *TestStateMachine {
	return &TestStateMachine{
		data:    make(map[string][]byte),
		applied: make([][]byte, 0),
		pending: make(map[uint64]chan *commandeventspb.CommandEventResponse),
	}
}

func (sm *TestStateMachine) Apply(data []byte) ([]byte, error) {
	sm.mu.Lock()
	sm.applied = append(sm.applied, data)
	sm.mu.Unlock()

	var batch commandeventspb.BatchedCommands
	if err := proto.Unmarshal(data, &batch); err != nil {
		var req commandeventspb.CommandEventRequest
		if err := proto.Unmarshal(data, &req); err != nil {
			return nil, err
		}
		return sm.applyCommand(&req)
	}

	for _, cmd := range batch.Commands {
		resp, err := sm.applyCommand(cmd)
		if err != nil {
			sm.respondToPending(cmd.EventId, transport.ErrorResponse(cmd.EventId, commandeventspb.ErrorCode_UNKNOWN, ""))
			continue
		}
		if cmd.GetType() == commandeventspb.CommandEventType_GET {
			sm.respondToPending(cmd.EventId, transport.SuccessReadResponse(cmd.EventId, types.ValueToProto(resp)))
		} else {
			sm.respondToPending(cmd.EventId, transport.SuccessModifyResponse(cmd.EventId))
		}
	}

	return nil, nil
}

func (sm *TestStateMachine) RestoreFromSnapshot(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var snap snapshotpb.KVSnapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.data = make(map[string][]byte, len(snap.Entries))
	for _, entry := range snap.Entries {

		val := types.FromProtoValue(entry.Value)
		switch v := val.(type) {
		case string:
			sm.data[entry.Key] = []byte(v)
		case []byte:
			sm.data[entry.Key] = v
		default:
			sm.data[entry.Key] = []byte(fmt.Sprintf("%v", v))
		}
	}

	return nil
}

func (sm *TestStateMachine) applyCommand(req *commandeventspb.CommandEventRequest) ([]byte, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	switch req.GetType() {
	case commandeventspb.CommandEventType_SET:
		val := req.GetValue()
		if val != nil {
			sm.data[req.GetKey()] = []byte(val.GetStringValue())
		}
		return nil, nil
	case commandeventspb.CommandEventType_DELETE:
		delete(sm.data, req.GetKey())
		return nil, nil
	case commandeventspb.CommandEventType_GET:
		if v, ok := sm.data[req.GetKey()]; ok {
			return v, nil
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown command type: %v", req.GetType())
	}
}

func (sm *TestStateMachine) respondToPending(eventID uint64, resp *commandeventspb.CommandEventResponse) {
	sm.pendingMu.RLock()
	ch, ok := sm.pending[eventID]
	sm.pendingMu.RUnlock()

	if ok {
		select {
		case ch <- resp:
		default:
		}
	}
}

func (sm *TestStateMachine) RegisterPending(eventID uint64, ch chan *commandeventspb.CommandEventResponse) {
	sm.pendingMu.Lock()
	defer sm.pendingMu.Unlock()
	sm.pending[eventID] = ch
}

func (sm *TestStateMachine) UnregisterPending(eventID uint64) {
	sm.pendingMu.Lock()
	defer sm.pendingMu.Unlock()
	delete(sm.pending, eventID)
}

func (sm *TestStateMachine) Get(key string) ([]byte, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	v, ok := sm.data[key]
	return v, ok
}

func (sm *TestStateMachine) AppliedCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.applied)
}

func NewTestCluster(t *testing.T) *TestCluster {
	baseDir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	return &TestCluster{
		t:        t,
		nodes:    make(map[uint64]*TestNode),
		baseDir:  baseDir,
		nextPort: 20000 + (os.Getpid() % 10000),
	}
}

// AddNewNode starts a new node that's not yet part of the cluster
func (tc *TestCluster) AddNewNode(id uint64) (raftAddr, clientAddr string, err error) {
	raftPort := tc.allocPort()
	clientPort := tc.allocPort()
	raftAddr = fmt.Sprintf("127.0.0.1:%d", raftPort)
	clientAddr = fmt.Sprintf("127.0.0.1:%d", clientPort)

	// Gather existing peers
	raftPeers := make(map[uint64]string)
	clientPeers := make(map[uint64]string)

	tc.mu.RLock()
	for nid, n := range tc.nodes {
		raftPeers[nid] = n.RaftAddr
		clientPeers[nid] = n.ClientAddr
	}
	tc.mu.RUnlock()

	if err := tc.startNode(id, raftAddr, clientAddr, raftPeers, clientPeers); err != nil {
		return "", "", fmt.Errorf("failed to start node %d: %w", id, err)
	}

	// Update existing nodes with new peer info
	tc.mu.Lock()
	for _, n := range tc.nodes {
		if n.ID == id {
			continue
		}
		n.Node.AddPeer(id, raftAddr, clientAddr)
	}
	tc.mu.Unlock()

	return raftAddr, clientAddr, nil
}

func (tc *TestCluster) allocPort() int {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	port := tc.nextPort
	tc.nextPort++
	return port
}

func (tc *TestCluster) StartNodes(n int) error {
	raftPeers := make(map[uint64]string)
	clientPeers := make(map[uint64]string)
	nodeConfigs := make([]struct {
		id         uint64
		raftAddr   string
		clientAddr string
	}, n)

	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		raftPort := tc.allocPort()
		clientPort := tc.allocPort()
		raftAddr := fmt.Sprintf("127.0.0.1:%d", raftPort)
		clientAddr := fmt.Sprintf("127.0.0.1:%d", clientPort)

		nodeConfigs[i] = struct {
			id         uint64
			raftAddr   string
			clientAddr string
		}{id, raftAddr, clientAddr}

		raftPeers[id] = raftAddr
		clientPeers[id] = clientAddr
	}

	for _, cfg := range nodeConfigs {

		nodePeers := make(map[uint64]string)
		nodeClientPeers := make(map[uint64]string)
		for id, addr := range raftPeers {
			if id != cfg.id {
				nodePeers[id] = addr
			}
		}
		for id, addr := range clientPeers {
			if id != cfg.id {
				nodeClientPeers[id] = addr
			}
		}

		if err := tc.startNode(cfg.id, cfg.raftAddr, cfg.clientAddr, nodePeers, nodeClientPeers); err != nil {
			return fmt.Errorf("failed to start node %d: %w", cfg.id, err)
		}
	}

	return nil
}

func (tc *TestCluster) startNode(
	id uint64,
	raftAddr, clientAddr string,
	raftPeers, clientPeers map[uint64]string,
) error {
	nodeDir := filepath.Join(tc.baseDir, fmt.Sprintf("node-%d", id))
	if err := os.MkdirAll(nodeDir, 0o750); err != nil {
		return err
	}

	rc := &properties.RaftConfigProperties{
		NodeId:         id,
		StorageBaseDir: nodeDir,
		RaftPeers:      raftPeers,
		ClientPeers:    clientPeers,
		TickInterval:   10,
		Timeout:        5,
		SnapCount:      1000,
		BatchSize:      10,
		BatchMaxWait:   5,
		Etcd: properties.EtcdConfigProperties{
			ElectionTick:  10,
			HeartbeatTick: 1,
		},
		Wal: properties.WriteAheadLogProperties{
			NoSync: true,
		},
	}

	node, err := raft.NewNode(rc, raftAddr)
	if err != nil {
		return fmt.Errorf("NewNode: %w", err)
	}

	storageSvc := store.NewStorageService()
	sm := NewTestStateMachine()
	service := raft.NewService(node, storageSvc, rc, sm)
	batcher := raft.NewBatcher(service, sm, rc.BatchSize, time.Duration(rc.BatchMaxWait)*time.Millisecond)

	raftListener, err := net.Listen("tcp", raftAddr)
	if err != nil {
		node.Stop()
		return fmt.Errorf("listen raft: %w", err)
	}

	clientListener, err := net.Listen("tcp", clientAddr)
	if err != nil {
		raftListener.Close()
		node.Stop()
		return fmt.Errorf("listen client: %w", err)
	}

	raftServer := grpc.NewServer()
	clientServer := grpc.NewServer()

	rafttransportpb.RegisterRaftTransportServiceServer(raftServer, &testRaftServer{service: service})
	commandeventspb.RegisterCommandEventClientServiceServer(clientServer, &testCommandServer{
		service: service,
		batcher: batcher,
		sm:      sm,
	})

	go raftServer.Serve(raftListener)
	go clientServer.Serve(clientListener)

	testNode := &TestNode{
		ID:             id,
		Node:           node,
		Service:        service,
		Storage:        storageSvc,
		StateMachine:   sm,
		Batcher:        batcher,
		RaftServer:     raftServer,
		ClientServer:   clientServer,
		RaftAddr:       raftAddr,
		ClientAddr:     clientAddr,
		RaftListener:   raftListener,
		ClientListener: clientListener,
	}

	tc.mu.Lock()
	tc.nodes[id] = testNode
	tc.mu.Unlock()

	service.Start()

	return nil
}

type testRaftServer struct {
	rafttransportpb.UnimplementedRaftTransportServiceServer
	service *raft.Service
}

func (s *testRaftServer) SendRaftMessage(ctx context.Context, req *rafttransportpb.RaftMessage) (*rafttransportpb.RaftMessageResponse, error) {
	var msg raftpb.Message
	if err := msg.Unmarshal(req.Data); err != nil {
		return nil, err
	}
	if err := s.service.CallRaftStep(ctx, msg); err != nil {
		return nil, err
	}
	return &rafttransportpb.RaftMessageResponse{}, nil
}

func (s *testRaftServer) GetReadIndex(ctx context.Context, req *rafttransportpb.GetReadIndexRequest) (*rafttransportpb.GetReadIndexResponse, error) {
	idx, err := s.service.GetReadIndex(ctx)
	if err != nil {
		return nil, err
	}
	return &rafttransportpb.GetReadIndexResponse{ReadIndex: idx}, nil
}

type testCommandServer struct {
	commandeventspb.UnimplementedCommandEventClientServiceServer
	service *raft.Service
	batcher *raft.Batcher
	sm      *TestStateMachine
}

func (s *testCommandServer) ProcessCommandEvent(ctx context.Context, req *commandeventspb.CommandEventRequest) (*commandeventspb.CommandEventResponse, error) {
	if !s.service.IsLeader() {
		return s.service.ForwardToLeader(ctx, req)
	}

	respCh, err := s.batcher.Submit(ctx, req)
	if err != nil {
		return nil, err
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (tc *TestCluster) WaitForLeader(timeout time.Duration) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("timeout waiting for leader")
		case <-ticker.C:
			tc.mu.RLock()
			for _, node := range tc.nodes {
				if node.stopped {
					continue
				}

				if node.Service.IsLeader() {
					leaderID := node.ID
					tc.mu.RUnlock()
					return leaderID, nil
				}
			}
			tc.mu.RUnlock()
		}
	}
}

func (tc *TestCluster) WaitForNewLeader(excludeID uint64, timeout time.Duration) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("timeout waiting for new leader (excluding %d)", excludeID)
		case <-ticker.C:
			tc.mu.RLock()
			for _, node := range tc.nodes {
				if node.stopped {
					continue
				}
				if node.ID == excludeID {
					continue
				}
				if node.Service.IsLeader() {
					leaderID := node.ID
					tc.mu.RUnlock()
					return leaderID, nil
				}
			}
			tc.mu.RUnlock()
		}
	}
}

func (tc *TestCluster) GetLeader() *TestNode {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	for _, node := range tc.nodes {
		if node.stopped {
			continue
		}
		if node.Service.IsLeader() {
			return node
		}
	}
	return nil
}

func (tc *TestCluster) GetFollowers() []*TestNode {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	var followers []*TestNode
	for _, node := range tc.nodes {
		if node.stopped {
			continue
		}
		if !node.Service.IsLeader() {
			followers = append(followers, node)
		}
	}
	return followers
}

func (tc *TestCluster) GetNode(id uint64) *TestNode {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.nodes[id]
}

func (tc *TestCluster) StopNode(id uint64) error {
	tc.mu.Lock()
	node, ok := tc.nodes[id]
	tc.mu.Unlock()

	if !ok {
		return fmt.Errorf("node %d not found", id)
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.stopped {
		return nil
	}

	node.RaftServer.GracefulStop()
	node.ClientServer.GracefulStop()
	node.Service.Stop()
	node.stopped = true

	return nil
}

func (tc *TestCluster) RestartNode(id uint64) error {
	tc.mu.RLock()
	oldNode, ok := tc.nodes[id]
	tc.mu.RUnlock()

	if !ok {
		return fmt.Errorf("node %d not found", id)
	}

	oldNode.mu.Lock()
	raftAddr := oldNode.RaftAddr
	clientAddr := oldNode.ClientAddr
	oldNode.mu.Unlock()

	raftPeers := make(map[uint64]string)
	clientPeers := make(map[uint64]string)

	tc.mu.RLock()
	for nid, n := range tc.nodes {
		if nid != id {
			raftPeers[nid] = n.RaftAddr
			clientPeers[nid] = n.ClientAddr
		}
	}
	tc.mu.RUnlock()

	tc.mu.Lock()
	delete(tc.nodes, id)
	tc.mu.Unlock()

	return tc.startNode(id, raftAddr, clientAddr, raftPeers, clientPeers)
}

func (tc *TestCluster) ProposeValue(ctx context.Context, key, value string) error {
	leader := tc.GetLeader()
	if leader == nil {
		return fmt.Errorf("no leader available")
	}

	req := &commandeventspb.CommandEventRequest{
		EventId: uint64(time.Now().UnixNano()),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     key,
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: value},
		},
	}

	respCh, err := leader.Batcher.Submit(ctx, req)
	if err != nil {
		return err
	}

	select {
	case resp := <-respCh:
		if !resp.Success {
			return fmt.Errorf("proposal failed")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (tc *TestCluster) WaitForApplied(index uint64, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for applied index %d", index)
		case <-ticker.C:
			allApplied := true
			tc.mu.RLock()
			for _, node := range tc.nodes {
				if node.stopped {
					continue
				}
				if node.Service.LastApplied() < index {
					allApplied = false
					break
				}
			}
			tc.mu.RUnlock()
			if allApplied {
				return nil
			}
		}
	}
}

func (tc *TestCluster) WaitForConvergence(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for convergence")
		case <-ticker.C:
			tc.mu.RLock()
			var lastApplied uint64
			first := true
			converged := true

			for _, node := range tc.nodes {
				if node.stopped {
					continue
				}
				applied := node.Service.LastApplied()
				if first {
					lastApplied = applied
					first = false
				} else if applied != lastApplied {
					converged = false
					break
				}
			}
			tc.mu.RUnlock()

			if converged && !first {
				return nil
			}
		}
	}
}

func (tc *TestCluster) WaitForLeaderConvergence(timeout time.Duration) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("timeout waiting for leader convergence")
		case <-ticker.C:
			tc.mu.RLock()
			var leaderID uint64
			converged := true
			first := true

			for _, node := range tc.nodes {
				if node.stopped {
					continue
				}
				status := node.Node.Status()
				if status.Lead == 0 {
					converged = false
					break
				}
				if first {
					leaderID = status.Lead
					first = false
				} else if status.Lead != leaderID {
					converged = false
					break
				}
			}
			tc.mu.RUnlock()

			if converged && !first {
				return leaderID, nil
			}
		}
	}
}

func (tc *TestCluster) Cleanup() {
	tc.mu.Lock()
	nodes := make([]*TestNode, 0, len(tc.nodes))
	for _, n := range tc.nodes {
		nodes = append(nodes, n)
	}
	tc.mu.Unlock()

	for _, node := range nodes {
		node.mu.Lock()
		if !node.stopped {
			node.RaftServer.Stop()
			node.ClientServer.Stop()
			node.Service.Stop()
			node.stopped = true
		}
		node.mu.Unlock()
	}

	os.RemoveAll(tc.baseDir)
}

func (tc *TestCluster) NewClientConn(nodeID uint64) (*grpc.ClientConn, error) {
	node := tc.GetNode(nodeID)
	if node == nil {
		return nil, fmt.Errorf("node %d not found", nodeID)
	}

	return grpc.NewClient(node.ClientAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func (tc *TestCluster) DeleteNodeWAL(id uint64) error {
	nodeDir := filepath.Join(tc.baseDir, fmt.Sprintf("node-%d", id))
	walDir := filepath.Join(nodeDir, "wal")

	if err := os.RemoveAll(walDir); err != nil {
		return fmt.Errorf("failed to remove WAL dir: %w", err)
	}

	return nil
}

func (tc *TestCluster) DeleteNodeSnapshots(id uint64) error {
	nodeDir := filepath.Join(tc.baseDir, fmt.Sprintf("node-%d", id))
	snapDir := filepath.Join(nodeDir, "snapshot")

	if err := os.RemoveAll(snapDir); err != nil {
		return fmt.Errorf("failed to remove snapshot dir: %w", err)
	}

	return nil
}

func (tc *TestCluster) DeleteNodeData(id uint64) error {
	if err := tc.DeleteNodeWAL(id); err != nil {
		return err
	}
	return tc.DeleteNodeSnapshots(id)
}

func (tc *TestCluster) ClusterMarkerExists(id uint64) bool {
	nodeDir := filepath.Join(tc.baseDir, fmt.Sprintf("node-%d", id))
	markerPath := filepath.Join(nodeDir, "cluster.id")
	_, err := os.Stat(markerPath)
	return err == nil
}

func (tc *TestCluster) WaitForNodeCatchUp(nodeID uint64, targetIndex uint64, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			node := tc.GetNode(nodeID)
			if node != nil {
				return fmt.Errorf("timeout: node %d at index %d, target %d",
					nodeID, node.Service.LastApplied(), targetIndex)
			}
			return fmt.Errorf("timeout waiting for node %d to catch up", nodeID)
		case <-ticker.C:
			node := tc.GetNode(nodeID)
			if node == nil || node.stopped {
				continue
			}
			if node.Service.LastApplied() >= targetIndex {
				return nil
			}
		}
	}
}

func (tc *TestCluster) GetClusterAppliedIndex() uint64 {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	var maxApplied uint64
	for _, node := range tc.nodes {
		if node.stopped {
			continue
		}
		if applied := node.Service.LastApplied(); applied > maxApplied {
			maxApplied = applied
		}
	}
	return maxApplied
}
