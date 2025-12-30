package helper

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"pulsardb/internal/logging"
	"pulsardb/internal/transport/handler"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pulsardb/internal/command"
	"pulsardb/internal/configuration"
	"pulsardb/internal/raft"
	"pulsardb/internal/statemachine"
	"pulsardb/internal/storage"
	commandeventspb "pulsardb/internal/transport/gen/commandevents"
	rafttransportpb "pulsardb/internal/transport/gen/raft"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var nextEventID atomic.Uint64
var nextPortBase = 30000 + rand.IntN(os.Getpid()%10000)
var portMu sync.Mutex

func NewEventID() uint64 {
	return nextEventID.Add(1)
}

func allocPort() int {
	portMu.Lock()
	defer portMu.Unlock()
	port := nextPortBase
	nextPortBase++
	return port
}

type TestClusterConfig struct {
	TickInterval time.Duration
	ElectionTick int
	BatchSize    int
	BatchWait    time.Duration
}

var DefaultConfig = TestClusterConfig{
	TickInterval: 100 * time.Millisecond,
	ElectionTick: 10,
	BatchSize:    10,
	BatchWait:    2,
}

type Cluster struct {
	t       *testing.T
	config  TestClusterConfig
	nodes   map[uint64]*TestNode
	BaseDir string
	mu      sync.RWMutex
}

type TestNode struct {
	ID             uint64
	RaftNode       *raft.Node
	RaftService    *raft.Service
	CmdService     *command.Service
	StorageService *storage.Service
	StateMachine   *statemachine.StateMachine
	RaftServer     *grpc.Server
	ClientServer   *grpc.Server
	RaftAddr       string
	ClientAddr     string
	RaftLis        net.Listener
	ClientLis      net.Listener

	stopped bool
	mu      sync.Mutex
}

var initLogging sync.Once

func NewCluster(t *testing.T, cfg *TestClusterConfig, logLevel string) *Cluster {
	initLogging.Do(func() {
		logging.Init(logLevel)
	})

	baseDir, err := os.MkdirTemp("", "pulsar-integration-*")
	require.NoError(t, err)

	actualCfg := DefaultConfig
	if cfg != nil {
		actualCfg = *cfg
	}

	c := &Cluster{
		t:       t,
		config:  actualCfg,
		nodes:   make(map[uint64]*TestNode),
		BaseDir: baseDir,
	}

	t.Cleanup(c.cleanup)

	return c
}

func (c *Cluster) StartNodes(n int, timeout uint64) {
	raftAddrs := make(map[uint64]string)
	clientAddrs := make(map[uint64]string)

	nodeCfgs := make([]struct {
		id         uint64
		raftAddr   string
		clientAddr string
	}, n)

	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		nodeCfgs[i] = struct {
			id         uint64
			raftAddr   string
			clientAddr string
		}{id, fmt.Sprintf("127.0.0.1:%d", allocPort()), fmt.Sprintf("127.0.0.1:%d", allocPort())}

		raftAddrs[id] = nodeCfgs[i].raftAddr
		clientAddrs[id] = nodeCfgs[i].clientAddr
	}

	for _, cfg := range nodeCfgs {

		peers := make(map[uint64]string)
		clientPeers := make(map[uint64]string)
		for id, addr := range raftAddrs {
			if id != cfg.id {
				peers[id] = addr
			}
		}
		for id, addr := range clientAddrs {
			if id != cfg.id {
				clientPeers[id] = addr
			}
		}

		err := c.startNode(cfg.id, cfg.raftAddr, cfg.clientAddr, peers, clientPeers)
		require.NoError(c.t, err, "failed to start node %d", cfg.id)
	}
	_, err := c.WaitForLeader(time.Duration(timeout) * time.Second)
	if err != nil {
		return
	}
}

func (c *Cluster) startNode(
	id uint64,
	raftAddr, clientAddr string,
	raftPeers, clientPeers map[uint64]string,
) error {
	nodeDir := filepath.Join(c.BaseDir, fmt.Sprintf("node-%d", id))
	if err := os.MkdirAll(nodeDir, 0o750); err != nil {
		return err
	}

	rc := &configuration.RaftConfigurationProperties{
		NodeID:        id,
		StorageDir:    nodeDir,
		RaftPeers:     raftPeers,
		ClientPeers:   clientPeers,
		TickInterval:  c.config.TickInterval,
		Timeout:       5,
		SnapCount:     1000,
		BatchSize:     c.config.BatchSize,
		BatchMaxWait:  c.config.BatchWait,
		SendQueueSize: 256,
		Etcd: configuration.EtcdConfigProperties{
			ElectionTick:  c.config.ElectionTick,
			HeartbeatTick: 1,
		},
		Wal: configuration.WriteAheadLogProperties{
			NoSync: true,
		},
	}

	raftNode, err := raft.NewNode(rc, raftAddr)
	if err != nil {
		return fmt.Errorf("new raft node: %w", err)
	}

	storageSvc := storage.NewService()
	sm := statemachine.New(storageSvc)
	raftSvc := raft.NewService(raftNode, storageSvc, sm, rc)

	batchCfg := command.BatchConfig{
		MaxSize: rc.BatchSize,
		MaxWait: rc.BatchTimeout(),
	}

	cmdSvc := command.NewService(storageSvc, raftSvc, batchCfg)

	sm.OnApply(cmdSvc.HandleApplied)

	raftLis, err := net.Listen("tcp", raftAddr)
	if err != nil {
		raftNode.Stop()
		return fmt.Errorf("listen raft: %w", err)
	}

	clientLis, err := net.Listen("tcp", clientAddr)
	if err != nil {
		raftLis.Close()
		raftNode.Stop()
		return fmt.Errorf("listen client: %w", err)
	}

	raftServer := grpc.NewServer()
	clientServer := grpc.NewServer()

	rafttransportpb.RegisterRaftTransportServiceServer(
		raftServer,
		&testRaftServer{raftSvc: raftSvc},
	)

	commandeventspb.RegisterCommandEventClientServiceServer(
		clientServer,
		&testClientServer{cmdSvc: cmdSvc},
	)

	go raftServer.Serve(raftLis)
	go clientServer.Serve(clientLis)

	node := &TestNode{
		ID:             id,
		RaftNode:       raftNode,
		RaftService:    raftSvc,
		CmdService:     cmdSvc,
		StorageService: storageSvc,
		StateMachine:   sm,
		RaftServer:     raftServer,
		ClientServer:   clientServer,
		RaftAddr:       raftAddr,
		ClientAddr:     clientAddr,
		RaftLis:        raftLis,
		ClientLis:      clientLis,
	}

	c.mu.Lock()
	c.nodes[id] = node
	c.mu.Unlock()

	raftSvc.Start()

	return nil
}

func (c *Cluster) cleanup() {
	c.mu.Lock()
	nodes := make([]*TestNode, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.mu.Unlock()

	for _, node := range nodes {
		node.mu.Lock()
		if !node.stopped {
			node.RaftServer.Stop()
			node.ClientServer.Stop()
			node.CmdService.Stop()
			node.RaftService.Stop()
			node.stopped = true
		}
		node.mu.Unlock()
	}

	os.RemoveAll(c.BaseDir)
}

func (c *Cluster) RestartNode(id uint64) error {
	c.mu.RLock()
	oldNode, ok := c.nodes[id]
	c.mu.RUnlock()

	if !ok {
		return fmt.Errorf("node %d not found", id)
	}

	oldNode.mu.Lock()
	raftAddr := oldNode.RaftAddr
	clientAddr := oldNode.ClientAddr

	if !oldNode.stopped {
		oldNode.RaftServer.GracefulStop()
		oldNode.ClientServer.GracefulStop()
		oldNode.CmdService.Stop()
		oldNode.RaftService.Stop()
		oldNode.stopped = true
	}
	oldNode.mu.Unlock()

	raftPeers := make(map[uint64]string)
	clientPeers := make(map[uint64]string)

	c.mu.RLock()
	for nid, n := range c.nodes {
		if nid != id {
			raftPeers[nid] = n.RaftAddr
			clientPeers[nid] = n.ClientAddr
		}
	}
	c.mu.RUnlock()

	c.mu.Lock()
	delete(c.nodes, id)
	c.mu.Unlock()

	return c.startNode(id, raftAddr, clientAddr, raftPeers, clientPeers)
}

func (c *Cluster) StopNode(id uint64) error {
	c.mu.Lock()
	node, ok := c.nodes[id]
	c.mu.Unlock()

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
	node.CmdService.Stop()
	node.RaftService.Stop()
	node.stopped = true

	return nil
}

func (c *Cluster) GetLeader() *TestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, node := range c.nodes {
		node.mu.Lock()
		stopped := node.stopped
		node.mu.Unlock()

		if stopped {
			continue
		}
		if node.RaftService.IsLeader() {
			return node
		}
	}
	return nil
}

func (c *Cluster) GetNode(id uint64) *TestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodes[id]
}

func (c *Cluster) WaitForLeader(timeout time.Duration) (uint64, error) {
	return c.waitForLeaderInternal(0, timeout)
}

func (c *Cluster) WaitForNewLeader(excludeID uint64, timeout time.Duration) (uint64, error) {
	return c.waitForLeaderInternal(excludeID, timeout)
}

func (c *Cluster) waitForLeaderInternal(excludeID uint64, timeout time.Duration) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("timeout waiting for leader")
		case <-ticker.C:
			leader := c.GetLeader()
			if leader != nil && leader.ID != excludeID {
				return leader.ID, nil
			}
		}
	}
}

func (c *Cluster) WaitForConvergence(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for convergence")
		case <-ticker.C:
			c.mu.RLock()
			var lastApplied uint64
			first := true
			converged := true

			for _, node := range c.nodes {
				node.mu.Lock()
				stopped := node.stopped
				node.mu.Unlock()

				if stopped {
					continue
				}

				applied := node.RaftService.LastApplied()
				if first {
					lastApplied = applied
					first = false
				} else if applied != lastApplied {
					converged = false
					break
				}
			}
			c.mu.RUnlock()

			if converged && !first {
				return nil
			}
		}
	}
}

func (c *Cluster) WaitForLeaderConvergence(timeout time.Duration) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("timeout waiting for leader convergence")
		case <-ticker.C:
			c.mu.RLock()
			var leaderID uint64
			converged := true
			first := true

			for _, node := range c.nodes {
				node.mu.Lock()
				stopped := node.stopped
				node.mu.Unlock()

				if stopped {
					continue
				}

				status := node.RaftNode.Status()
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
			c.mu.RUnlock()

			if converged && !first {
				return leaderID, nil
			}
		}
	}
}

func (c *Cluster) Set(ctx context.Context, key, value string) error {
	req := &commandeventspb.CommandEventRequest{
		EventId: NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     key,
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: value},
		},
	}

	resp, err := c.SendToLeader(ctx, req)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("set failed: %s", resp.GetError().GetMessage())
	}
	return nil
}

func (c *Cluster) Get(ctx context.Context, key string) (string, bool, error) {
	req := &commandeventspb.CommandEventRequest{
		EventId: NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     key,
	}

	resp, err := c.SendToLeader(ctx, req)
	if err != nil {
		return "", false, err
	}

	if resp.GetError() != nil && resp.GetError().GetCode() == commandeventspb.ErrorCode_KEY_NOT_FOUND {
		return "", false, nil
	}

	if !resp.Success {
		return "", false, fmt.Errorf("get failed: %s", resp.GetError().GetMessage())
	}
	return resp.GetValue().GetStringValue(), true, nil
}

func (c *Cluster) Delete(ctx context.Context, key string) error {
	req := &commandeventspb.CommandEventRequest{
		EventId: NewEventID(),
		Type:    commandeventspb.CommandEventType_DELETE,
		Key:     key,
	}

	resp, err := c.SendToLeader(ctx, req)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("delete failed: %s", resp.GetError().GetMessage())
	}
	return nil
}

func (c *Cluster) SendToLeader(ctx context.Context, req *commandeventspb.CommandEventRequest) (*commandeventspb.CommandEventResponse, error) {

	for i := 0; i < 5; i++ {
		leader := c.GetLeader()
		if leader != nil {
			return leader.CmdService.ProcessCommand(ctx, req)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, fmt.Errorf("no leader available")
}

func (c *Cluster) VerifyConsistency(key string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var expected any
	first := true

	for _, node := range c.nodes {
		node.mu.Lock()
		stopped := node.stopped
		node.mu.Unlock()

		if stopped {
			continue
		}

		val, exists := node.StorageService.Get(key)
		if first {
			expected = val
			first = false
			if !exists {
				expected = nil
			}
		} else {
			if exists {
				if !reflect.DeepEqual(val, expected) {
					return false, fmt.Errorf("consistency mismatch on node %d: got %v, want %v", node.ID, val, expected)
				}
			} else if expected != nil {
				return false, fmt.Errorf("missing value on node %d, expected %v", node.ID, expected)
			}
		}
	}

	return true, nil
}

type testRaftServer struct {
	rafttransportpb.UnimplementedRaftTransportServiceServer
	raftSvc *raft.Service
}

func (s *testRaftServer) SendRaftMessage(ctx context.Context, req *rafttransportpb.RaftMessage) (*rafttransportpb.RaftMessageResponse, error) {
	var msg raftpb.Message
	if err := msg.Unmarshal(req.Data); err != nil {
		return nil, err
	}
	if err := s.raftSvc.CallRaftStep(ctx, msg); err != nil {
		return nil, err
	}
	return &rafttransportpb.RaftMessageResponse{}, nil
}

func (s *testRaftServer) GetReadIndex(ctx context.Context, req *rafttransportpb.GetReadIndexRequest) (*rafttransportpb.GetReadIndexResponse, error) {
	idx, err := s.raftSvc.GetReadIndex(ctx)
	if err != nil {
		return nil, err
	}
	return &rafttransportpb.GetReadIndexResponse{ReadIndex: idx}, nil
}

type testClientServer struct {
	commandeventspb.UnimplementedCommandEventClientServiceServer
	cmdSvc *command.Service
}

func (s *testClientServer) ProcessCommandEvent(ctx context.Context, req *commandeventspb.CommandEventRequest) (*commandeventspb.CommandEventResponse, error) {
	resp, err := s.cmdSvc.ProcessCommand(ctx, req)
	if err != nil {
		return nil, handler.ToGRPCError(err)
	}
	return resp, nil
}

func getFreePorts(t *testing.T, n int) []string {
	t.Helper()
	ports := make([]string, n)
	listeners := make([]net.Listener, n)

	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = l
		_, ports[i], _ = net.SplitHostPort(l.Addr().String())
	}

	for _, l := range listeners {
		l.Close()
	}

	return ports
}

func RequireSuccess(t *testing.T, resp *commandeventspb.CommandEventResponse, err error) {
	t.Helper()
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.GetSuccess(), "expected success, got error: %v", resp.GetError())
}

func requireFailure(t *testing.T, resp *commandeventspb.CommandEventResponse, err error) {
	t.Helper()
	require.Error(t, err)
	require.Nil(t, resp)
}

func processCmd(t *testing.T, cmdSvc *command.Service, req *commandeventspb.CommandEventRequest) (*commandeventspb.CommandEventResponse, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return cmdSvc.ProcessCommand(ctx, req)
}

func (c *Cluster) SetValue(ctx context.Context, key string, value *commandeventspb.CommandEventValue) (*commandeventspb.CommandEventResponse, error) {
	req := &commandeventspb.CommandEventRequest{
		EventId: NewEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     key,
		Value:   value,
	}
	return c.SendToLeader(ctx, req)
}

func (c *Cluster) GetValue(ctx context.Context, key string) (*commandeventspb.CommandEventResponse, error) {
	req := &commandeventspb.CommandEventRequest{
		EventId: NewEventID(),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     key,
	}
	return c.SendToLeader(ctx, req)
}

func (c *Cluster) GetClient(t *testing.T) (commandeventspb.CommandEventClientServiceClient, func()) {
	t.Helper()

	_, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	leader := c.GetLeader()
	require.NotNil(t, leader)

	conn, err := grpc.NewClient(leader.ClientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client := commandeventspb.NewCommandEventClientServiceClient(conn)
	cleanup := func() {
		conn.Close()
	}

	return client, cleanup
}

func (c *Cluster) GetFollowers() []*TestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var followers []*TestNode
	for _, node := range c.nodes {
		if !node.stopped && !node.RaftService.IsLeader() {
			followers = append(followers, node)
		}
	}
	return followers
}

func (c *Cluster) WaitForApplied(index uint64, timeout time.Duration) error {
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
			c.mu.RLock()
			for _, node := range c.nodes {
				if !node.stopped && node.RaftService.LastApplied() < index {
					allApplied = false
					break
				}
			}
			c.mu.RUnlock()
			if allApplied {
				return nil
			}
		}
	}
}

func (c *Cluster) GetClusterAppliedIndex() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var maxApplied uint64
	for _, node := range c.nodes {
		if !node.stopped {
			if applied := node.RaftService.LastApplied(); applied > maxApplied {
				maxApplied = applied
			}
		}
	}
	return maxApplied
}

func (c *Cluster) DeleteNodeData(id uint64) error {
	nodeDir := filepath.Join(c.BaseDir, fmt.Sprintf("node-%d", id))
	for _, subdir := range []string{"wal", "snapshot"} {
		if err := os.RemoveAll(filepath.Join(nodeDir, subdir)); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) AddNewNode(id uint64) (raftAddr, clientAddr string, err error) {
	raftAddr = fmt.Sprintf("127.0.0.1:%d", allocPort())
	clientAddr = fmt.Sprintf("127.0.0.1:%d", allocPort())

	raftPeers := make(map[uint64]string)
	clientPeers := make(map[uint64]string)

	c.mu.RLock()
	for nid, n := range c.nodes {
		raftPeers[nid] = n.RaftAddr
		clientPeers[nid] = n.ClientAddr
	}
	c.mu.RUnlock()

	if err := c.startNode(id, raftAddr, clientAddr, raftPeers, clientPeers); err != nil {
		return "", "", err
	}

	return raftAddr, clientAddr, nil
}

func AsString(v any) (string, bool) {
	if v == nil {
		return "", false
	}

	switch x := v.(type) {
	case string:
		return x, true
	case []byte:
		return string(x), true
	case fmt.Stringer:
		return x.String(), true
	}

	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return "", false
	}

	if rv.Kind() == reflect.String {
		return rv.String(), true
	}
	if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
		b := make([]byte, rv.Len())
		reflect.Copy(reflect.ValueOf(b), rv)
		return string(b), true
	}
	if rv.Kind() == reflect.Pointer && !rv.IsNil() {
		e := rv.Elem()
		if e.IsValid() && e.Kind() == reflect.Struct {

			f := e.FieldByName("StringValue")
			if f.IsValid() && f.Kind() == reflect.String {
				return f.String(), true
			}
		}
	}

	return "", false
}
