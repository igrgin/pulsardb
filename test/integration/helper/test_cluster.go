package helper

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"pulsardb/convert"
	"pulsardb/internal/command"
	"pulsardb/internal/configuration"
	"pulsardb/internal/logging"
	"pulsardb/internal/metrics"
	"pulsardb/internal/raft"
	"pulsardb/internal/raft/coordinator"
	"pulsardb/internal/statemachine"
	"pulsardb/internal/storage"
	"pulsardb/internal/transport"
	command2 "pulsardb/internal/transport/gen/command"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var nextEventID atomic.Uint64
var nextPortBase atomic.Uint64

func NewEventID() uint64 {
	return nextEventID.Add(1)
}

func allocPort() int {
	if nextPortBase.Load() == 0 {
		nextPortBase.Add(uint64(30000 + rand.IntN(os.Getpid()%10000)))
	}
	port := +nextPortBase.Load()
	nextPortBase.Add(1)
	return int(port)
}

type TestClusterConfig struct {
	TickInterval           time.Duration
	ElectionTick           int
	BatchSize              int
	BatchWait              time.Duration
	PromotionThreshold     uint64
	PromotionCheckInterval time.Duration
	CleanupTickInterval    time.Duration
}

var DefaultConfig = TestClusterConfig{
	TickInterval:           100 * time.Millisecond,
	ElectionTick:           10,
	BatchSize:              10,
	BatchWait:              2 * time.Millisecond,
	PromotionThreshold:     5,
	PromotionCheckInterval: 500 * time.Millisecond,
	CleanupTickInterval:    100 * time.Millisecond,
}

type Cluster struct {
	t       *testing.T
	config  TestClusterConfig
	nodes   map[uint64]*TestNode
	BaseDir string
	mu      sync.RWMutex
	connMu  sync.Mutex
	conns   map[uint64]*grpc.ClientConn
	clients map[uint64]command2.CommandEventClientServiceClient
}

type TestNode struct {
	ID             uint64
	RaftNode       *raft.Node
	Coordinator    *coordinator.Coordinator
	CmdService     *command.Processor
	StorageService *storage.Service
	StateMachine   *statemachine.StateMachine
	RaftAddr       string
	ClientAddr     string
	stopped        bool
	mu             sync.Mutex
	IsLeaseBased   bool

	Transport *transport.Server
}

var initOnce sync.Once

func NewCluster(t *testing.T, cfg *TestClusterConfig, logLevel string) *Cluster {
	initOnce.Do(func() {
		logging.Init(logLevel)
		metrics.Init(0)
	})

	baseDir := t.TempDir()

	actualCfg := DefaultConfig
	if cfg != nil {
		actualCfg = *cfg
	}

	c := &Cluster{
		t:       t,
		config:  actualCfg,
		nodes:   make(map[uint64]*TestNode),
		conns:   make(map[uint64]*grpc.ClientConn),
		clients: make(map[uint64]command2.CommandEventClientServiceClient),
		BaseDir: baseDir,
	}

	t.Cleanup(c.cleanup)

	return c
}

func (c *Cluster) StartNodes(n int, timeout uint64, isLeasedBased bool) {
	raftAddrs := make(map[uint64]string)

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
	}

	for _, cfg := range nodeCfgs {
		peers := make(map[uint64]string)
		for id, addr := range raftAddrs {
			if id != cfg.id {
				peers[id] = addr
			}
		}

		err := c.StartNode(cfg.id, cfg.raftAddr, cfg.clientAddr, peers, false, isLeasedBased)
		require.NoError(c.t, err, "failed to start node %d", cfg.id)
	}
	_, err := c.WaitForLeader(time.Duration(timeout) * time.Second)
	require.NoError(c.t, err, "timeout waiting for leader")
}

func (c *Cluster) StartNode(
	id uint64,
	raftAddr, clientAddr string,
	raftPeers map[uint64]string,
	join, isLeaseBased bool,
) error {
	nodeDir := filepath.Join(c.BaseDir, fmt.Sprintf("node-%d", id))
	if err := os.MkdirAll(nodeDir, 0o750); err != nil {
		return err
	}

	cc := &configuration.CommandConfigurationProperties{
		BatchSize:           c.config.BatchSize,
		BatchMaxWait:        c.config.BatchWait,
		CleanupTickInterval: c.config.CleanupTickInterval,
	}

	rc := &configuration.RaftConfigurationProperties{
		NodeID:                 id,
		StorageDir:             nodeDir,
		RaftPeers:              raftPeers,
		TickInterval:           c.config.TickInterval,
		Timeout:                5 * time.Second,
		SnapCount:              1000,
		SendQueueSize:          256,
		Join:                   join,
		PromotionThreshold:     c.config.PromotionThreshold,
		PromotionCheckInterval: c.config.PromotionCheckInterval,
		Etcd: configuration.EtcdConfigProperties{
			ElectionTick:  c.config.ElectionTick,
			HeartbeatTick: 1,
		},
		Wal: configuration.WriteAheadLogProperties{
			NoSync: true,
		},
		ServiceDrainTimeout: 1 * time.Second,
		NodeDrainTimeout:    1 * time.Second,
		LeaseBasedRead:      isLeaseBased,
		CheckQuorum:         true,
		PreVote:             true,
	}

	storageSvc := storage.NewService()
	sm := statemachine.New(storageSvc)

	raftNode, err := raft.NewNode(rc, raftAddr)
	if err != nil {
		return fmt.Errorf("new raft node: %w", err)
	}

	batchCfg := command.BatchConfig{
		MaxSize:             cc.BatchSize,
		MaxWait:             cc.BatchMaxWait,
		CleanupTickInterval: cc.CleanupTickInterval,
	}

	raftHost, raftPort, err := net.SplitHostPort(raftAddr)
	if err != nil {
		raftNode.Stop()
		return fmt.Errorf("split raft addr %q: %w", raftAddr, err)
	}
	clientHost, clientPort, err := net.SplitHostPort(clientAddr)
	if err != nil {
		raftNode.Stop()
		return fmt.Errorf("split client addr %q: %w", clientAddr, err)
	}
	if raftHost != clientHost {
		raftNode.Stop()
		return fmt.Errorf("mismatched hosts: raft host=%q client host=%q", raftHost, clientHost)
	}

	tc := &configuration.TransportConfigurationProperties{
		Network:    "tcp",
		Address:    clientHost,
		RaftPort:   raftPort,
		ClientPort: clientPort,

		RaftTransportConfig: configuration.RaftTransportConfigProperties{
			MaxConcurrentStreams: 100,
			NumStreamWorkers:     4,
		},
		ClientTransportConfig: configuration.ClientTransportConfigProperties{
			MaxConcurrentStreams: 100,
			NumStreamWorkers:     4,
		},
	}

	raftNodeAdapter := coordinator.NewRaftNodeAdapter(raftNode)
	transportAdapter := coordinator.NewTransportAdapter(raftNode)

	Coordinator := coordinator.New(
		raftNodeAdapter,
		transportAdapter,
		storageSvc,
		sm,
		coordinator.NewConfigFromProperties(rc, raftAddr, clientAddr),
	)
	cmdSvc := command.NewProcessor(storageSvc, Coordinator, batchCfg)
	sm.OnApply(cmdSvc.HandleApplied)

	ts := transport.NewServer(tc, cmdSvc, Coordinator)

	if err := ts.StartRaft(); err != nil {
		raftNode.Stop()
		return fmt.Errorf("start raft transport: %w", err)
	}
	if err := ts.StartClient(); err != nil {
		ts.Stop()
		raftNode.Stop()
		return fmt.Errorf("start client transport: %w", err)
	}

	Coordinator.Start()

	node := &TestNode{
		ID:             id,
		RaftNode:       raftNode,
		Coordinator:    Coordinator,
		CmdService:     cmdSvc,
		StorageService: storageSvc,
		StateMachine:   sm,
		RaftAddr:       raftAddr,
		ClientAddr:     clientAddr,
		Transport:      ts,
		IsLeaseBased:   rc.LeaseBasedRead,
	}

	c.mu.Lock()
	c.nodes[id] = node
	c.mu.Unlock()

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
			node.Transport.Stop()
			node.CmdService.Stop()
			node.Coordinator.Stop()
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
		oldNode.Transport.Stop()
		oldNode.CmdService.Stop()
		oldNode.Coordinator.Stop()
		oldNode.stopped = true
	}
	oldNode.mu.Unlock()

	raftPeers := make(map[uint64]string)

	c.mu.RLock()
	for nid, n := range c.nodes {
		if nid != id {
			raftPeers[nid] = n.RaftAddr
		}
	}
	c.mu.RUnlock()

	c.mu.Lock()
	delete(c.nodes, id)
	c.mu.Unlock()

	return c.StartNode(id, raftAddr, clientAddr, raftPeers, false, false)
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

	node.Transport.Stop()
	node.CmdService.Stop()
	node.Coordinator.Stop()
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
		if node.Coordinator.IsLeader() {
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

func (c *Cluster) GetNodes() *map[uint64]*TestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return &c.nodes
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

				applied := node.Coordinator.LastApplied()
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
	vproto := convert.ToCommandProto(value)

	resp, err := c.SetValue(ctx, key, vproto)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("set failed: %s", resp.GetError().GetMessage())
	}
	return nil
}

func (c *Cluster) Get(ctx context.Context, key string) (string, bool, error) {
	resp, err := c.GetValue(ctx, key)
	if err != nil {
		return "", false, err
	}

	if resp.GetError() != nil && resp.GetError().GetCode() == command2.ErrorCode_KEY_NOT_FOUND {
		return "", false, nil
	}
	if !resp.Success {
		return "", false, fmt.Errorf("get failed: %s", resp.GetError().GetMessage())
	}

	v := convert.FromCommandProto(resp.GetValue())

	s, ok := AsString(v)
	if !ok {
		return "", false, fmt.Errorf("get returned non-string value type %T", v)
	}
	return s, true, nil
}

func (c *Cluster) Delete(ctx context.Context, key string) error {
	req := &command2.CommandEventRequest{
		EventId: NewEventID(),
		Type:    command2.CommandEventType_DELETE,
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

func RequireSuccess(t *testing.T, resp *command2.CommandEventResponse, err error) {
	t.Helper()
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.GetSuccess(), "expected success, got error: %v", resp.GetError())
}

func (c *Cluster) SetValue(ctx context.Context, key string, value *command2.CommandEventValue) (*command2.CommandEventResponse, error) {
	req := &command2.CommandEventRequest{
		EventId: NewEventID(),
		Type:    command2.CommandEventType_SET,
		Key:     key,
		Value:   value,
	}
	return c.SendToLeader(ctx, req)
}

func (c *Cluster) GetValue(ctx context.Context, key string) (*command2.CommandEventResponse, error) {
	req := &command2.CommandEventRequest{
		EventId: NewEventID(),
		Type:    command2.CommandEventType_GET,
		Key:     key,
	}
	return c.SendToLeader(ctx, req)
}

func (c *Cluster) GetClient(t *testing.T) (command2.CommandEventClientServiceClient, func()) {
	t.Helper()

	_, err := c.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	leader := c.GetLeader()
	require.NotNil(t, leader)

	conn, err := grpc.NewClient(leader.ClientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client := command2.NewCommandEventClientServiceClient(conn)
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
		if !node.stopped && !node.Coordinator.IsLeader() {
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

	slog.Debug("starting wait", "timeout", timeout, "index", index)

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for applied index %d", index)
		case <-ticker.C:
			allApplied := true
			c.mu.RLock()
			for _, node := range c.nodes {
				if !node.stopped && node.Coordinator.LastApplied() < index {
					slog.Debug("not yet", "nodeId", node.ID, "lastindex", node.Coordinator.NodeID(), "isLeader", node.Coordinator.IsLeader())
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
			if applied := node.Coordinator.LastApplied(); applied > maxApplied {
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

func (c *Cluster) AddNewNode(id uint64) (string, string, error) {
	raftAddr := fmt.Sprintf("127.0.0.1:%d", allocPort())
	clientAddr := fmt.Sprintf("127.0.0.1:%d", allocPort())

	raftPeers := make(map[uint64]string)

	c.mu.RLock()
	for nid, n := range c.nodes {
		raftPeers[nid] = n.RaftAddr
	}
	c.mu.RUnlock()

	if err := c.StartNode(id, raftAddr, clientAddr, raftPeers, true, false); err != nil {
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

func (c *Cluster) RestartNodeAfterDataLoss(id uint64) error {
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
		oldNode.Transport.Stop()
		oldNode.CmdService.Stop()
		oldNode.Coordinator.Stop()
		oldNode.stopped = true
	}
	oldNode.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	leader := c.GetLeader()
	if leader != nil {
		if err := leader.Coordinator.ProposeRemoveNode(ctx, id); err != nil {
			slog.Warn("failed to remove node before recovery", "node_id", id, "error", err)
		}

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				slog.Warn("timeout waiting for node removal", "node_id", id)
				goto restart
			case <-ticker.C:
				cs := leader.RaftNode.ConfState()
				found := false
				for _, v := range cs.Voters {
					if v == id {
						found = true
						break
					}
				}
				for _, l := range cs.Learners {
					if l == id {
						found = true
						break
					}
				}
				if !found {
					slog.Info("node removed", "node_id", id)
					goto restart
				}
			}
		}
	}

restart:
	raftPeers := make(map[uint64]string)
	c.mu.RLock()
	for nid, n := range c.nodes {
		if nid != id {
			raftPeers[nid] = n.RaftAddr
		}
	}
	c.mu.RUnlock()

	c.mu.Lock()
	delete(c.nodes, id)
	c.mu.Unlock()

	return c.StartNode(id, raftAddr, clientAddr, raftPeers, true, false)
}

func (c *Cluster) SendToLeader(ctx context.Context, req *command2.CommandEventRequest) (*command2.CommandEventResponse, error) {
	var targetID uint64

	for i := 0; i < 10; i++ {
		if targetID == 0 {
			leader := c.GetLeader()
			if leader != nil {
				targetID = leader.ID
			}
		}
		if targetID == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		client, err := c.getGRPCClientForNode(ctx, targetID)
		if err != nil {
			targetID = 0
			time.Sleep(100 * time.Millisecond)
			continue
		}

		resp, err := client.ProcessCommandEvent(ctx, req)
		if err == nil {
			return resp, nil
		}

		st, ok := status.FromError(err)
		if ok && st.Code() == codes.FailedPrecondition {
			for _, d := range st.Details() {
				if info, ok := d.(*command2.NotLeaderDetails); ok && info.GetLeaderId() != 0 {
					targetID = info.GetLeaderId()
					goto nextAttempt
				}
			}
		}

		if ok && (st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded) {
			targetID = 0
			time.Sleep(100 * time.Millisecond)
			continue
		}

		return nil, err

	nextAttempt:
		continue
	}

	return nil, fmt.Errorf("no leader available")
}

func (c *Cluster) SendToNode(ctx context.Context, nodeID uint64, req *command2.CommandEventRequest) (*command2.CommandEventResponse, error) {
	client, err := c.getGRPCClientForNode(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	return client.ProcessCommandEvent(ctx, req)
}

func (c *Cluster) getGRPCClientForNode(
	ctx context.Context,
	nodeID uint64,
) (command2.CommandEventClientServiceClient, error) {
	if nodeID == 0 {
		return nil, fmt.Errorf("invalid node id")
	}

	c.connMu.Lock()
	if client, ok := c.clients[nodeID]; ok && client != nil {
		c.connMu.Unlock()
		return client, nil
	}
	c.connMu.Unlock()

	n := c.GetNode(nodeID)
	if n == nil {
		return nil, fmt.Errorf("node %d not found", nodeID)
	}
	addr := n.ClientAddr

	conn, err := newClientDialContextEquivalent(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	client := command2.NewCommandEventClientServiceClient(conn)

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if existing := c.conns[nodeID]; existing != nil {
		_ = conn.Close()
		if cached, ok := c.clients[nodeID]; ok && cached != nil {
			return cached, nil
		}
		return command2.NewCommandEventClientServiceClient(existing), nil
	}

	c.conns[nodeID] = conn
	c.clients[nodeID] = client
	return client, nil
}

func newClientDialContextEquivalent(
	ctx context.Context,
	addr string,
	opts ...grpc.DialOption,
) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	conn.Connect()

	for {
		s := conn.GetState()
		if s == connectivity.Ready {
			return conn, nil
		}
		if !conn.WaitForStateChange(ctx, s) {
			_ = conn.Close()
			return nil, ctx.Err()
		}
	}
}
