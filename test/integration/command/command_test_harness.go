package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"pulsardb/internal/command"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/raft"
	"pulsardb/internal/storage"
	"pulsardb/internal/transport/gen/commandevents"
	rafttransportpb "pulsardb/internal/transport/gen/raft"

	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CommandTestCluster struct {
	t        *testing.T
	nodes    map[uint64]*CommandTestNode
	baseDir  string
	mu       sync.RWMutex
	nextPort int
}

type CommandTestNode struct {
	ID             uint64
	RaftNode       *raft.Node
	RaftService    *raft.Service
	CmdService     *command.Service
	StorageService *storage.Service
	Batcher        *raft.Batcher
	RaftServer     *grpc.Server
	ClientServer   *grpc.Server
	RaftAddr       string
	ClientAddr     string
	RaftLis        net.Listener
	ClientLis      net.Listener
	stopped        bool
	mu             sync.Mutex
}

func NewCommandTestCluster(t *testing.T) *CommandTestCluster {
	baseDir, err := os.MkdirTemp("", "cmd-test-*")
	require.NoError(t, err)

	return &CommandTestCluster{
		t:        t,
		nodes:    make(map[uint64]*CommandTestNode),
		baseDir:  baseDir,
		nextPort: 30000 + (os.Getpid() % 10000),
	}
}

func (c *CommandTestCluster) allocPort() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	port := c.nextPort
	c.nextPort++
	return port
}

func (c *CommandTestCluster) StartNodes(n int) error {
	raftAddrs := make(map[uint64]string)
	clientAddrs := make(map[uint64]string)
	configs := make([]struct {
		id         uint64
		raftAddr   string
		clientAddr string
	}, n)

	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		raftAddr := fmt.Sprintf("127.0.0.1:%d", c.allocPort())
		clientAddr := fmt.Sprintf("127.0.0.1:%d", c.allocPort())
		configs[i] = struct {
			id         uint64
			raftAddr   string
			clientAddr string
		}{id, raftAddr, clientAddr}
		raftAddrs[id] = raftAddr
		clientAddrs[id] = clientAddr
	}

	for _, cfg := range configs {
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

		if err := c.startNode(cfg.id, cfg.raftAddr, cfg.clientAddr, peers, clientPeers); err != nil {
			return fmt.Errorf("start node %d: %w", cfg.id, err)
		}
	}

	return nil
}

func (c *CommandTestCluster) startNode(
	id uint64,
	raftAddr, clientAddr string,
	raftPeers, clientPeers map[uint64]string,
) error {
	nodeDir := filepath.Join(c.baseDir, fmt.Sprintf("node-%d", id))
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

	raftNode, err := raft.NewNode(rc, raftAddr)
	if err != nil {
		return fmt.Errorf("new raft node: %w", err)
	}

	storageSvc := storage.NewStorageService()
	holder := &cmdServiceHolder{}

	raftSvc := raft.NewService(raftNode, storageSvc, rc, holder)

	batcher := raft.NewBatcher(
		raftSvc,
		holder,
		rc.BatchSize,
		time.Duration(rc.BatchMaxWait)*time.Millisecond,
	)

	cmdSvc := command.NewCommandService(storageSvc, raftSvc, batcher)
	holder.svc = cmdSvc

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
		&cmdTestRaftServer{raftSvc: raftSvc},
	)

	commandeventspb.RegisterCommandEventClientServiceServer(
		clientServer,
		&cmdTestClientServer{cmdSvc: cmdSvc, raftSvc: raftSvc},
	)

	go raftServer.Serve(raftLis)
	go clientServer.Serve(clientLis)

	node := &CommandTestNode{
		ID:             id,
		RaftNode:       raftNode,
		RaftService:    raftSvc,
		CmdService:     cmdSvc,
		StorageService: storageSvc,
		Batcher:        batcher,
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

func (c *CommandTestCluster) Cleanup() {
	c.mu.Lock()
	nodes := make([]*CommandTestNode, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.mu.Unlock()

	for _, node := range nodes {
		node.mu.Lock()
		if !node.stopped {
			node.RaftServer.Stop()
			node.ClientServer.Stop()
			node.RaftService.Stop()
			node.stopped = true
		}
		node.mu.Unlock()
	}

	os.RemoveAll(c.baseDir)
}

type cmdServiceHolder struct {
	svc *command.Service
}

func (h *cmdServiceHolder) Apply(data []byte) ([]byte, error) {
	return h.svc.Apply(data)
}

func (h *cmdServiceHolder) RegisterPending(eventID uint64, ch chan *commandeventspb.CommandEventResponse) {
	h.svc.RegisterPending(eventID, ch)
}

func (h *cmdServiceHolder) UnregisterPending(eventID uint64) {
	h.svc.UnregisterPending(eventID)
}

type cmdTestRaftServer struct {
	rafttransportpb.UnimplementedRaftTransportServiceServer
	raftSvc *raft.Service
}

func (s *cmdTestRaftServer) SendRaftMessage(ctx context.Context, req *rafttransportpb.RaftMessage) (*rafttransportpb.RaftMessageResponse, error) {
	var msg raftpb.Message
	if err := msg.Unmarshal(req.Data); err != nil {
		return nil, err
	}
	if err := s.raftSvc.CallRaftStep(ctx, msg); err != nil {
		return nil, err
	}
	return &rafttransportpb.RaftMessageResponse{}, nil
}

func (s *cmdTestRaftServer) GetReadIndex(ctx context.Context, req *rafttransportpb.GetReadIndexRequest) (*rafttransportpb.GetReadIndexResponse, error) {
	idx, err := s.raftSvc.GetReadIndex(ctx)
	if err != nil {
		return nil, err
	}
	return &rafttransportpb.GetReadIndexResponse{ReadIndex: idx}, nil
}

type cmdTestClientServer struct {
	commandeventspb.UnimplementedCommandEventClientServiceServer
	cmdSvc  *command.Service
	raftSvc *raft.Service
}

func (s *cmdTestClientServer) ProcessCommandEvent(ctx context.Context, req *commandeventspb.CommandEventRequest) (*commandeventspb.CommandEventResponse, error) {
	return s.cmdSvc.ProcessCommand(ctx, req)
}

func (c *CommandTestCluster) NewClientConn(nodeID uint64) (*grpc.ClientConn, error) {
	node := c.nodes[nodeID]
	if node == nil {
		return nil, fmt.Errorf("node %d not found", nodeID)
	}
	return grpc.NewClient(node.ClientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
