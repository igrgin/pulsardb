package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"pulsardb/internal/configuration"
	"pulsardb/internal/domain"
	"pulsardb/internal/metrics"
	"pulsardb/internal/raft/ops"
	"pulsardb/internal/raft/ports"
	"sync"
	"sync/atomic"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

type Coordinator struct {
	node         ports.RaftNode
	transport    ports.Transport
	store        domain.Store
	stateMachine domain.StateMachine

	stopCh    chan struct{}
	stoppedWg sync.WaitGroup

	inFlight     sync.WaitGroup
	shuttingDown atomic.Bool

	idGen       *ops.RequestIDGenerator
	readWaiters map[string]*readWaiter
	readMu      sync.Mutex

	lastApplied uint64
	appliedMu   sync.RWMutex

	snapCount uint64

	promotionThreshold uint64
	sendQueueSize      int

	tickInterval           time.Duration
	promotionCheckInterval time.Duration
	drainTimeout           time.Duration
	leaseBasedRead         bool
	localRaftAddr          string
	localClientAddr        string

	stepInbox  chan StepRequest
	stopCtx    context.Context
	stopCancel context.CancelFunc
}

type readWaiter struct {
	index uint64
	ch    chan uint64
}

type StepRequest struct {
	Ctx  context.Context
	Msg  raftpb.Message
	Resp chan error
}

type Config struct {
	TickInterval           time.Duration
	SnapCount              uint64
	PromotionThreshold     uint64
	PromotionCheckInterval time.Duration
	StepInboxSize          uint64
	SendQueueSize          int
	DrainTimeout           time.Duration
	LeaseBasedRead         bool
	LocalRaftAddr          string
	LocalClientAddr        string
}

func NewConfigFromProperties(
	cfg *configuration.RaftConfigurationProperties,
	localRaftAddr, localClientAddr string,
) Config {
	return Config{
		TickInterval:           cfg.TickInterval,
		SnapCount:              cfg.SnapCount,
		PromotionThreshold:     cfg.PromotionThreshold,
		PromotionCheckInterval: cfg.PromotionCheckInterval,
		StepInboxSize:          cfg.StepInboxSize,
		SendQueueSize:          cfg.SendQueueSize,
		DrainTimeout:           cfg.ServiceDrainTimeout,
		LeaseBasedRead:         cfg.LeaseBasedRead,
		LocalRaftAddr:          localRaftAddr,
		LocalClientAddr:        localClientAddr,
	}
}

func New(
	node ports.RaftNode,
	transport ports.Transport,
	store domain.Store,
	sm domain.StateMachine,
	cfg Config,
) *Coordinator {
	stopCtx, stopCancel := context.WithCancel(context.Background())

	c := &Coordinator{
		node:         node,
		transport:    transport,
		store:        store,
		stateMachine: sm,

		stopCh: make(chan struct{}),

		idGen:       ops.NewRequestIDGenerator(),
		readWaiters: make(map[string]*readWaiter),

		snapCount: cfg.SnapCount,

		promotionThreshold: cfg.PromotionThreshold,
		sendQueueSize:      cfg.SendQueueSize,

		tickInterval:           cfg.TickInterval,
		promotionCheckInterval: cfg.PromotionCheckInterval,
		drainTimeout:           cfg.DrainTimeout,
		leaseBasedRead:         cfg.LeaseBasedRead,
		localRaftAddr:          cfg.LocalRaftAddr,
		localClientAddr:        cfg.LocalClientAddr,

		stepInbox:  make(chan StepRequest, cfg.StepInboxSize),
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
	}

	transport.AddPeer(node.ID(), cfg.LocalRaftAddr, cfg.LocalClientAddr)

	slog.Info("raft coordinator created",
		"node_id", node.ID(),
		"tickInterval", cfg.TickInterval,
		"snapCount", cfg.SnapCount,
	)

	return c
}

func (c *Coordinator) Start() {
	slog.Info("starting raft coordinator", "node_id", c.node.ID())

	if err := c.recoverState(); err != nil {
		slog.Error("failed to recover state", "error", err)
	}

	c.node.RestoreFromConfState()
	c.startLoops()

	if c.node.IsJoining() {
		go c.requestJoin()
	}

	slog.Info("raft coordinator started", "node_id", c.node.ID())
}

func (c *Coordinator) startLoops() {
	c.stoppedWg.Add(3)

	go func() {
		defer c.stoppedWg.Done()
		c.runMainLoop()
	}()

	go func() {
		defer c.stoppedWg.Done()
		c.runMetricsCollector()
	}()

	go func() {
		defer c.stoppedWg.Done()
		c.runPromotionChecker()
	}()

	slog.Info("raft loops started", "node_id", c.node.ID())
}

func (c *Coordinator) Stop() {
	slog.Info("initiating graceful shutdown", "node_id", c.node.ID())

	c.shuttingDown.Store(true)

	if c.IsLeader() {
		c.tryTransferLeadership()
	}

	c.waitForInflight()
	c.waitForPendingApplies()
	c.cancelAllReadWaiters()
	c.transport.DrainMessageQueues(c.drainTimeout)

	c.stopCancel()
	close(c.stopCh)
	c.stoppedWg.Wait()

	c.node.Stop()

	slog.Info("raft coordinator stopped", "node_id", c.node.ID())
}

func (c *Coordinator) tryTransferLeadership() {
	status := c.node.Status()
	if status.RaftState != etcdraft.StateLeader {
		return
	}

	var targetID uint64
	var maxMatch uint64

	for id, pr := range status.Progress {
		if id == c.node.ID() {
			continue
		}
		if pr.State == tracker.StateReplicate && pr.Match > maxMatch {
			maxMatch = pr.Match
			targetID = id
		}
	}

	if targetID == 0 {
		slog.Warn("no suitable target for leadership transfer", "node_id", c.node.ID())
		return
	}

	metrics.LeaderStepDownTotal.Inc()

	slog.Info("transferring leadership",
		"node_id", c.node.ID(),
		"target", targetID,
	)

	c.node.TransferLeadership(context.Background(), c.node.ID(), targetID)

	deadline := time.Now().Add(2 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		<-ticker.C
		if c.node.Status().RaftState != etcdraft.StateLeader {
			slog.Info("leadership transferred", "new_leader", c.node.Status().Lead)
			return
		}
	}

	slog.Warn("leadership transfer timed out", "node_id", c.node.ID())
}

func (c *Coordinator) waitForInflight() {
	done := make(chan struct{})
	go func() {
		c.inFlight.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Debug("all in-flight operations completed", "node_id", c.node.ID())
	case <-time.After(c.drainTimeout):
		slog.Warn("timed out waiting for in-flight operations", "node_id", c.node.ID())
	}
}

func (c *Coordinator) waitForPendingApplies() {
	status := c.node.Status()
	target := status.Commit

	if c.LastApplied() >= target {
		return
	}

	slog.Debug("waiting for pending applies",
		"node_id", c.node.ID(),
		"current", c.LastApplied(),
		"target", target,
	)

	deadline := time.Now().Add(c.drainTimeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		if c.LastApplied() >= target {
			slog.Debug("pending applies completed", "applied", c.LastApplied())
			return
		}
		<-ticker.C
	}

	slog.Warn("timed out waiting for pending applies",
		"applied", c.LastApplied(),
		"target", target,
	)
}

func (c *Coordinator) acquireInflight() bool {
	if c.shuttingDown.Load() {
		return false
	}
	c.inFlight.Add(1)

	if c.shuttingDown.Load() {
		c.inFlight.Done()
		return false
	}
	return true
}

func (c *Coordinator) releaseInflight() {
	c.inFlight.Done()
}

func (c *Coordinator) Propose(ctx context.Context, data []byte) error {
	if c.shuttingDown.Load() {
		return ErrShuttingDown
	}
	if c.node.Status().Lead == 0 {
		return ErrNoLeader
	}
	return c.node.Propose(ctx, data)
}

func (c *Coordinator) ReadIndex(ctx context.Context) (uint64, error) {
	if c.shuttingDown.Load() {
		return 0, ErrShuttingDown
	}
	return c.doReadIndex(ctx)
}

func (c *Coordinator) IsLeader() bool {
	return c.node.Status().RaftState == etcdraft.StateLeader
}

func (c *Coordinator) LeaderID() uint64 {
	return c.node.Status().Lead
}

func (c *Coordinator) NodeID() uint64 {
	return c.node.ID()
}

func (c *Coordinator) GetPeerAddr(nodeID uint64) string {
	_, clientAddr := c.transport.GetPeerAddrs(nodeID)
	if clientAddr == "" {
		slog.Warn("leader address lookup failed", "node_id", nodeID)
	}
	return clientAddr
}

func (c *Coordinator) Step(ctx context.Context, msg raftpb.Message) error {
	req := StepRequest{
		Ctx:  ctx,
		Msg:  msg,
		Resp: make(chan error, 1),
	}

	select {
	case c.stepInbox <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-req.Resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Coordinator) HandleJoinRequest(ctx context.Context, nodeID uint64, raftAddr, clientAddr string) error {
	if !c.IsLeader() {
		return ErrNotLeader
	}

	confState := c.node.ConfState()

	if ops.IsVoter(confState, nodeID) {
		slog.Info("node already a voter", "node_id", nodeID)
		return nil
	}

	if ops.IsLearner(confState, nodeID) {
		slog.Info("node already a learner, will be promoted when ready", "node_id", nodeID)
		return nil
	}

	c.transport.AddPeer(nodeID, raftAddr, clientAddr)

	slog.Info("adding node as learner", "node_id", nodeID, "raft_addr", raftAddr)
	return c.ProposeAddLearnerNode(ctx, nodeID, raftAddr, clientAddr)
}

func (c *Coordinator) ProposeAddNode(ctx context.Context, nodeID uint64, raftAddr, clientAddr string) error {
	if !c.IsLeader() {
		return ErrNotLeader
	}

	slog.Info("proposing add node", "node_id", c.node.ID(), "target", nodeID, "raftAddr", raftAddr)
	cc := ops.BuildAddNodeChange(nodeID, raftAddr, clientAddr)
	return c.node.ProposeConfChange(ctx, cc)
}

func (c *Coordinator) ProposeAddLearnerNode(ctx context.Context, nodeID uint64, raftAddr, clientAddr string) error {
	if !c.IsLeader() {
		return ErrNotLeader
	}

	slog.Info("proposing add learner node", "node_id", c.node.ID(), "target", nodeID, "raftAddr", raftAddr)
	cc := ops.BuildAddLearnerChange(nodeID, raftAddr, clientAddr)
	return c.node.ProposeConfChange(ctx, cc)
}

func (c *Coordinator) ProposeRemoveNode(ctx context.Context, nodeID uint64) error {
	if !c.IsLeader() {
		return ErrNotLeader
	}

	slog.Info("proposing remove node", "node_id", c.node.ID(), "target", nodeID)
	cc := ops.BuildRemoveNodeChange(nodeID)
	return c.node.ProposeConfChange(ctx, cc)
}

func (c *Coordinator) WaitForQuorum(ctx context.Context) (leaderID uint64, readIndex uint64, err error) {
	slog.Debug("waiting for quorum", "node_id", c.node.ID())

	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	for {
		st := c.node.Status()

		if st.Lead == 0 {
			slog.Debug("no leader yet", "node_id", c.node.ID())
		} else if st.RaftState == etcdraft.StateLeader {
			idx, err := c.ReadIndex(ctx)
			if err == nil {
				slog.Info("quorum established as leader",
					"node_id", c.node.ID(),
					"read_index", idx,
				)
				return st.Lead, idx, nil
			}
			slog.Debug("leader read index failed",
				"node_id", c.node.ID(),
				"error", err,
			)
		} else {
			slog.Debug("checking quorum as follower",
				"node_id", c.node.ID(),
				"leader_id", st.Lead,
			)

			idx, err := c.GetReadIndexFromLeader(ctx, st.Lead)
			if err != nil {
				slog.Debug("get read index from leader failed",
					"node_id", c.node.ID(),
					"leader_id", st.Lead,
					"error", err,
				)
			} else {
				if err := c.WaitUntilApplied(ctx, idx); err == nil {
					slog.Info("quorum established as follower",
						"node_id", c.node.ID(),
						"leader_id", st.Lead,
						"read_index", idx,
					)
					return st.Lead, idx, nil
				}

				slog.Debug("wait until applied failed",
					"node_id", c.node.ID(),
					"index", idx,
					"error", err,
				)
			}
		}

		select {
		case <-ctx.Done():
			slog.Warn("wait for quorum timed out",
				"node_id", c.node.ID(),
				"last_leader", st.Lead,
				"raft_state", st.RaftState.String(),
			)
			return 0, 0, ctx.Err()
		case <-c.stopCh:
			return 0, 0, context.Canceled
		case <-t.C:
		}
	}
}

func (c *Coordinator) GetReadIndexFromLeader(ctx context.Context, leaderID uint64) (uint64, error) {
	if leaderID == 0 {
		return 0, fmt.Errorf("no known leader")
	}

	client, ok := c.transport.GetLeaderClient(leaderID)
	if !ok {
		return 0, fmt.Errorf("no client for leader %d", leaderID)
	}

	slog.Debug("getting read index from leader", "node_id", c.node.ID(), "leader_id", leaderID)
	readIndex, err := client.GetReadIndex(ctx, c.node.ID())
	if err != nil {
		slog.Debug("get read index from leader failed", "leader_id", leaderID, "error", err)
		return 0, fmt.Errorf("forward to leader %d: %w", leaderID, err)
	}

	return readIndex, nil
}

func (c *Coordinator) WaitUntilApplied(ctx context.Context, index uint64) error {
	if c.LastApplied() >= index {
		return nil
	}

	slog.Debug("waiting until applied", "node_id", c.node.ID(), "target_index", index, "current", c.LastApplied())

	key, ch := c.registerAppliedWaiter(index)
	defer c.unregisterReadWaiter(key)

	select {
	case <-ch:
		slog.Debug("applied index reached", "node_id", c.node.ID(), "index", index)
		return nil
	case <-ctx.Done():
		slog.Debug("wait for applied timed out", "node_id", c.node.ID(), "index", index)
		return ctx.Err()
	}
}

func (c *Coordinator) ReconcileConfiguredPeers() {
	time.Sleep(5 * time.Second)

	if !c.IsLeader() {
		slog.Debug("skipping peer reconciliation: not leader", "node_id", c.node.ID())
		return
	}

	slog.Debug("reconciling configured peers", "node_id", c.node.ID())

	confState := c.node.ConfState()
	raftPeers, clientPeers := c.transport.AllPeers()

	configuredSet := make(map[uint64]bool)
	configuredSet[c.node.ID()] = true
	for nodeID := range raftPeers {
		configuredSet[nodeID] = true
	}

	voterSet := make(map[uint64]bool)
	for _, v := range confState.Voters {
		voterSet[v] = true
	}

	for nodeID, raftAddr := range raftPeers {
		if nodeID == c.node.ID() {
			continue
		}
		if !voterSet[nodeID] {
			clientAddr := clientPeers[nodeID]
			slog.Info("proposing new peer from config",
				"node_id", nodeID,
				"raftAddr", raftAddr,
				"clientAddr", clientAddr,
			)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := c.ProposeAddLearnerNode(ctx, nodeID, raftAddr, clientAddr); err != nil {
				slog.Warn("failed to propose new peer", "node_id", nodeID, "error", err)
			}
			cancel()
			time.Sleep(time.Second)
		}
	}

	for _, voterID := range confState.Voters {
		if voterID == c.node.ID() {
			continue
		}
		if !configuredSet[voterID] {
			slog.Info("proposing removal of peer not in config", "node_id", voterID)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := c.ProposeRemoveNode(ctx, voterID); err != nil {
				slog.Warn("failed to propose peer removal", "node_id", voterID, "error", err)
			}
			cancel()
			time.Sleep(time.Second)
		}
	}

	for _, learnerID := range confState.Learners {
		if learnerID == c.node.ID() {
			continue
		}
		if !configuredSet[learnerID] {
			slog.Info("proposing removal of learner not in config", "node_id", learnerID)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := c.ProposeRemoveNode(ctx, learnerID); err != nil {
				slog.Warn("failed to propose learner removal", "node_id", learnerID, "error", err)
			}
			cancel()
			time.Sleep(time.Second)
		}
	}

	slog.Debug("peer reconciliation complete", "node_id", c.node.ID())
}

func (c *Coordinator) LastApplied() uint64 {
	c.appliedMu.RLock()
	defer c.appliedMu.RUnlock()
	return c.lastApplied
}

func (c *Coordinator) SetLastApplied(index uint64) {
	c.appliedMu.Lock()
	if index > c.lastApplied {
		c.lastApplied = index
		slog.Debug("last applied updated", "node_id", c.node.ID(), "index", index)
	}
	c.appliedMu.Unlock()

	c.completeReadWaiters(index)
}

func (c *Coordinator) TriggerSnapshot() error {
	confState := c.node.ConfState()
	return c.triggerSnapshot(c.LastApplied(), &confState)
}

func (c *Coordinator) UpdateMetrics() {
	status := c.node.Status()

	if status.RaftState == etcdraft.StateLeader {
		metrics.RaftIsLeader.Set(1)
	} else {
		metrics.RaftIsLeader.Set(0)
	}

	metrics.RaftTerm.Set(float64(status.Term))
	metrics.RaftCommitIndex.Set(float64(status.Commit))
	metrics.RaftAppliedIndex.Set(float64(c.LastApplied()))
	metrics.RaftSnapshotIndex.Set(float64(c.snapshotIndex()))

	confState := c.node.ConfState()
	metrics.RaftPeersTotal.Set(float64(len(confState.Voters) + len(confState.Learners)))
	metrics.StorageKeysTotal.Set(float64(c.store.Len()))
}
