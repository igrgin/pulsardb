package command

import (
	"context"
	"log/slog"
	"pulsardb/internal/domain"
	"pulsardb/internal/metrics"
	"pulsardb/internal/transport/gen/command"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

type PendingRegistry interface {
	Register(eventID uint64, ch chan *commandeventspb.CommandEventResponse)
	Unregister(eventID uint64)
}

type pendingCmd struct {
	ctx  context.Context
	req  *commandeventspb.CommandEventRequest
	resp chan *commandeventspb.CommandEventResponse
}

type Batcher struct {
	proposer domain.Consensus
	registry PendingRegistry
	maxSize  int
	maxWait  time.Duration

	Mu      sync.Mutex
	Pending []pendingCmd
	Timer   *time.Timer

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewBatcher(proposer domain.Consensus, registry PendingRegistry, cfg BatchConfig) *Batcher {
	b := &Batcher{
		proposer: proposer,
		registry: registry,
		maxSize:  cfg.MaxSize,
		maxWait:  cfg.MaxWait,
		Pending:  make([]pendingCmd, 0, cfg.MaxSize),
		stopCh:   make(chan struct{}),
	}

	b.wg.Add(1)
	go b.cleanupLoop(cfg.CleanupTickInterval)

	slog.Info("batcher started", "maxSize", cfg.MaxSize, "maxWait", b.maxWait)
	return b
}

func (b *Batcher) Stop() {
	slog.Info("batcher stopping")
	close(b.stopCh)
	b.wg.Wait()
	slog.Info("batcher stopped")
}

func (b *Batcher) cleanupLoop(cleanupTickInterval time.Duration) {
	defer b.wg.Done()

	ticker := time.NewTicker(cleanupTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.cleanupExpired()
		}
	}
}

func (b *Batcher) cleanupExpired() {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if len(b.Pending) == 0 {
		return
	}

	before := len(b.Pending)
	alive := b.Pending[:0]
	for _, p := range b.Pending {
		if p.ctx.Err() == nil {
			alive = append(alive, p)
		}
	}
	b.Pending = alive

	if expired := before - len(alive); expired > 0 {
		slog.Info("cleaned up expired commands", "expired", expired, "remaining", len(alive))
	}

	metrics.BatchPendingCommands.Set(float64(len(b.Pending)))

	if len(b.Pending) == 0 && b.Timer != nil {
		b.Timer.Stop()
		b.Timer = nil
	}
}

func (b *Batcher) Submit(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (chan *commandeventspb.CommandEventResponse, error) {
	if ctx.Err() != nil {
		slog.Debug("submit rejected: context already cancelled", "eventId", req.EventId)
		return nil, ctx.Err()
	}

	respCh := make(chan *commandeventspb.CommandEventResponse, 1)

	b.Mu.Lock()
	defer b.Mu.Unlock()

	b.Pending = append(b.Pending, pendingCmd{
		ctx:  ctx,
		req:  req,
		resp: respCh,
	})

	metrics.BatchPendingCommands.Set(float64(len(b.Pending)))

	slog.Debug("command submitted", "eventId", req.EventId, "pending", len(b.Pending))

	if len(b.Pending) >= b.maxSize {
		slog.Debug("batch full, flushing", "size", len(b.Pending))
		metrics.BatchFlushTotal.WithLabelValues("full").Inc()
		b.flushLocked()
		return respCh, nil
	}

	if len(b.Pending) == 1 {
		b.Timer = time.AfterFunc(b.maxWait, func() {
			b.Mu.Lock()
			defer b.Mu.Unlock()
			if len(b.Pending) > 0 {
				slog.Debug("batch timer expired, flushing", "size", len(b.Pending))
				metrics.BatchFlushTotal.WithLabelValues("timeout").Inc()
				b.flushLocked()
			}
		})
	}

	return respCh, nil
}

func (b *Batcher) flushLocked() {
	if len(b.Pending) == 0 {
		return
	}

	if b.Timer != nil {
		b.Timer.Stop()
		b.Timer = nil
	}

	alive := make([]pendingCmd, 0, len(b.Pending))
	for _, p := range b.Pending {
		if p.ctx.Err() == nil {
			alive = append(alive, p)
		}
	}

	if len(alive) == 0 {
		slog.Debug("flush skipped: all commands expired")
		b.Pending = make([]pendingCmd, 0, b.maxSize)
		metrics.BatchPendingCommands.Set(0)
		return
	}

	batch := alive
	b.Pending = make([]pendingCmd, 0, b.maxSize)
	metrics.BatchPendingCommands.Set(0)

	metrics.BatchSize.Observe(float64(len(batch)))

	slog.Debug("flushing batch", "size", len(batch))
	go b.sendBatch(batch)
}

func (b *Batcher) sendBatch(batch []pendingCmd) {
	batchedCmd := &commandeventspb.BatchedCommands{
		Commands: make([]*commandeventspb.CommandEventRequest, len(batch)),
	}

	for i, p := range batch {
		batchedCmd.Commands[i] = p.req
		b.registry.Register(p.req.EventId, p.resp)
	}

	data, err := proto.Marshal(batchedCmd)
	if err != nil {
		slog.Warn("failed to marshal batch", "error", err, "size", len(batch))
		b.failBatch(batch, err)
		return
	}

	ctx, cancel := b.deriveContext(batch)
	defer cancel()

	metrics.RaftProposalsTotal.Inc()

	slog.Debug("proposing batch", "size", len(batch), "bytes", len(data))
	if err := b.proposer.Propose(ctx, data); err != nil {
		slog.Warn("batch proposal failed", "error", err, "size", len(batch))
		metrics.RaftProposalsFailed.Inc()
		b.failBatch(batch, err)
	}
}

func (b *Batcher) failBatch(batch []pendingCmd, err error) {
	slog.Warn("failing batch", "error", err, "size", len(batch))

	for _, p := range batch {
		errCode := commandeventspb.ErrorCode_NO_QUORUM
		errMsg := err.Error()

		if p.ctx.Err() != nil {
			errCode = commandeventspb.ErrorCode_TIMEOUT
			errMsg = "request context expired"
		}

		select {
		case p.resp <- &commandeventspb.CommandEventResponse{
			EventId: p.req.EventId,
			Success: false,
			Error: &commandeventspb.CommandError{
				Code:    errCode,
				Message: errMsg,
			},
		}:
		default:
		}
		b.registry.Unregister(p.req.EventId)
	}
}

func (b *Batcher) deriveContext(batch []pendingCmd) (context.Context, context.CancelFunc) {
	var earliest time.Time
	hasDeadline := false

	for _, p := range batch {
		if dl, ok := p.ctx.Deadline(); ok {
			if !hasDeadline || dl.Before(earliest) {
				earliest = dl
				hasDeadline = true
			}
		}
	}

	if hasDeadline {
		return context.WithDeadline(context.Background(), earliest)
	}

	return context.WithTimeout(context.Background(), 30*time.Second)
}
