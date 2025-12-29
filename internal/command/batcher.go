package command

import (
	"context"
	"log/slog"
	"pulsardb/internal/domain"
	"sync"
	"time"

	"pulsardb/internal/transport/gen/commandevents"

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

	mu      sync.Mutex
	pending []pendingCmd
	timer   *time.Timer

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewBatcher(proposer domain.Consensus, registry PendingRegistry, cfg BatchConfig) *Batcher {
	b := &Batcher{
		proposer: proposer,
		registry: registry,
		maxSize:  cfg.MaxSize,
		maxWait:  cfg.MaxWait,
		pending:  make([]pendingCmd, 0, cfg.MaxSize),
		stopCh:   make(chan struct{}),
	}

	b.wg.Add(1)
	go b.cleanupLoop()

	slog.Info("batcher started", "maxSize", cfg.MaxSize, "maxWait", b.maxWait)
	return b
}

func (b *Batcher) Stop() {
	slog.Info("batcher stopping")
	close(b.stopCh)
	b.wg.Wait()
	slog.Info("batcher stopped")
}

func (b *Batcher) cleanupLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
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
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.pending) == 0 {
		return
	}

	before := len(b.pending)
	alive := b.pending[:0]
	for _, p := range b.pending {
		if p.ctx.Err() == nil {
			alive = append(alive, p)
		}
	}
	b.pending = alive

	if expired := before - len(alive); expired > 0 {
		slog.Debug("cleaned up expired commands", "expired", expired, "remaining", len(alive))
	}

	if len(b.pending) == 0 && b.timer != nil {
		b.timer.Stop()
		b.timer = nil
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

	b.mu.Lock()
	defer b.mu.Unlock()

	b.pending = append(b.pending, pendingCmd{
		ctx:  ctx,
		req:  req,
		resp: respCh,
	})

	slog.Debug("command submitted", "eventId", req.EventId, "pending", len(b.pending))

	if len(b.pending) >= b.maxSize {
		slog.Debug("batch full, flushing", "size", len(b.pending))
		b.flushLocked()
		return respCh, nil
	}

	if len(b.pending) == 1 {
		b.timer = time.AfterFunc(b.maxWait, func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if len(b.pending) > 0 {
				slog.Debug("batch timer expired, flushing", "size", len(b.pending))
				b.flushLocked()
			}
		})
	}

	return respCh, nil
}

func (b *Batcher) flushLocked() {
	if len(b.pending) == 0 {
		return
	}

	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}

	alive := make([]pendingCmd, 0, len(b.pending))
	for _, p := range b.pending {
		if p.ctx.Err() == nil {
			alive = append(alive, p)
		}
	}

	if len(alive) == 0 {
		slog.Debug("flush skipped: all commands expired")
		b.pending = make([]pendingCmd, 0, b.maxSize)
		return
	}

	batch := alive
	b.pending = make([]pendingCmd, 0, b.maxSize)

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

	slog.Debug("proposing batch", "size", len(batch), "bytes", len(data))
	if err := b.proposer.Propose(ctx, data); err != nil {
		slog.Warn("batch proposal failed", "error", err, "size", len(batch))
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
