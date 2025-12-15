package raft

import (
	"context"
	"pulsardb/internal/transport/util"
	"sync"
	"time"

	"pulsardb/internal/transport/gen/commandevents"

	"google.golang.org/protobuf/proto"
)

type PendingRegistry interface {
	RegisterPending(eventID uint64, ch chan *commandeventspb.CommandEventResponse)
	UnregisterPending(eventID uint64)
}

type Proposer interface {
	Propose(ctx context.Context, data []byte) error
}

type pendingProposal struct {
	req    *commandeventspb.CommandEventRequest
	respCh chan *commandeventspb.CommandEventResponse
}

type Batcher struct {
	proposer     Proposer
	registry     PendingRegistry
	maxBatchSize int
	maxWait      time.Duration

	mu      sync.Mutex
	pending []pendingProposal
	timer   *time.Timer
}

func NewBatcher(proposer Proposer, registry PendingRegistry, maxBatchSize int, maxWait time.Duration) *Batcher {
	return &Batcher{
		proposer:     proposer,
		registry:     registry,
		maxBatchSize: maxBatchSize,
		maxWait:      maxWait,
		pending:      make([]pendingProposal, 0, maxBatchSize),
	}
}

func (b *Batcher) Propose(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (chan *commandeventspb.CommandEventResponse, error) {
	respCh := make(chan *commandeventspb.CommandEventResponse, 1)

	b.mu.Lock()
	b.pending = append(b.pending, pendingProposal{req: req, respCh: respCh})

	if len(b.pending) >= b.maxBatchSize {
		b.flushLocked(ctx)
		b.mu.Unlock()
		return respCh, nil
	}

	if len(b.pending) == 1 {
		b.timer = time.AfterFunc(b.maxWait, func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if len(b.pending) > 0 {
				b.flushLocked(context.Background())
			}
		})
	}

	b.mu.Unlock()
	return respCh, nil
}

func (b *Batcher) flushLocked(ctx context.Context) {
	if len(b.pending) == 0 {
		return
	}

	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}

	batch := b.pending
	b.pending = make([]pendingProposal, 0, b.maxBatchSize)

	batchedCmd := batchedCommands(make([]*commandeventspb.CommandEventRequest, len(batch)))
	for i, p := range batch {
		batchedCmd.Commands[i] = p.req
		b.registry.RegisterPending(p.req.EventId, p.respCh)
	}

	data, err := proto.Marshal(batchedCmd)
	if err != nil {
		for _, p := range batch {
			p.respCh <- transport.ErrorResponse(p.req.EventId, commandeventspb.ErrorCode_UNKNOWN, "marshal error")
			b.registry.UnregisterPending(p.req.EventId)
		}
		return
	}

	go func() {
		if err := b.proposer.Propose(ctx, data); err != nil {
			for _, p := range batch {
				p.respCh <- transport.ErrorResponse(p.req.EventId, commandeventspb.ErrorCode_NO_QUORUM, err.Error())
				b.registry.UnregisterPending(p.req.EventId)
			}
		}
	}()
}

func batchedCommands(commands []*commandeventspb.CommandEventRequest) *commandeventspb.BatchedCommands {
	return &commandeventspb.BatchedCommands{Commands: commands}
}
