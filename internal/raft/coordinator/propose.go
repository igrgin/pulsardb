package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"pulsardb/internal/metrics"
	"pulsardb/internal/raft/ops"
	"time"

	etcdraft "go.etcd.io/raft/v3"
)

func (c *Coordinator) doReadIndex(ctx context.Context) (uint64, error) {
	start := time.Now()

	if c.node.Status().Lead == 0 {
		slog.Debug("read index failed: no leader", "node_id", c.node.ID())
		metrics.ReadIndexTotal.WithLabelValues("no_leader").Inc()
		return 0, ErrNoLeader
	}

	if c.leaseBasedRead {
		if c.node.Status().RaftState != etcdraft.StateLeader {
			metrics.LeaseReadTotal.WithLabelValues("not_leader").Inc()
			return 0, ErrNotLeader
		}
	}

	reqID := c.idGen.Next()
	reqCtx := ops.EncodeReadIndexContext(reqID)
	reqCtxKey := string(reqCtx)

	ch := make(chan uint64, 1)
	c.registerReadWaiter(reqCtxKey, ch)
	defer c.unregisterReadWaiter(reqCtxKey)

	if err := c.node.ReadIndex(ctx, reqCtx); err != nil {
		slog.Debug("read index request failed", "node_id", c.node.ID(), "error", err)
		metrics.ReadIndexTotal.WithLabelValues("error").Inc()
		if c.leaseBasedRead {
			metrics.LeaseReadTotal.WithLabelValues("error").Inc()
		}
		return 0, fmt.Errorf("ReadIndex: %w", err)
	}

	select {
	case idx := <-ch:
		slog.Debug("read index received", "node_id", c.node.ID(), "index", idx)
		metrics.ReadIndexTotal.WithLabelValues("success").Inc()
		metrics.ReadIndexDuration.Observe(time.Since(start).Seconds())
		if c.leaseBasedRead {
			metrics.LeaseReadTotal.WithLabelValues("success").Inc()
		}
		return idx, nil

	case <-ctx.Done():
		slog.Debug("read index timeout", "node_id", c.node.ID())
		metrics.ReadIndexTotal.WithLabelValues("timeout").Inc()
		if c.leaseBasedRead {
			metrics.LeaseReadTotal.WithLabelValues("timeout").Inc()
		}
		return 0, ctx.Err()
	}
}

func (c *Coordinator) registerReadWaiter(key string, ch chan uint64) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	c.readWaiters[key] = &readWaiter{index: 0, ch: ch}
}

func (c *Coordinator) unregisterReadWaiter(key string) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	delete(c.readWaiters, key)
}

func (c *Coordinator) handleReadStates(readStates []etcdraft.ReadState) {
	if len(readStates) == 0 {
		return
	}

	slog.Debug("handling read states", "count", len(readStates))
	lastApplied := c.LastApplied()

	c.readMu.Lock()
	defer c.readMu.Unlock()

	for _, rs := range readStates {
		ctxKey := string(rs.RequestCtx)
		waiter, ok := c.readWaiters[ctxKey]
		if !ok {
			continue
		}

		if rs.Index > waiter.index {
			waiter.index = rs.Index
		}

		if lastApplied >= waiter.index {
			select {
			case waiter.ch <- waiter.index:
			default:
			}
			delete(c.readWaiters, ctxKey)
		}
	}
}

func (c *Coordinator) completeReadWaiters(lastApplied uint64) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	completed := 0
	for ctxKey, w := range c.readWaiters {
		if w == nil || w.index == 0 {
			continue
		}
		if lastApplied >= w.index {
			select {
			case w.ch <- w.index:
			default:
			}
			delete(c.readWaiters, ctxKey)
			completed++
		}
	}
	if completed > 0 {
		slog.Debug("completed read waiters", "count", completed, "lastApplied", lastApplied)
	}
}

func (c *Coordinator) cancelAllReadWaiters() {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for key, w := range c.readWaiters {
		if w != nil && w.ch != nil {
			close(w.ch)
		}
		delete(c.readWaiters, key)
	}
}

func (c *Coordinator) registerAppliedWaiter(index uint64) (string, chan uint64) {
	key := ops.EncodeAppliedWaiterKey(index, c.idGen.Next())
	ch := make(chan uint64, 1)

	c.readMu.Lock()
	c.readWaiters[key] = &readWaiter{index: index, ch: ch}
	c.readMu.Unlock()

	return key, ch
}
