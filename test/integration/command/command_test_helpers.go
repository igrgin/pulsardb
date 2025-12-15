package integration

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"pulsardb/internal/transport/gen/commandevents"
)

var nextEventID atomic.Uint64

func newEventID() uint64 {
	return nextEventID.Add(1)
}

func (c *CommandTestCluster) GetLeader() *CommandTestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, node := range c.nodes {
		if node.stopped {
			continue
		}
		if node.RaftService.IsLeader() {
			return node
		}
	}
	return nil
}

func (c *CommandTestCluster) GetFollowers() []*CommandTestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var followers []*CommandTestNode
	for _, node := range c.nodes {
		if node.stopped {
			continue
		}
		if !node.RaftService.IsLeader() {
			followers = append(followers, node)
		}
	}
	return followers
}

func (c *CommandTestCluster) GetNode(id uint64) *CommandTestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodes[id]
}

func (c *CommandTestCluster) StopNode(id uint64) error {
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
	node.RaftService.Stop()
	node.stopped = true

	return nil
}

func (c *CommandTestCluster) RestartNode(id uint64) error {
	c.mu.RLock()
	oldNode, ok := c.nodes[id]
	c.mu.RUnlock()

	if !ok {
		return fmt.Errorf("node %d not found", id)
	}

	oldNode.mu.Lock()
	raftAddr := oldNode.RaftAddr
	clientAddr := oldNode.ClientAddr
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

func (c *CommandTestCluster) WaitForLeader(timeout time.Duration) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("timeout waiting for leader")
		case <-ticker.C:
			c.mu.RLock()
			for _, node := range c.nodes {
				if node.stopped {
					continue
				}
				if node.RaftService.IsLeader() {
					id := node.ID
					c.mu.RUnlock()
					return id, nil
				}
			}
			c.mu.RUnlock()
		}
	}
}

func (c *CommandTestCluster) WaitForNewLeader(excludeID uint64, timeout time.Duration) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("timeout waiting for new leader")
		case <-ticker.C:
			c.mu.RLock()
			for _, node := range c.nodes {
				if node.stopped || node.ID == excludeID {
					continue
				}
				if node.RaftService.IsLeader() {
					id := node.ID
					c.mu.RUnlock()
					return id, nil
				}
			}
			c.mu.RUnlock()
		}
	}
}

func (c *CommandTestCluster) WaitForConvergence(timeout time.Duration) error {
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
				if node.stopped {
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

func (c *CommandTestCluster) ProcessCommand(ctx context.Context, req *commandeventspb.CommandEventRequest) (*commandeventspb.CommandEventResponse, error) {
	leader := c.GetLeader()
	if leader == nil {
		return nil, fmt.Errorf("no leader available")
	}
	return leader.CmdService.ProcessCommand(ctx, req)
}

func (c *CommandTestCluster) SetValue(ctx context.Context, key, value string) error {
	req := &commandeventspb.CommandEventRequest{
		EventId: newEventID(),
		Type:    commandeventspb.CommandEventType_SET,
		Key:     key,
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: value},
		},
	}
	resp, err := c.ProcessCommand(ctx, req)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("set failed: %s", resp.GetError().GetMessage())
	}
	return nil
}

func (c *CommandTestCluster) GetValue(ctx context.Context, key string) (string, bool, error) {
	req := &commandeventspb.CommandEventRequest{
		EventId: uint64(time.Now().UnixNano()),
		Type:    commandeventspb.CommandEventType_GET,
		Key:     key,
	}
	resp, err := c.ProcessCommand(ctx, req)
	if err != nil {
		return "", false, err
	}
	if !resp.Success {
		return "", false, nil
	}
	return resp.GetValue().GetStringValue(), true, nil
}

func (c *CommandTestCluster) DeleteValue(ctx context.Context, key string) error {
	req := &commandeventspb.CommandEventRequest{
		EventId: uint64(time.Now().UnixNano()),
		Type:    commandeventspb.CommandEventType_DELETE,
		Key:     key,
	}
	resp, err := c.ProcessCommand(ctx, req)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("delete failed: %s", resp.GetError().GetMessage())
	}
	return nil
}

func (c *CommandTestCluster) VerifyConsistency(key string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var expected any
	first := true

	for _, node := range c.nodes {
		if node.stopped {
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
					return false, fmt.Errorf("mismatch on node %d", node.ID)
				}
			} else if expected != nil {
				return false, fmt.Errorf("missing on node %d", node.ID)
			}
		}
	}

	return true, nil
}
