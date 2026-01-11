package domain

import "context"

type Store interface {
	Get(key string) (any, bool)
	Set(key string, value any)
	Delete(key string)
	Snapshot() ([]byte, error)
	Restore(data []byte) error
	Len() int
}

type StateMachine interface {
	Apply(data []byte) error
}

type Consensus interface {
	Propose(ctx context.Context, data []byte) error
	ReadIndex(ctx context.Context) (uint64, error)
	IsLeader() bool
	LeaderID() uint64
	NodeID() uint64
	GetPeerAddr(id uint64) string
}
