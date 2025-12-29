package command

import "errors"

var (
	ErrNotLeader      = errors.New("not leader")
	ErrNoLeader       = errors.New("no leader available")
	ErrInvalidCommand = errors.New("invalid command")
	ErrKeyNotFound    = errors.New("key not found")
)
