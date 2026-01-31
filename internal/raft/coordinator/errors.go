package coordinator

import "errors"

var (
	ErrNotLeader = errors.New("not leader")

	ErrShuttingDown = errors.New("shutting down")

	ErrNoLeader = errors.New("no leader")

	ErrAlreadyVoter = errors.New("already voter")

	ErrAlreadyLearner = errors.New("already learner")
)
