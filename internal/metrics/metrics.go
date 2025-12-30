package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RaftIsLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "is_leader",
		Help:      "Whether this node is the Raft leader (1=leader, 0=follower)",
	})

	RaftTerm = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "term",
		Help:      "Current Raft term",
	})

	RaftCommitIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "commit_index",
		Help:      "Current Raft commit index",
	})

	RaftAppliedIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "applied_index",
		Help:      "Last applied Raft index",
	})

	RaftSnapshotIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "snapshot_index",
		Help:      "Last snapshot index",
	})

	RaftPeersTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "peers_total",
		Help:      "Number of Raft peers",
	})

	RaftMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "messages_total",
		Help:      "Total Raft messages sent/received",
	}, []string{"direction", "type"})

	RaftMessageErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "message_errors_total",
		Help:      "Total Raft message errors",
	}, []string{"peer_id"})

	RaftProposalsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "proposals_total",
		Help:      "Total proposals submitted",
	})

	RaftProposalsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "proposals_failed_total",
		Help:      "Total failed proposals",
	})

	RaftSnapshotsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "snapshots_total",
		Help:      "Total snapshots taken",
	})

	RaftSnapshotDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "snapshot_duration_seconds",
		Help:      "Time to create snapshot",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
	})

	CommandsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "command",
		Name:      "total",
		Help:      "Total commands processed",
	}, []string{"type", "status"})

	CommandDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "command",
		Name:      "duration_seconds",
		Help:      "Command processing duration",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20),
	}, []string{"type"})

	CommandsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "command",
		Name:      "in_flight",
		Help:      "Commands currently being processed",
	})

	BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "batch",
		Name:      "size",
		Help:      "Number of commands per batch",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
	})

	BatchFlushTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "batch",
		Name:      "flush_total",
		Help:      "Total batch flushes",
	}, []string{"reason"})

	BatchPendingCommands = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "batch",
		Name:      "pending_commands",
		Help:      "Commands waiting in batch",
	})

	StorageKeysTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "storage",
		Name:      "keys_total",
		Help:      "Total keys in storage",
	})

	StorageOperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "storage",
		Name:      "operations_total",
		Help:      "Total storage operations",
	}, []string{"operation"})

	StorageSnapshotSize = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "storage",
		Name:      "snapshot_size_bytes",
		Help:      "Size of last snapshot in bytes",
	})

	GRPCRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "grpc",
		Name:      "requests_total",
		Help:      "Total gRPC requests",
	}, []string{"service", "method", "code"})

	GRPCRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "grpc",
		Name:      "request_duration_seconds",
		Help:      "gRPC request duration",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20),
	}, []string{"service", "method"})

	ReadIndexTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "read_index_total",
		Help:      "Total read index requests",
	}, []string{"status"})

	ReadIndexDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "read_index_duration_seconds",
		Help:      "Read index request duration",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
	})

	WALWritesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "wal",
		Name:      "writes_total",
		Help:      "Total WAL writes",
	})

	WALWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "wal",
		Name:      "write_duration_seconds",
		Help:      "WAL write duration",
		Buckets:   prometheus.ExponentialBuckets(0.00001, 2, 20),
	})

	WALSyncDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "wal",
		Name:      "sync_duration_seconds",
		Help:      "WAL sync duration",
		Buckets:   prometheus.ExponentialBuckets(0.00001, 2, 20),
	})
)
