package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	factory promauto.Factory

	RaftIsLeader         prometheus.Gauge
	RaftTerm             prometheus.Gauge
	RaftCommitIndex      prometheus.Gauge
	RaftAppliedIndex     prometheus.Gauge
	RaftSnapshotIndex    prometheus.Gauge
	RaftPeersTotal       prometheus.Gauge
	RaftMessagesTotal    *prometheus.CounterVec
	RaftMessageErrors    *prometheus.CounterVec
	RaftProposalsTotal   prometheus.Counter
	RaftProposalsFailed  prometheus.Counter
	RaftSnapshotsTotal   prometheus.Counter
	RaftSnapshotDuration prometheus.Histogram
	ReadIndexTotal       *prometheus.CounterVec
	ReadIndexDuration    prometheus.Histogram

	CommandsTotal    *prometheus.CounterVec
	CommandDuration  *prometheus.HistogramVec
	CommandsInFlight prometheus.Gauge

	BatchSize            prometheus.Histogram
	BatchFlushTotal      *prometheus.CounterVec
	BatchPendingCommands prometheus.Gauge

	StorageKeysTotal       prometheus.Gauge
	StorageOperationsTotal *prometheus.CounterVec
	StorageSnapshotSize    prometheus.Gauge

	GRPCRequestsTotal   *prometheus.CounterVec
	GRPCRequestDuration *prometheus.HistogramVec

	WALWritesTotal   prometheus.Counter
	WALWriteDuration prometheus.Histogram
	WALSyncDuration  *prometheus.HistogramVec
)

func Init(nodeID uint64) {
	wrappedRegistry := prometheus.WrapRegistererWith(
		prometheus.Labels{"node_id": fmt.Sprintf("%d", nodeID)},
		prometheus.DefaultRegisterer,
	)
	factory = promauto.With(wrappedRegistry)

	RaftIsLeader = factory.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "is_leader",
		Help:      "Whether this node is the Raft leader (1=leader, 0=follower)",
	})

	RaftTerm = factory.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "term",
		Help:      "Current Raft term",
	})

	RaftCommitIndex = factory.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "commit_index",
		Help:      "Current Raft commit index",
	})

	RaftAppliedIndex = factory.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "applied_index",
		Help:      "Last applied Raft index",
	})

	RaftSnapshotIndex = factory.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "snapshot_index",
		Help:      "Last snapshot index",
	})

	RaftPeersTotal = factory.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "peers_total",
		Help:      "Number of Raft peers",
	})

	RaftMessagesTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "messages_total",
		Help:      "Total Raft messages sent/received",
	}, []string{"direction", "type"})

	RaftMessageErrors = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "message_errors_total",
		Help:      "Total Raft message errors",
	}, []string{"peer_id"})

	RaftProposalsTotal = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "proposals_total",
		Help:      "Total proposals submitted",
	})

	RaftProposalsFailed = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "proposals_failed_total",
		Help:      "Total failed proposals",
	})

	RaftSnapshotsTotal = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "snapshots_total",
		Help:      "Total snapshots taken",
	})

	RaftSnapshotDuration = factory.NewHistogram(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "snapshot_duration_seconds",
		Help:      "Time to create snapshot",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
	})

	ReadIndexTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "read_index_total",
		Help:      "Total read index requests",
	}, []string{"status"})

	ReadIndexDuration = factory.NewHistogram(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "raft",
		Name:      "read_index_duration_seconds",
		Help:      "Read index request duration",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
	})

	CommandsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "command",
		Name:      "total",
		Help:      "Total commands processed",
	}, []string{"type", "status"})

	CommandDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "command",
		Name:      "duration_seconds",
		Help:      "Command processing duration",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20),
	}, []string{"type"})

	CommandsInFlight = factory.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "command",
		Name:      "in_flight",
		Help:      "Commands currently being processed",
	})

	BatchSize = factory.NewHistogram(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "batch",
		Name:      "size",
		Help:      "Number of commands per batch",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
	})

	BatchFlushTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "batch",
		Name:      "flush_total",
		Help:      "Total batch flushes",
	}, []string{"reason"})

	BatchPendingCommands = factory.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "batch",
		Name:      "pending_commands",
		Help:      "Commands waiting in batch",
	})

	StorageKeysTotal = factory.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "storage",
		Name:      "keys_total",
		Help:      "Total keys in storage",
	})

	StorageOperationsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "storage",
		Name:      "operations_total",
		Help:      "Total storage operations",
	}, []string{"operation"})

	StorageSnapshotSize = factory.NewGauge(prometheus.GaugeOpts{
		Namespace: "pulsardb",
		Subsystem: "storage",
		Name:      "snapshot_size_bytes",
		Help:      "Size of last snapshot in bytes",
	})

	GRPCRequestsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "grpc",
		Name:      "requests_total",
		Help:      "Total gRPC requests",
	}, []string{"service", "method", "code"})

	GRPCRequestDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "grpc",
		Name:      "request_duration_seconds",
		Help:      "gRPC request duration",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20),
	}, []string{"service", "method"})

	WALWritesTotal = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "pulsardb",
		Subsystem: "wal",
		Name:      "writes_total",
		Help:      "Total WAL writes",
	})

	WALWriteDuration = factory.NewHistogram(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "wal",
		Name:      "write_duration_seconds",
		Help:      "WAL write duration",
		Buckets:   prometheus.ExponentialBuckets(0.00001, 2, 20),
	})

	WALSyncDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "pulsardb",
		Subsystem: "wal",
		Name:      "sync_duration_seconds",
		Help:      "WAL sync duration",
		Buckets:   prometheus.ExponentialBuckets(0.00001, 2, 20),
	}, []string{"operation"})
}
