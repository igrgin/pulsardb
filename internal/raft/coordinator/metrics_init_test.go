package coordinator

import (
	"os"
	"pulsardb/internal/metrics"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMain(m *testing.M) {
	initTestMetrics()
	os.Exit(m.Run())
}

func initTestMetrics() {

	if metrics.CommandsInFlight == nil {
		metrics.CommandsInFlight = prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_commands_in_flight"})
	}
	if metrics.WALWriteDuration == nil {
		metrics.WALWriteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "test_wal_write_duration_seconds"})
	}
	if metrics.WALWritesTotal == nil {
		metrics.WALWritesTotal = prometheus.NewCounter(prometheus.CounterOpts{Name: "test_wal_writes_total"})
	}
	if metrics.RaftMessagesTotal == nil {
		metrics.RaftMessagesTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "test_raft_messages_total"},
			[]string{"direction", "type"},
		)
	}

	if metrics.StorageSnapshotSize == nil {
		metrics.StorageSnapshotSize = prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_storage_snapshot_size"})
	}
	if metrics.RaftSnapshotsTotal == nil {
		metrics.RaftSnapshotsTotal = prometheus.NewCounter(prometheus.CounterOpts{Name: "test_raft_snapshots_total"})
	}
	if metrics.RaftSnapshotDuration == nil {
		metrics.RaftSnapshotDuration = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "test_raft_snapshot_duration_seconds"})
	}

	if metrics.CommandsInFlight == nil {
		metrics.CommandsInFlight = prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_commands_in_flight"})
	}
	if metrics.WALWriteDuration == nil {
		metrics.WALWriteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "test_wal_write_duration_seconds"})
	}
	if metrics.WALWritesTotal == nil {
		metrics.WALWritesTotal = prometheus.NewCounter(prometheus.CounterOpts{Name: "test_wal_writes_total"})
	}
	if metrics.RaftMessagesTotal == nil {

		metrics.RaftMessagesTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "test_raft_messages_total"},
			[]string{"direction", "type"},
		)
	}

	if metrics.ReadIndexTotal == nil {

		metrics.ReadIndexTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "test_read_index_requests_total"},
			[]string{"result"},
		)
	}

	if metrics.LeaseReadTotal == nil {

		metrics.LeaseReadTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "raft_lease_reads_total",
				Help: "Total lease-based reads",
			},
			[]string{"result"},
		)
	}

	if metrics.ReadIndexDuration == nil {

		metrics.ReadIndexDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "pulsardb",
			Subsystem: "raft",
			Name:      "read_index_duration_seconds",
			Help:      "Read index request duration",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
		})
	}

}
