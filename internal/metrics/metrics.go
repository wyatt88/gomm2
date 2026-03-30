// Package metrics provides Prometheus metrics for gomm2.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for gomm2.
type Metrics struct {
	RecordsReplicated   *prometheus.CounterVec
	BytesReplicated     *prometheus.CounterVec
	ReplicationLag      *prometheus.GaugeVec
	RecordAge           *prometheus.HistogramVec
	HeartbeatLatency    *prometheus.HistogramVec
	CheckpointsEmitted  *prometheus.CounterVec
	OffsetSyncsEmitted  *prometheus.CounterVec
	TopicsDiscovered    *prometheus.GaugeVec
	GroupsDiscovered    *prometheus.GaugeVec
	PollBatchSize       *prometheus.HistogramVec
	ProduceErrors       *prometheus.CounterVec
	ConsumeErrors       *prometheus.CounterVec
	DLQRecordsTotal     *prometheus.CounterVec
	RetryAttempts       *prometheus.CounterVec
	RetryExhausted      *prometheus.CounterVec
	CircuitBreakerState *prometheus.GaugeVec
	OffsetCommits       *prometheus.CounterVec
	OffsetCommitErrors  *prometheus.CounterVec
	TxnCommits          *prometheus.CounterVec
	TxnAborts           *prometheus.CounterVec
}

// New creates and registers all gomm2 Prometheus metrics.
func New() *Metrics {
	return &Metrics{
		RecordsReplicated: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomm2",
			Name:      "records_replicated_total",
			Help:      "Total number of records replicated.",
		}, []string{"source", "target", "topic"}),

		BytesReplicated: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomm2",
			Name:      "bytes_replicated_total",
			Help:      "Total bytes replicated.",
		}, []string{"source", "target", "topic"}),

		ReplicationLag: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "gomm2",
			Name:      "replication_lag_records",
			Help:      "Current replication lag per partition (number of records behind).",
		}, []string{"source", "target", "topic", "partition"}),

		RecordAge: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "gomm2",
			Name:      "record_age_seconds",
			Help:      "Age of replicated records in seconds.",
			Buckets:   []float64{0.001, 0.01, 0.1, 0.5, 1, 5, 10, 30, 60, 300},
		}, []string{"source", "target", "topic"}),

		HeartbeatLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "gomm2",
			Name:      "heartbeat_latency_seconds",
			Help:      "Heartbeat round-trip latency in seconds.",
			Buckets:   []float64{0.001, 0.01, 0.05, 0.1, 0.5, 1, 5},
		}, []string{"source", "target"}),

		CheckpointsEmitted: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomm2",
			Name:      "checkpoints_emitted_total",
			Help:      "Total checkpoint records emitted.",
		}, []string{"source", "target"}),

		OffsetSyncsEmitted: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomm2",
			Name:      "offset_syncs_emitted_total",
			Help:      "Total offset sync records emitted.",
		}, []string{"source", "target"}),

		TopicsDiscovered: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "gomm2",
			Name:      "topics_discovered",
			Help:      "Number of topics discovered for replication.",
		}, []string{"source", "target"}),

		GroupsDiscovered: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "gomm2",
			Name:      "groups_discovered",
			Help:      "Number of consumer groups discovered for checkpoint sync.",
		}, []string{"source", "target"}),

		PollBatchSize: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "gomm2",
			Name:      "poll_batch_size",
			Help:      "Number of records returned per poll.",
			Buckets:   []float64{1, 10, 50, 100, 500, 1000, 5000},
		}, []string{"source", "target"}),

		ProduceErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomm2",
			Name:      "produce_errors_total",
			Help:      "Total produce errors.",
		}, []string{"source", "target", "topic"}),

		ConsumeErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomm2",
			Name:      "consume_errors_total",
			Help:      "Total consume errors.",
		}, []string{"source", "target"}),

		DLQRecordsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomm2",
			Name:      "dlq_records_total",
			Help:      "Total records sent to the dead letter queue.",
		}, []string{"source", "target", "topic"}),

		RetryAttempts: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomm2",
			Name:      "retry_attempts_total",
			Help:      "Total retry attempts for operations.",
		}, []string{"source", "target", "operation"}),

		RetryExhausted: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomm2",
			Name:      "retry_exhausted_total",
			Help:      "Total operations that exhausted all retries.",
		}, []string{"source", "target", "operation"}),

		CircuitBreakerState: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "gomm2",
			Name:      "circuit_breaker_state",
			Help:      "Circuit breaker state: 0=closed, 1=half-open, 2=open.",
		}, []string{"source", "target", "operation"}),

		OffsetCommits: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomm2",
			Name:      "offset_commits_total",
			Help:      "Total successful offset commits.",
		}, []string{"source", "target"}),

		OffsetCommitErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomm2",
			Name:      "offset_commit_errors_total",
			Help:      "Total offset commit errors.",
		}, []string{"source", "target"}),

		TxnCommits: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomm2",
			Name:      "txn_commits_total",
			Help:      "Total successful transactional commits (exactly-once).",
		}, []string{"source", "target"}),

		TxnAborts: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomm2",
			Name:      "txn_aborts_total",
			Help:      "Total transactional aborts (exactly-once).",
		}, []string{"source", "target"}),
	}
}
