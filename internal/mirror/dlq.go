// Package mirror implements the core replication engine for gomm2.
//
// This file implements a Dead Letter Queue (DLQ) for records that cannot be
// produced to the target cluster after all retries are exhausted. Failed records
// are sent to a DLQ topic with headers indicating the original topic, partition,
// offset, and error message.
package mirror

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gomm2/gomm2/internal/config"
	"github.com/gomm2/gomm2/internal/metrics"
)

// DLQ produces failed records to a dead letter queue topic when retries are exhausted.
type DLQ struct {
	producer *kgo.Client
	topic    string
	m        *metrics.Metrics
	cfg      config.ReplicationConfig
	logger   *slog.Logger
	mu       sync.Mutex
	closed   bool
}

// NewDLQ creates a new DLQ producer for the given replication flow. The DLQ
// topic name follows the pattern: gomm2-dlq-<source>-<target>.
func NewDLQ(
	cfg config.ReplicationConfig,
	tgtCfg config.ClusterConfig,
	m *metrics.Metrics,
	logger *slog.Logger,
) (*DLQ, error) {
	if !cfg.DLQEnabled {
		return nil, nil
	}

	topic := fmt.Sprintf("gomm2-dlq-%s-%s", cfg.Source, cfg.Target)

	opts := []kgo.Opt{
		kgo.SeedBrokers(tgtCfg.BootstrapServers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(100 * time.Millisecond),
	}
	if tgtCfg.TLS != nil && tgtCfg.TLS.Enabled {
		tlsCfg, err := tgtCfg.TLS.BuildTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("DLQ target TLS: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	producer, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create DLQ producer: %w", err)
	}

	return &DLQ{
		producer: producer,
		topic:    topic,
		m:        m,
		cfg:      cfg,
		logger:   logger.With("component", "dlq", "flow", cfg.Source+"->"+cfg.Target),
	}, nil
}

// Send produces a failed record to the DLQ topic. The original record metadata
// is included as headers on the DLQ record so operators can trace its origin.
func (d *DLQ) Send(ctx context.Context, originalTopic string, partition int32, offset int64, key, value []byte, originalHeaders []kgo.RecordHeader, produceErr error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.mu.Unlock()

	headers := make([]kgo.RecordHeader, 0, len(originalHeaders)+4)
	headers = append(headers, kgo.RecordHeader{
		Key:   "gomm2.dlq.original.topic",
		Value: []byte(originalTopic),
	})
	headers = append(headers, kgo.RecordHeader{
		Key:   "gomm2.dlq.original.partition",
		Value: []byte(strconv.FormatInt(int64(partition), 10)),
	})
	headers = append(headers, kgo.RecordHeader{
		Key:   "gomm2.dlq.original.offset",
		Value: []byte(strconv.FormatInt(offset, 10)),
	})
	headers = append(headers, kgo.RecordHeader{
		Key:   "gomm2.dlq.error",
		Value: []byte(produceErr.Error()),
	})
	// Preserve original headers
	headers = append(headers, originalHeaders...)

	record := &kgo.Record{
		Topic:   d.topic,
		Key:     key,
		Value:   value,
		Headers: headers,
	}

	d.producer.Produce(ctx, record, func(r *kgo.Record, err error) {
		if err != nil {
			d.logger.Error("failed to produce to DLQ",
				"original_topic", originalTopic,
				"partition", partition,
				"offset", offset,
				"err", err,
			)
			return
		}
		d.m.DLQRecordsTotal.WithLabelValues(d.cfg.Source, d.cfg.Target, originalTopic).Inc()
		d.logger.Warn("record sent to DLQ",
			"original_topic", originalTopic,
			"partition", partition,
			"offset", offset,
			"error", produceErr.Error(),
		)
	})
}

// Topic returns the DLQ topic name.
func (d *DLQ) Topic() string {
	return d.topic
}

// Close shuts down the DLQ producer, flushing any pending records.
func (d *DLQ) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return
	}
	d.closed = true
	if d.producer != nil {
		d.producer.Close()
	}
	d.logger.Info("DLQ producer closed", "topic", d.topic)
}
