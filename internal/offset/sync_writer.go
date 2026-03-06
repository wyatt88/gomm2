// Package offset — sync_writer.go produces offset sync records to the
// mm2-offset-syncs.<cluster>.internal topic so that they persist across
// restarts. One writer is created per replication flow.
package offset

import (
	"context"
	"fmt"
	"log/slog"
	gosync "sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gomm2/gomm2/internal/config"
	"github.com/gomm2/gomm2/internal/kafka"
	"github.com/gomm2/gomm2/pkg/types"
)

// SyncWriter produces offset-sync records to the offset-syncs internal topic.
type SyncWriter struct {
	topic    string
	producer *kgo.Client
	logger   *slog.Logger

	mu      gosync.Mutex
	closed  bool
}

// NewSyncWriter creates a SyncWriter that writes to the given offset-syncs topic
// on the cluster described by cc.
func NewSyncWriter(ctx context.Context, cc config.ClusterConfig, topic string, logger *slog.Logger) (*SyncWriter, error) {
	opts, err := kafka.BuildClientOpts(cc)
	if err != nil {
		return nil, fmt.Errorf("build client opts for sync writer: %w", err)
	}
	opts = append(opts,
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(64*1024), // small batches for internal topic
	)

	producer, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create sync writer producer: %w", err)
	}

	return &SyncWriter{
		topic:    topic,
		producer: producer,
		logger:   logger.With("component", "sync_writer", "topic", topic),
	}, nil
}

// Write produces a single offset-sync record to the internal topic.
// It blocks until the record is acknowledged or an error occurs.
func (sw *SyncWriter) Write(ctx context.Context, os types.OffsetSync) error {
	sw.mu.Lock()
	if sw.closed {
		sw.mu.Unlock()
		return fmt.Errorf("sync writer is closed")
	}
	sw.mu.Unlock()

	record := &kgo.Record{
		Topic: sw.topic,
		Key:   os.SerializeKey(),
		Value: os.SerializeValue(),
	}

	var produceErr error
	var wg gosync.WaitGroup
	wg.Add(1)
	sw.producer.Produce(ctx, record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		produceErr = err
	})
	wg.Wait()

	if produceErr != nil {
		return fmt.Errorf("produce offset sync for %s: %w", os.TopicPartition, produceErr)
	}

	sw.logger.Debug("offset sync written",
		"tp", os.TopicPartition,
		"upstream", os.UpstreamOffset,
		"downstream", os.DownstreamOffset,
	)
	return nil
}

// Close flushes pending records and releases the underlying producer.
func (sw *SyncWriter) Close() {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.closed {
		return
	}
	sw.closed = true
	sw.producer.Close()
	sw.logger.Info("sync writer closed")
}
