// Package offset — sync_reader.go consumes offset-sync records from the
// mm2-offset-syncs.<cluster>.internal topic at startup, populating the
// SyncStore. It reads to the end of the topic before marking the store as
// ready, matching Java MM2's OffsetSyncStore behaviour.
package offset

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gomm2/gomm2/internal/config"
	"github.com/gomm2/gomm2/internal/kafka"
	"github.com/gomm2/gomm2/pkg/types"
)

// SyncReader consumes the offset-syncs internal topic and feeds records
// into a SyncStore. Call ReadToEnd to consume all existing records (blocking),
// then optionally call Follow to keep consuming in the background.
type SyncReader struct {
	topic    string
	consumer *kgo.Client
	store    *SyncStore
	logger   *slog.Logger
}

// NewSyncReader creates a SyncReader for the given offset-syncs topic.
// It configures a franz-go consumer that starts from the beginning of the topic.
func NewSyncReader(ctx context.Context, cc config.ClusterConfig, topic string, store *SyncStore, logger *slog.Logger) (*SyncReader, error) {
	opts, err := kafka.BuildClientOpts(cc)
	if err != nil {
		return nil, fmt.Errorf("build client opts for sync reader: %w", err)
	}
	opts = append(opts,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	consumer, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create sync reader consumer: %w", err)
	}

	return &SyncReader{
		topic:    topic,
		consumer: consumer,
		store:    store,
		logger:   logger.With("component", "sync_reader", "topic", topic),
	}, nil
}

// ReadToEnd consumes records from the offset-syncs topic until the consumer
// has caught up with the high-water marks of all partitions. Once caught up
// it calls store.MarkReadToEnd() so that downstream components (like
// Checkpoint) know the store is ready.
//
// This blocks until reading is complete, the context is cancelled, or a
// timeout is reached.
func (sr *SyncReader) ReadToEnd(ctx context.Context, timeout time.Duration) error {
	sr.logger.Info("reading offset-syncs topic to end", "timeout", timeout)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var totalRecords int64
	for {
		fetches := sr.consumer.PollFetches(ctx)

		// Check for context cancellation (timeout or parent cancel)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			// Timeout reached — mark as ready with whatever we have
			sr.logger.Warn("offset-syncs read timed out, marking ready with partial data",
				"records_read", totalRecords)
			sr.store.MarkReadToEnd()
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("context cancelled while reading offset-syncs: %w", ctx.Err())
		}

		// Process any errors
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				sr.logger.Error("sync reader fetch error", "topic", e.Topic, "partition", e.Partition, "err", e.Err)
			}
		}

		records := fetches.Records()
		for _, r := range records {
			os, err := types.DeserializeOffsetSync(r.Key, r.Value)
			if err != nil {
				sr.logger.Warn("skipping malformed offset sync record",
					"partition", r.Partition, "offset", r.Offset, "err", err)
				continue
			}
			sr.store.HandleSync(os)
			totalRecords++
		}

		// Check if we've reached the end. When PollFetches returns no
		// records and no errors, and the context hasn't expired, we've
		// caught up with the current high-water marks.
		if len(records) == 0 && len(fetches.Errors()) == 0 {
			sr.logger.Info("offset-syncs topic read to end", "records_read", totalRecords)
			sr.store.MarkReadToEnd()
			return nil
		}
	}
}

// Follow consumes the offset-syncs topic continuously, feeding new records
// into the store in real time. This should be called after ReadToEnd in a
// separate goroutine. It returns when the context is cancelled.
func (sr *SyncReader) Follow(ctx context.Context) {
	sr.logger.Info("following offset-syncs topic for new records")
	for {
		fetches := sr.consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			sr.logger.Info("sync reader follow stopped (context cancelled)")
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				sr.logger.Error("sync reader follow error", "topic", e.Topic, "partition", e.Partition, "err", e.Err)
			}
		}
		for _, r := range fetches.Records() {
			os, err := types.DeserializeOffsetSync(r.Key, r.Value)
			if err != nil {
				sr.logger.Warn("skipping malformed offset sync record",
					"partition", r.Partition, "offset", r.Offset, "err", err)
				continue
			}
			sr.store.HandleSync(os)
		}
	}
}

// Close releases the underlying consumer.
func (sr *SyncReader) Close() {
	sr.consumer.Close()
	sr.logger.Info("sync reader closed")
}
