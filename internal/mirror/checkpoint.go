package mirror

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gomm2/gomm2/internal/config"
	"github.com/gomm2/gomm2/internal/filter"
	"github.com/gomm2/gomm2/internal/kafka"
	"github.com/gomm2/gomm2/internal/metrics"
	"github.com/gomm2/gomm2/internal/offset"
	"github.com/gomm2/gomm2/internal/policy"
	"github.com/gomm2/gomm2/pkg/types"
)

// Checkpoint emits checkpoint records that translate consumer group offsets
// between source and target clusters. Corresponds to Java MM2's
// MirrorCheckpointConnector + MirrorCheckpointTask.
type Checkpoint struct {
	cfg         config.ReplicationConfig
	srcCfg      config.ClusterConfig
	tgtCfg      config.ClusterConfig
	topicFilter *filter.TopicFilter
	groupFilter *filter.GroupFilter
	policy      policy.ReplicationPolicy
	syncStore   *offset.SyncStore
	m           *metrics.Metrics
	logger      *slog.Logger

	srcAdmin *kgo.Client
	producer *kgo.Client

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewCheckpoint creates a new Checkpoint emitter.
func NewCheckpoint(
	cfg config.ReplicationConfig,
	srcCfg, tgtCfg config.ClusterConfig,
	syncStore *offset.SyncStore,
	m *metrics.Metrics,
	logger *slog.Logger,
) (*Checkpoint, error) {
	topicFilter, err := filter.NewTopicFilter(cfg.TopicFilter.Whitelist, cfg.TopicFilter.Blacklist)
	if err != nil {
		return nil, fmt.Errorf("create topic filter: %w", err)
	}
	groupFilter, err := filter.NewGroupFilter(cfg.GroupFilter.Whitelist, cfg.GroupFilter.Blacklist)
	if err != nil {
		return nil, fmt.Errorf("create group filter: %w", err)
	}
	pol, err := policy.NewPolicy(cfg.ReplicationPolicy, cfg.Separator)
	if err != nil {
		return nil, fmt.Errorf("create replication policy: %w", err)
	}
	return &Checkpoint{
		cfg:         cfg,
		srcCfg:      srcCfg,
		tgtCfg:      tgtCfg,
		topicFilter: topicFilter,
		groupFilter: groupFilter,
		policy:      pol,
		syncStore:   syncStore,
		m:           m,
		logger:      logger.With("component", "checkpoint", "flow", cfg.Source+"->"+cfg.Target),
	}, nil
}

// Start begins checkpoint emission.
func (c *Checkpoint) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("checkpoint already running")
	}
	if !config.BoolDefault(c.cfg.EmitCheckpoints, true) {
		c.logger.Info("checkpoint emission disabled")
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	// Source admin client using shared builder
	srcOpts, err := kafka.BuildClientOpts(c.srcCfg)
	if err != nil {
		cancel()
		return fmt.Errorf("build source client opts: %w", err)
	}
	srcAdmin, err := kgo.NewClient(srcOpts...)
	if err != nil {
		cancel()
		return fmt.Errorf("create source admin client: %w", err)
	}
	c.srcAdmin = srcAdmin

	// Target producer using shared builder
	tgtOpts, err := kafka.BuildClientOpts(c.tgtCfg)
	if err != nil {
		srcAdmin.Close()
		cancel()
		return fmt.Errorf("build target client opts: %w", err)
	}
	tgtOpts = append(tgtOpts, kgo.RequiredAcks(kgo.AllISRAcks()))

	producer, err := kgo.NewClient(tgtOpts...)
	if err != nil {
		srcAdmin.Close()
		cancel()
		return fmt.Errorf("create checkpoint producer: %w", err)
	}
	c.producer = producer
	c.running = true

	c.wg.Add(1)
	go c.emitLoop(ctx)

	c.logger.Info("checkpoint emission started", "interval", c.cfg.CheckpointInterval.Duration)
	return nil
}

// Stop shuts down the checkpoint emitter.
func (c *Checkpoint) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return
	}
	c.cancel()
	c.wg.Wait()
	if c.srcAdmin != nil {
		c.srcAdmin.Close()
	}
	if c.producer != nil {
		c.producer.Close()
	}
	c.running = false
	c.logger.Info("checkpoint emission stopped")
}

func (c *Checkpoint) emitLoop(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.CheckpointInterval.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.emitCheckpoints(ctx); err != nil {
				c.logger.Error("failed to emit checkpoints", "err", err)
			}
		}
	}
}

func (c *Checkpoint) emitCheckpoints(ctx context.Context) error {
	if !c.syncStore.IsReady() {
		c.logger.Debug("sync store not ready, skipping checkpoint emission")
		return nil
	}

	// List consumer groups from source
	admin := kadm.NewClient(c.srcAdmin)
	listed, err := admin.ListGroups(ctx)
	if err != nil {
		return fmt.Errorf("list consumer groups: %w", err)
	}

	for _, group := range listed.Groups() {
		if !c.groupFilter.ShouldReplicate(group) {
			continue
		}

		// Fetch offsets for this group
		offsets, err := admin.FetchOffsets(ctx, group)
		if err != nil {
			c.logger.Warn("failed to fetch offsets", "group", group, "err", err)
			continue
		}

		offsets.Each(func(o kadm.OffsetResponse) {
			if !c.topicFilter.ShouldReplicate(o.Topic) {
				return
			}

			tp := types.TopicPartition{Topic: o.Topic, Partition: o.Partition}
			downstream, ok := c.syncStore.TranslateDownstream(tp, o.At)
			if !ok {
				return
			}
			if downstream < 0 {
				return // offset too far in the past
			}

			cp := types.Checkpoint{
				ConsumerGroupID:  group,
				TopicPartition:   tp,
				UpstreamOffset:   o.At,
				DownstreamOffset: downstream,
				Metadata:         "",
			}

			record := &kgo.Record{
				Topic: c.policy.CheckpointsTopic(c.cfg.Source),
				Key:   cp.SerializeKey(),
				Value: cp.SerializeValue(),
			}

			c.producer.Produce(ctx, record, func(_ *kgo.Record, err error) {
				if err != nil {
					c.logger.Error("failed to produce checkpoint",
						"group", group, "tp", tp, "err", err)
					return
				}
				c.m.CheckpointsEmitted.WithLabelValues(c.cfg.Source, c.cfg.Target).Inc()
			})
		})
	}

	return nil
}
