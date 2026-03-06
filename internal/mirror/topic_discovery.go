package mirror

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gomm2/gomm2/internal/admin"
	"github.com/gomm2/gomm2/internal/config"
	"github.com/gomm2/gomm2/internal/filter"
	"github.com/gomm2/gomm2/internal/metrics"
	"github.com/gomm2/gomm2/internal/policy"
)

// TopicDiscovery periodically lists topics on the source cluster, filters them
// through TopicFilter and ReplicationPolicy, creates missing topics on the
// target cluster with matching partition counts, increases partitions if the
// source has more, and (optionally) syncs topic configs.
//
// This mirrors the behaviour of Java MM2's
// MirrorSourceConnector.refreshTopicPartitions().
type TopicDiscovery struct {
	cfg         config.ReplicationConfig
	srcCfg      config.ClusterConfig
	tgtCfg      config.ClusterConfig
	topicFilter *filter.TopicFilter
	policy      policy.ReplicationPolicy
	m           *metrics.Metrics
	logger      *slog.Logger

	source  *Source // reference to source replicator for topic subscription

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewTopicDiscovery creates a new TopicDiscovery for the given replication flow.
func NewTopicDiscovery(
	cfg config.ReplicationConfig,
	srcCfg, tgtCfg config.ClusterConfig,
	m *metrics.Metrics,
	source *Source,
	logger *slog.Logger,
) (*TopicDiscovery, error) {
	topicFilter, err := filter.NewTopicFilter(cfg.TopicFilter.Whitelist, cfg.TopicFilter.Blacklist)
	if err != nil {
		return nil, fmt.Errorf("create topic filter: %w", err)
	}
	pol, err := policy.NewPolicy(cfg.ReplicationPolicy, cfg.Separator)
	if err != nil {
		return nil, fmt.Errorf("create replication policy: %w", err)
	}
	return &TopicDiscovery{
		cfg:         cfg,
		srcCfg:      srcCfg,
		tgtCfg:      tgtCfg,
		topicFilter: topicFilter,
		policy:      pol,
		m:           m,
		source:      source,
		logger:      logger.With("component", "topic_discovery", "flow", cfg.Source+"->"+cfg.Target),
	}, nil
}

// Start begins the periodic topic discovery loop.
func (td *TopicDiscovery) Start(ctx context.Context) error {
	td.mu.Lock()
	defer td.mu.Unlock()

	if td.running {
		return fmt.Errorf("topic discovery already running")
	}

	ctx, cancel := context.WithCancel(ctx)
	td.cancel = cancel
	td.running = true

	td.wg.Add(1)
	go td.loop(ctx)

	td.logger.Info("topic discovery started", "interval", td.cfg.RefreshTopicsInterval.Duration)
	return nil
}

// Stop shuts down the topic discovery loop.
func (td *TopicDiscovery) Stop() {
	td.mu.Lock()
	defer td.mu.Unlock()

	if !td.running {
		return
	}
	td.cancel()
	td.wg.Wait()
	td.running = false
	td.logger.Info("topic discovery stopped")
}

func (td *TopicDiscovery) loop(ctx context.Context) {
	defer td.wg.Done()

	// Run immediately on start, then periodically
	td.refresh(ctx)

	ticker := time.NewTicker(td.cfg.RefreshTopicsInterval.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			td.refresh(ctx)
		}
	}
}

// refresh performs a single round of topic discovery and creation.
func (td *TopicDiscovery) refresh(ctx context.Context) {
	srcAdmin, err := admin.NewClient(ctx, td.srcCfg)
	if err != nil {
		td.logger.Error("failed to create source admin client", "err", err)
		return
	}
	defer srcAdmin.Close()

	tgtAdmin, err := admin.NewClient(ctx, td.tgtCfg)
	if err != nil {
		td.logger.Error("failed to create target admin client", "err", err)
		return
	}
	defer tgtAdmin.Close()

	// List source topics with full details
	srcTopics, err := srcAdmin.ListTopicsDetails(ctx)
	if err != nil {
		td.logger.Error("failed to list source topics", "err", err)
		return
	}

	// List target topics for comparison
	tgtTopics, err := tgtAdmin.ListTopicsDetails(ctx)
	if err != nil {
		td.logger.Error("failed to list target topics", "err", err)
		return
	}

	var discovered int
	var replicableTopics []string
	for _, srcDetail := range srcTopics.Sorted() {
		srcTopic := srcDetail.Topic

		// Skip topics that should not be replicated
		if !td.shouldReplicate(srcTopic) {
			continue
		}

		discovered++
		replicableTopics = append(replicableTopics, srcTopic)
		remoteTopic := td.policy.FormatRemoteTopic(td.cfg.Source, srcTopic)
		srcPartitions := int32(len(srcDetail.Partitions))

		// Check if target topic exists
		tgtDetail, exists := tgtTopics[remoteTopic]
		if !exists {
			// Create the topic on target
			td.logger.Info("creating topic on target",
				"source_topic", srcTopic,
				"target_topic", remoteTopic,
				"partitions", srcPartitions,
			)
			if err := tgtAdmin.CreateTopic(ctx, remoteTopic, srcPartitions, td.cfg.ReplicationFactor, nil); err != nil {
				td.logger.Error("failed to create topic on target",
					"topic", remoteTopic, "err", err)
				continue
			}
		} else {
			// Topic exists — ensure partition count matches
			tgtPartitions := int32(len(tgtDetail.Partitions))
			if srcPartitions > tgtPartitions {
				td.logger.Info("increasing partition count on target",
					"topic", remoteTopic,
					"from", tgtPartitions,
					"to", srcPartitions,
				)
				if err := tgtAdmin.EnsurePartitions(ctx, remoteTopic, srcPartitions); err != nil {
					td.logger.Error("failed to increase partitions",
						"topic", remoteTopic, "err", err)
				}
			}
		}

		// Sync topic configs if enabled
		if config.BoolDefault(td.cfg.SyncTopicConfigs, true) {
			if err := admin.SyncTopicConfigs(ctx, srcAdmin, tgtAdmin, srcTopic, remoteTopic, nil, td.logger); err != nil {
				td.logger.Error("failed to sync topic configs",
					"source_topic", srcTopic,
					"target_topic", remoteTopic,
					"err", err,
				)
			}
		}
	}

	td.m.TopicsDiscovered.WithLabelValues(td.cfg.Source, td.cfg.Target).Set(float64(discovered))
	td.logger.Debug("topic discovery refresh complete", "discovered", discovered)

	// Notify the source replicator about discovered topics so it subscribes
	if td.source != nil && len(replicableTopics) > 0 {
		td.source.UpdateTopics(replicableTopics)
	}
}

// shouldReplicate checks whether a topic should be replicated by applying
// the topic filter, skipping internal/mm2 topics, and detecting cycles.
func (td *TopicDiscovery) shouldReplicate(topic string) bool {
	// Skip MM2 internal topics
	if td.policy.IsInternalTopic(topic) {
		return false
	}

	// Skip heartbeat topics
	if td.policy.IsHeartbeatsTopic(topic) {
		return false
	}

	// Detect cycles: if the topic already appears to originate from some
	// other cluster (i.e. it has a source prefix), skip it to avoid
	// re-replicating already-mirrored data.
	if td.policy.TopicSource(topic) != "" {
		return false
	}

	// Apply the user-defined topic filter (whitelist/blacklist)
	return td.topicFilter.ShouldReplicate(topic)
}
