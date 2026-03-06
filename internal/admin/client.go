// Package admin wraps Kafka admin operations for gomm2.
package admin

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/gomm2/gomm2/internal/config"
	"github.com/gomm2/gomm2/internal/kafka"
	"github.com/gomm2/gomm2/pkg/types"
)

// Client wraps kadm.Client for admin operations.
type Client struct {
	inner *kadm.Client
	raw   *kgo.Client
}

// NewClient creates an admin client for the given cluster config.
// It uses the shared kafka.BuildClientOpts to configure TLS and SASL.
func NewClient(ctx context.Context, cc config.ClusterConfig) (*Client, error) {
	opts, err := kafka.BuildClientOpts(cc)
	if err != nil {
		return nil, fmt.Errorf("build client opts: %w", err)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &Client{
		inner: kadm.NewClient(client),
		raw:   client,
	}, nil
}

// Close releases admin client resources.
func (c *Client) Close() {
	c.raw.Close()
}

// Inner returns the underlying kadm.Client for advanced operations.
func (c *Client) Inner() *kadm.Client {
	return c.inner
}

// ListTopics returns all topic names.
func (c *Client) ListTopics(ctx context.Context) ([]string, error) {
	topics, err := c.inner.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// ListTopicPartitions returns all partition IDs for a given topic.
func (c *Client) ListTopicPartitions(ctx context.Context, topic string) ([]int32, error) {
	topics, err := c.inner.ListTopics(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list topic partitions: %w", err)
	}
	detail, ok := topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	parts := make([]int32, 0, len(detail.Partitions))
	for _, p := range detail.Partitions.Sorted() {
		parts = append(parts, p.Partition)
	}
	return parts, nil
}

// ListTopicsDetails returns full topic details (kadm.TopicDetails) for all topics.
func (c *Client) ListTopicsDetails(ctx context.Context) (kadm.TopicDetails, error) {
	topics, err := c.inner.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	return topics, nil
}

// TopicPartitions returns all partitions for the given topics.
func (c *Client) TopicPartitions(ctx context.Context, topics []string) ([]types.TopicPartition, error) {
	details, err := c.inner.ListTopics(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("describe topics: %w", err)
	}
	var tps []types.TopicPartition
	for _, detail := range details {
		for _, p := range detail.Partitions.Sorted() {
			tps = append(tps, types.TopicPartition{
				Topic:     detail.Topic,
				Partition: p.Partition,
			})
		}
	}
	return tps, nil
}

// CreateTopic creates a topic on the cluster.
func (c *Client) CreateTopic(ctx context.Context, name string, partitions int32, replicationFactor int16, configs map[string]*string) error {
	resp, err := c.inner.CreateTopics(ctx, partitions, replicationFactor, configs, name)
	if err != nil {
		return fmt.Errorf("create topic %s: %w", name, err)
	}
	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("create topic %s: %w", name, r.Err)
		}
	}
	return nil
}

// CreateCompactedTopic creates a single-partition compacted topic (for internal topics).
func (c *Client) CreateCompactedTopic(ctx context.Context, name string, replicationFactor int16) error {
	cleanupPolicy := "compact"
	return c.CreateTopic(ctx, name, 1, replicationFactor, map[string]*string{
		"cleanup.policy": &cleanupPolicy,
	})
}

// EnsurePartitions increases the partition count if needed.
func (c *Client) EnsurePartitions(ctx context.Context, topic string, count int32) error {
	resp, err := c.inner.CreatePartitions(ctx, int(count), topic)
	if err != nil {
		return fmt.Errorf("create partitions for %s: %w", topic, err)
	}
	for _, r := range resp {
		if r.Err != nil {
			// Ignore "partition count is already X" errors
			return nil
		}
	}
	return nil
}

// ListConsumerGroups lists all consumer groups on the cluster.
func (c *Client) ListConsumerGroups(ctx context.Context) ([]string, error) {
	groups, err := c.inner.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list consumer groups: %w", err)
	}
	names := groups.Groups()
	sort.Strings(names)
	return names, nil
}

// FetchGroupOffsets fetches committed offsets for a consumer group.
func (c *Client) FetchGroupOffsets(ctx context.Context, group string) (map[types.TopicPartition]int64, error) {
	offsets, err := c.inner.FetchOffsets(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("fetch offsets for group %s: %w", group, err)
	}
	result := make(map[types.TopicPartition]int64)
	offsets.Each(func(o kadm.OffsetResponse) {
		tp := types.TopicPartition{Topic: o.Topic, Partition: o.Partition}
		result[tp] = o.At
	})
	return result, nil
}

// DescribeTopicConfigs returns the non-default configuration entries for a topic.
func (c *Client) DescribeTopicConfigs(ctx context.Context, topic string) (map[string]string, error) {
	resources, err := c.inner.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("describe topic configs for %s: %w", topic, err)
	}
	result := make(map[string]string)
	for _, rc := range resources {
		if rc.Err != nil {
			return nil, fmt.Errorf("describe topic config %s: %w", topic, rc.Err)
		}
		for _, entry := range rc.Configs {
			// Only include non-default, non-sensitive values
			if entry.Source == kmsg.ConfigSourceDynamicTopicConfig {
				if entry.Value != nil {
					result[entry.Key] = *entry.Value
				}
			}
		}
	}
	return result, nil
}

// AlterTopicConfigs sets topic configuration entries.
func (c *Client) AlterTopicConfigs(ctx context.Context, topic string, configs map[string]*string) error {
	cfgs := make([]kadm.AlterConfig, 0, len(configs))
	for k, v := range configs {
		ac := kadm.AlterConfig{Name: k, Op: kadm.SetConfig}
		if v != nil {
			ac.Value = v
		}
		cfgs = append(cfgs, ac)
	}
	resp, err := c.inner.AlterTopicConfigs(ctx, cfgs, topic)
	if err != nil {
		return fmt.Errorf("alter topic configs for %s: %w", topic, err)
	}
	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("alter topic config %s: %w", topic, r.Err)
		}
	}
	return nil
}
