// Package policy implements replication policies for topic naming.
package policy

import (
	"fmt"
	"strings"
)

// ReplicationPolicy defines how topics are named in the target cluster and
// how to determine the source cluster from a topic name.
type ReplicationPolicy interface {
	// FormatRemoteTopic returns the topic name on the target cluster.
	FormatRemoteTopic(sourceCluster, topic string) string

	// TopicSource returns the source cluster alias from a remote topic name, or "" if not a remote topic.
	TopicSource(topic string) string

	// UpstreamTopic extracts the original topic name from a remote topic name.
	UpstreamTopic(topic string) string

	// IsInternalTopic returns true if the topic is a MM2 internal topic.
	IsInternalTopic(topic string) bool

	// IsHeartbeatsTopic returns true if topic is a heartbeats topic.
	IsHeartbeatsTopic(topic string) bool

	// OffsetSyncsTopic returns the offset syncs topic name for a cluster.
	OffsetSyncsTopic(clusterAlias string) string

	// CheckpointsTopic returns the checkpoints topic name for a cluster.
	CheckpointsTopic(clusterAlias string) string

	// HeartbeatsTopic returns the heartbeats topic name.
	HeartbeatsTopic() string
}

// DefaultPolicy prepends source cluster alias to topic names using a configurable separator.
// e.g., source cluster "us-west" + topic "orders" → "us-west.orders"
type DefaultPolicy struct {
	Separator string
}

// NewDefaultPolicy creates a DefaultPolicy with the given separator (defaults to ".").
func NewDefaultPolicy(separator string) *DefaultPolicy {
	if separator == "" {
		separator = "."
	}
	return &DefaultPolicy{Separator: separator}
}

func (p *DefaultPolicy) FormatRemoteTopic(sourceCluster, topic string) string {
	return sourceCluster + p.Separator + topic
}

func (p *DefaultPolicy) TopicSource(topic string) string {
	idx := strings.Index(topic, p.Separator)
	if idx < 0 {
		return ""
	}
	return topic[:idx]
}

func (p *DefaultPolicy) UpstreamTopic(topic string) string {
	source := p.TopicSource(topic)
	if source == "" {
		return ""
	}
	return topic[len(source)+len(p.Separator):]
}

func (p *DefaultPolicy) IsInternalTopic(topic string) bool {
	return strings.HasPrefix(topic, "mm2") && strings.HasSuffix(topic, p.Separator+"internal") ||
		strings.HasSuffix(topic, p.Separator+"checkpoints"+p.Separator+"internal")
}

func (p *DefaultPolicy) IsHeartbeatsTopic(topic string) bool {
	return topic == "heartbeats"
}

func (p *DefaultPolicy) OffsetSyncsTopic(clusterAlias string) string {
	return fmt.Sprintf("mm2-offset-syncs%s%s%sinternal", p.Separator, clusterAlias, p.Separator)
}

func (p *DefaultPolicy) CheckpointsTopic(clusterAlias string) string {
	return fmt.Sprintf("%s%scheckpoints%sinternal", clusterAlias, p.Separator, p.Separator)
}

func (p *DefaultPolicy) HeartbeatsTopic() string {
	return "heartbeats"
}

// IdentityPolicy passes topic names through unchanged (no prefix).
// WARNING: can cause replication cycles in bidirectional setups.
type IdentityPolicy struct{}

func NewIdentityPolicy() *IdentityPolicy {
	return &IdentityPolicy{}
}

func (p *IdentityPolicy) FormatRemoteTopic(_, topic string) string {
	return topic
}

func (p *IdentityPolicy) TopicSource(string) string {
	return ""
}

func (p *IdentityPolicy) UpstreamTopic(topic string) string {
	return topic
}

func (p *IdentityPolicy) IsInternalTopic(topic string) bool {
	return strings.HasPrefix(topic, "mm2") && strings.HasSuffix(topic, ".internal") ||
		strings.HasSuffix(topic, ".checkpoints.internal")
}

func (p *IdentityPolicy) IsHeartbeatsTopic(topic string) bool {
	return topic == "heartbeats"
}

func (p *IdentityPolicy) OffsetSyncsTopic(clusterAlias string) string {
	return fmt.Sprintf("mm2-offset-syncs.%s.internal", clusterAlias)
}

func (p *IdentityPolicy) CheckpointsTopic(clusterAlias string) string {
	return fmt.Sprintf("%s.checkpoints.internal", clusterAlias)
}

func (p *IdentityPolicy) HeartbeatsTopic() string {
	return "heartbeats"
}

// NewPolicy creates a ReplicationPolicy by name.
func NewPolicy(name, separator string) (ReplicationPolicy, error) {
	switch name {
	case "default", "":
		return NewDefaultPolicy(separator), nil
	case "identity":
		return NewIdentityPolicy(), nil
	default:
		return nil, fmt.Errorf("unknown replication policy: %s", name)
	}
}
