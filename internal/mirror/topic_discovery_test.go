package mirror

import (
	"testing"

	"github.com/gomm2/gomm2/internal/config"
	"github.com/gomm2/gomm2/internal/filter"
	"github.com/gomm2/gomm2/internal/policy"
)

// testTopicDiscovery is a minimal TopicDiscovery for unit-testing shouldReplicate.
func newTestTopicDiscovery(t *testing.T, policyName, separator string, whitelist, blacklist []string) *TopicDiscovery {
	t.Helper()
	tf, err := filter.NewTopicFilter(whitelist, blacklist)
	if err != nil {
		t.Fatalf("create topic filter: %v", err)
	}
	pol, err := policy.NewPolicy(policyName, separator)
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	return &TopicDiscovery{
		cfg: config.ReplicationConfig{
			Source: "src",
			Target: "tgt",
		},
		topicFilter: tf,
		policy:      pol,
	}
}

func TestShouldReplicate_BasicTopics(t *testing.T) {
	td := newTestTopicDiscovery(t, "default", ".", nil, nil)

	tests := []struct {
		topic string
		want  bool
	}{
		{"orders", true},
		{"payments", true},
		{"__consumer_offsets", false}, // internal
		{".hidden", false},           // internal (dot prefix caught by TopicFilter)
		{"heartbeats", false},        // heartbeats topic
		{"mm2-offset-syncs.src.internal", false}, // MM2 internal
		{"src.checkpoints.internal", false},       // MM2 internal
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			got := td.shouldReplicate(tt.topic)
			if got != tt.want {
				t.Errorf("shouldReplicate(%q) = %v, want %v", tt.topic, got, tt.want)
			}
		})
	}
}

func TestShouldReplicate_CycleDetection(t *testing.T) {
	td := newTestTopicDiscovery(t, "default", ".", nil, nil)

	// A topic that looks like it was already replicated from another cluster
	if td.shouldReplicate("other-dc.orders") {
		t.Error("shouldReplicate should reject topics with a cluster prefix (cycle detection)")
	}
	// But a topic with a dot that is not a cluster prefix should still pass if
	// the policy can extract a source — with default policy any dot is treated
	// as a separator, so "foo.bar" → source="foo", which means cycle detection
	// triggers. This is the intended behaviour for default policy.
	if td.shouldReplicate("foo.bar") {
		t.Error("shouldReplicate should reject 'foo.bar' under default policy (dot = separator)")
	}
}

func TestShouldReplicate_IdentityPolicy(t *testing.T) {
	td := newTestTopicDiscovery(t, "identity", "", nil, nil)

	// Identity policy has no prefix, so TopicSource always returns "" — no
	// cycle detection except for internal topics.
	tests := []struct {
		topic string
		want  bool
	}{
		{"orders", true},
		{"foo.bar", true},  // identity policy doesn't split on dots
		{"heartbeats", false}, // still a heartbeats topic
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			got := td.shouldReplicate(tt.topic)
			if got != tt.want {
				t.Errorf("shouldReplicate(%q) = %v, want %v", tt.topic, got, tt.want)
			}
		})
	}
}

func TestShouldReplicate_Whitelist(t *testing.T) {
	td := newTestTopicDiscovery(t, "default", ".", []string{"^orders$", "^payments.*"}, nil)

	tests := []struct {
		topic string
		want  bool
	}{
		{"orders", true},
		{"payments", true},
		{"payments-v2", true},
		{"users", false},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			got := td.shouldReplicate(tt.topic)
			if got != tt.want {
				t.Errorf("shouldReplicate(%q) = %v, want %v", tt.topic, got, tt.want)
			}
		})
	}
}

func TestShouldReplicate_Blacklist(t *testing.T) {
	td := newTestTopicDiscovery(t, "default", ".", nil, []string{"^test-.*"})

	tests := []struct {
		topic string
		want  bool
	}{
		{"orders", true},
		{"test-topic", false},
		{"test-123", false},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			got := td.shouldReplicate(tt.topic)
			if got != tt.want {
				t.Errorf("shouldReplicate(%q) = %v, want %v", tt.topic, got, tt.want)
			}
		})
	}
}
