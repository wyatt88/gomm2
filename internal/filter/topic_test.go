package filter

import (
	"testing"
)

// ---------------------------------------------------------------------------
// TopicFilter
// ---------------------------------------------------------------------------

func TestTopicFilterWhitelistOnly(t *testing.T) {
	tf, err := NewTopicFilter([]string{"^orders.*", "^payments.*"}, nil)
	if err != nil {
		t.Fatalf("NewTopicFilter: %v", err)
	}

	tests := []struct {
		topic string
		want  bool
	}{
		{"orders", true},
		{"orders-v2", true},
		{"payments", true},
		{"payments.refunds", true},
		{"events", false},
		{"user-orders", false}, // "orders" not at start
	}
	for _, tt := range tests {
		got := tf.ShouldReplicate(tt.topic)
		if got != tt.want {
			t.Errorf("ShouldReplicate(%q) = %v, want %v", tt.topic, got, tt.want)
		}
	}
}

func TestTopicFilterBlacklistOnly(t *testing.T) {
	tf, err := NewTopicFilter(nil, []string{".*\\.test$", ".*\\.staging$"})
	if err != nil {
		t.Fatalf("NewTopicFilter: %v", err)
	}

	tests := []struct {
		topic string
		want  bool
	}{
		{"orders", true},
		{"events.production", true},
		{"orders.test", false},
		{"payments.staging", false},
		{"test", true}, // no prefix dot
	}
	for _, tt := range tests {
		got := tf.ShouldReplicate(tt.topic)
		if got != tt.want {
			t.Errorf("ShouldReplicate(%q) = %v, want %v", tt.topic, got, tt.want)
		}
	}
}

func TestTopicFilterWhitelistAndBlacklist(t *testing.T) {
	tf, err := NewTopicFilter(
		[]string{"^orders.*"},
		[]string{".*\\.test$"},
	)
	if err != nil {
		t.Fatalf("NewTopicFilter: %v", err)
	}

	tests := []struct {
		topic string
		want  bool
	}{
		{"orders", true},
		{"orders.prod", true},
		{"orders.test", false}, // blacklist takes precedence
		{"events", false},      // not in whitelist
	}
	for _, tt := range tests {
		got := tf.ShouldReplicate(tt.topic)
		if got != tt.want {
			t.Errorf("ShouldReplicate(%q) = %v, want %v", tt.topic, got, tt.want)
		}
	}
}

func TestTopicFilterEmptyMatchAll(t *testing.T) {
	tf, err := NewTopicFilter(nil, nil)
	if err != nil {
		t.Fatalf("NewTopicFilter: %v", err)
	}

	tests := []struct {
		topic string
		want  bool
	}{
		{"orders", true},
		{"any-topic-at-all", true},
		{"events.production", true},
		// Internal topics are still rejected
		{"__consumer_offsets", false},
		{".internal", false},
	}
	for _, tt := range tests {
		got := tf.ShouldReplicate(tt.topic)
		if got != tt.want {
			t.Errorf("ShouldReplicate(%q) = %v, want %v", tt.topic, got, tt.want)
		}
	}
}

func TestTopicFilterInternalTopicRejection(t *testing.T) {
	// Even with a wildcard whitelist, internal topics should be rejected
	tf, err := NewTopicFilter([]string{".*"}, nil)
	if err != nil {
		t.Fatalf("NewTopicFilter: %v", err)
	}

	internalTopics := []string{
		"__consumer_offsets",
		"__transaction_state",
		".__amazon_msk_canary",
		".internal",
	}
	for _, topic := range internalTopics {
		if tf.ShouldReplicate(topic) {
			t.Errorf("ShouldReplicate(%q) = true, want false (internal topic)", topic)
		}
	}
}

func TestTopicFilterInvalidRegex(t *testing.T) {
	_, err := NewTopicFilter([]string{"[invalid"}, nil)
	if err == nil {
		t.Error("expected error for invalid whitelist regex")
	}

	_, err = NewTopicFilter(nil, []string{"(unclosed"})
	if err == nil {
		t.Error("expected error for invalid blacklist regex")
	}
}

func TestTopicFilterComplexPatterns(t *testing.T) {
	tf, err := NewTopicFilter(
		[]string{`^(orders|payments|events)\.\w+$`},
		[]string{`\.internal$`},
	)
	if err != nil {
		t.Fatalf("NewTopicFilter: %v", err)
	}

	tests := []struct {
		topic string
		want  bool
	}{
		{"orders.v1", true},
		{"payments.prod", true},
		{"events.live", true},
		{"orders.internal", false}, // blacklisted
		{"users.prod", false},      // not in whitelist pattern
	}
	for _, tt := range tests {
		got := tf.ShouldReplicate(tt.topic)
		if got != tt.want {
			t.Errorf("ShouldReplicate(%q) = %v, want %v", tt.topic, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// GroupFilter
// ---------------------------------------------------------------------------

func TestGroupFilterWhitelistOnly(t *testing.T) {
	gf, err := NewGroupFilter([]string{"^app-.*"}, nil)
	if err != nil {
		t.Fatalf("NewGroupFilter: %v", err)
	}

	tests := []struct {
		group string
		want  bool
	}{
		{"app-orders", true},
		{"app-payments", true},
		{"system-monitor", false},
		{"internal-app-group", false},
	}
	for _, tt := range tests {
		got := gf.ShouldReplicate(tt.group)
		if got != tt.want {
			t.Errorf("ShouldReplicate(%q) = %v, want %v", tt.group, got, tt.want)
		}
	}
}

func TestGroupFilterBlacklistOnly(t *testing.T) {
	gf, err := NewGroupFilter(nil, []string{"^__.*", "^gomm2-.*"})
	if err != nil {
		t.Fatalf("NewGroupFilter: %v", err)
	}

	tests := []struct {
		group string
		want  bool
	}{
		{"my-app", true},
		{"__consumer_offsets", false},
		{"gomm2-source-a-b", false},
		{"app-group", true},
	}
	for _, tt := range tests {
		got := gf.ShouldReplicate(tt.group)
		if got != tt.want {
			t.Errorf("ShouldReplicate(%q) = %v, want %v", tt.group, got, tt.want)
		}
	}
}

func TestGroupFilterEmptyMatchAll(t *testing.T) {
	gf, err := NewGroupFilter(nil, nil)
	if err != nil {
		t.Fatalf("NewGroupFilter: %v", err)
	}

	for _, group := range []string{"anything", "at-all", "__internal"} {
		if !gf.ShouldReplicate(group) {
			t.Errorf("ShouldReplicate(%q) = false, want true (no filter)", group)
		}
	}
}

func TestGroupFilterWhitelistAndBlacklist(t *testing.T) {
	gf, err := NewGroupFilter(
		[]string{"^app-.*"},
		[]string{".*-test$"},
	)
	if err != nil {
		t.Fatalf("NewGroupFilter: %v", err)
	}

	tests := []struct {
		group string
		want  bool
	}{
		{"app-orders", true},
		{"app-orders-test", false}, // blacklist takes precedence
		{"system-monitor", false},  // not in whitelist
	}
	for _, tt := range tests {
		got := gf.ShouldReplicate(tt.group)
		if got != tt.want {
			t.Errorf("ShouldReplicate(%q) = %v, want %v", tt.group, got, tt.want)
		}
	}
}

func TestGroupFilterInvalidRegex(t *testing.T) {
	_, err := NewGroupFilter([]string{"[bad"}, nil)
	if err == nil {
		t.Error("expected error for invalid whitelist regex")
	}

	_, err = NewGroupFilter(nil, []string{"(unclosed"})
	if err == nil {
		t.Error("expected error for invalid blacklist regex")
	}
}

// ---------------------------------------------------------------------------
// ConfigPropertyFilter
// ---------------------------------------------------------------------------

func TestConfigPropertyFilterWhitelistOnly(t *testing.T) {
	cf, err := NewConfigPropertyFilter(
		[]string{"^retention\\..*", "^cleanup\\..*"},
		nil,
	)
	if err != nil {
		t.Fatalf("NewConfigPropertyFilter: %v", err)
	}

	tests := []struct {
		prop string
		want bool
	}{
		{"retention.ms", true},
		{"retention.bytes", true},
		{"cleanup.policy", true},
		{"segment.bytes", false},
		{"min.insync.replicas", false},
	}
	for _, tt := range tests {
		got := cf.ShouldReplicate(tt.prop)
		if got != tt.want {
			t.Errorf("ShouldReplicate(%q) = %v, want %v", tt.prop, got, tt.want)
		}
	}
}

func TestConfigPropertyFilterBlacklistOnly(t *testing.T) {
	cf, err := NewConfigPropertyFilter(nil, []string{
		"follower\\.replication\\.throttled\\.replicas",
		"leader\\.replication\\.throttled\\.replicas",
	})
	if err != nil {
		t.Fatalf("NewConfigPropertyFilter: %v", err)
	}

	tests := []struct {
		prop string
		want bool
	}{
		{"retention.ms", true},
		{"cleanup.policy", true},
		{"follower.replication.throttled.replicas", false},
		{"leader.replication.throttled.replicas", false},
	}
	for _, tt := range tests {
		got := cf.ShouldReplicate(tt.prop)
		if got != tt.want {
			t.Errorf("ShouldReplicate(%q) = %v, want %v", tt.prop, got, tt.want)
		}
	}
}

func TestConfigPropertyFilterEmptyMatchAll(t *testing.T) {
	cf, err := NewConfigPropertyFilter(nil, nil)
	if err != nil {
		t.Fatalf("NewConfigPropertyFilter: %v", err)
	}

	for _, prop := range []string{"any.property", "whatever", "retention.ms"} {
		if !cf.ShouldReplicate(prop) {
			t.Errorf("ShouldReplicate(%q) = false, want true (no filter)", prop)
		}
	}
}

func TestConfigPropertyFilterInvalidRegex(t *testing.T) {
	_, err := NewConfigPropertyFilter([]string{"[invalid"}, nil)
	if err == nil {
		t.Error("expected error for invalid whitelist regex")
	}
}
