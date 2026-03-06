package policy

import "testing"

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkFormatRemoteTopic(b *testing.B) {
	p := NewDefaultPolicy(".")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.FormatRemoteTopic("us-west", "orders.events.v2")
	}
}

func BenchmarkTopicSource(b *testing.B) {
	p := NewDefaultPolicy(".")
	topic := "us-west.orders.events.v2"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.TopicSource(topic)
	}
}

func BenchmarkUpstreamTopic(b *testing.B) {
	p := NewDefaultPolicy(".")
	topic := "us-west.orders.events.v2"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.UpstreamTopic(topic)
	}
}

func BenchmarkFormatRemoteTopicIdentity(b *testing.B) {
	p := NewIdentityPolicy()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.FormatRemoteTopic("us-west", "orders")
	}
}

func BenchmarkIsInternalTopic(b *testing.B) {
	p := NewDefaultPolicy(".")
	topics := []string{
		"mm2-offset-syncs.primary.internal",
		"primary.checkpoints.internal",
		"orders",
		"heartbeats",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.IsInternalTopic(topics[i%len(topics)])
	}
}

// ---------------------------------------------------------------------------
// Cycle detection tests
// ---------------------------------------------------------------------------

// isCycle determines if producing to targetCluster would create a replication
// cycle. A topic like "A.B.topic" indicates it was replicated from A to B.
// If the target is A, producing this topic back creates a cycle.
func isCycle(policy ReplicationPolicy, targetCluster, topic string) bool {
	source := policy.TopicSource(topic)
	if source == "" {
		return false // not a remote topic
	}
	if source == targetCluster {
		return true // direct cycle: A.topic → target=A
	}
	// Check for multi-level cycles: A.B.topic → upstream is B.topic → source is B
	upstream := policy.UpstreamTopic(topic)
	if upstream == "" {
		return false
	}
	return isCycle(policy, targetCluster, upstream)
}

func TestCycleDetectionDirectCycle(t *testing.T) {
	p := NewDefaultPolicy(".")

	// A.topic was replicated from A; producing back to A is a cycle
	topic := p.FormatRemoteTopic("A", "orders") // "A.orders"
	if !isCycle(p, "A", topic) {
		t.Errorf("expected cycle for %q → target A", topic)
	}
}

func TestCycleDetectionNoCycle(t *testing.T) {
	p := NewDefaultPolicy(".")

	topic := p.FormatRemoteTopic("A", "orders") // "A.orders"
	if isCycle(p, "B", topic) {
		t.Errorf("expected no cycle for %q → target B", topic)
	}
}

func TestCycleDetectionMultiLevel(t *testing.T) {
	p := NewDefaultPolicy(".")

	// Simulate: A replicates "orders" to B → "A.orders"
	// Then B replicates "A.orders" to C → "B.A.orders"
	// Producing "B.A.orders" back to A should detect a cycle
	abTopic := p.FormatRemoteTopic("A", "orders")            // "A.orders"
	bcTopic := p.FormatRemoteTopic("B", abTopic)              // "B.A.orders"

	if !isCycle(p, "A", bcTopic) {
		t.Errorf("expected multi-level cycle for %q → target A", bcTopic)
	}
	if !isCycle(p, "B", bcTopic) {
		t.Errorf("expected multi-level cycle for %q → target B", bcTopic)
	}
	if isCycle(p, "C", bcTopic) {
		t.Errorf("expected no cycle for %q → target C", bcTopic)
	}
}

func TestCycleDetectionThreeLevel(t *testing.T) {
	p := NewDefaultPolicy(".")

	// A → B → C → D
	ab := p.FormatRemoteTopic("A", "orders")   // "A.orders"
	bc := p.FormatRemoteTopic("B", ab)          // "B.A.orders"
	cd := p.FormatRemoteTopic("C", bc)          // "C.B.A.orders"

	// Sending "C.B.A.orders" back to A should detect a 3-level cycle
	if !isCycle(p, "A", cd) {
		t.Errorf("expected 3-level cycle for %q → target A", cd)
	}
	if !isCycle(p, "B", cd) {
		t.Errorf("expected 3-level cycle for %q → target B", cd)
	}
	if !isCycle(p, "C", cd) {
		t.Errorf("expected 3-level cycle for %q → target C", cd)
	}
	if isCycle(p, "D", cd) {
		t.Errorf("expected no cycle for %q → target D", cd)
	}
}

func TestCycleDetectionPlainTopicNoCycle(t *testing.T) {
	p := NewDefaultPolicy(".")
	if isCycle(p, "A", "orders") {
		t.Error("plain topic should not be a cycle")
	}
}

func TestCycleDetectionCustomSeparator(t *testing.T) {
	p := NewDefaultPolicy("_")

	topic := p.FormatRemoteTopic("A", "orders") // "A_orders"
	if !isCycle(p, "A", topic) {
		t.Errorf("expected cycle for %q → target A (custom separator)", topic)
	}
	if isCycle(p, "B", topic) {
		t.Errorf("expected no cycle for %q → target B (custom separator)", topic)
	}
}

// ---------------------------------------------------------------------------
// Additional policy tests
// ---------------------------------------------------------------------------

func TestDefaultPolicyOffsetSyncsTopic(t *testing.T) {
	p := NewDefaultPolicy(".")
	got := p.OffsetSyncsTopic("primary")
	want := "mm2-offset-syncs.primary.internal"
	if got != want {
		t.Errorf("OffsetSyncsTopic = %q, want %q", got, want)
	}
}

func TestDefaultPolicyCheckpointsTopic(t *testing.T) {
	p := NewDefaultPolicy(".")
	got := p.CheckpointsTopic("source")
	want := "source.checkpoints.internal"
	if got != want {
		t.Errorf("CheckpointsTopic = %q, want %q", got, want)
	}
}

func TestDefaultPolicyHeartbeatsTopic(t *testing.T) {
	p := NewDefaultPolicy(".")
	got := p.HeartbeatsTopic()
	if got != "heartbeats" {
		t.Errorf("HeartbeatsTopic = %q, want %q", got, "heartbeats")
	}
}

func TestIdentityPolicyInternalTopics(t *testing.T) {
	p := NewIdentityPolicy()
	if !p.IsInternalTopic("mm2-offset-syncs.us-west.internal") {
		t.Error("expected mm2-offset-syncs to be internal for identity policy")
	}
	if !p.IsInternalTopic("us-west.checkpoints.internal") {
		t.Error("expected checkpoints to be internal for identity policy")
	}
	if p.IsInternalTopic("orders") {
		t.Error("orders should not be internal for identity policy")
	}
}

func TestIdentityPolicyUpstreamTopic(t *testing.T) {
	p := NewIdentityPolicy()
	got := p.UpstreamTopic("orders")
	if got != "orders" {
		t.Errorf("IdentityPolicy.UpstreamTopic = %q, want %q", got, "orders")
	}
}

func TestNewPolicyValidation(t *testing.T) {
	_, err := NewPolicy("default", ".")
	if err != nil {
		t.Errorf("default policy: %v", err)
	}

	_, err = NewPolicy("identity", "")
	if err != nil {
		t.Errorf("identity policy: %v", err)
	}

	_, err = NewPolicy("", ".")
	if err != nil {
		t.Errorf("empty name (should default): %v", err)
	}

	_, err = NewPolicy("unknown-policy", ".")
	if err == nil {
		t.Error("expected error for unknown policy name")
	}
}

func TestDefaultPolicyEmptySeparator(t *testing.T) {
	// Empty separator defaults to "."
	p := NewDefaultPolicy("")
	got := p.FormatRemoteTopic("dc1", "orders")
	if got != "dc1.orders" {
		t.Errorf("FormatRemoteTopic with empty separator = %q, want %q", got, "dc1.orders")
	}
}
