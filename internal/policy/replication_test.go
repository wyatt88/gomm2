package policy

import "testing"

func TestDefaultPolicyFormatRemoteTopic(t *testing.T) {
	p := NewDefaultPolicy(".")
	got := p.FormatRemoteTopic("us-west", "orders")
	want := "us-west.orders"
	if got != want {
		t.Errorf("FormatRemoteTopic = %q, want %q", got, want)
	}
}

func TestDefaultPolicyTopicSource(t *testing.T) {
	p := NewDefaultPolicy(".")
	tests := []struct {
		topic string
		want  string
	}{
		{"us-west.orders", "us-west"},
		{"orders", ""},
		{"a.b.c", "a"},
	}
	for _, tt := range tests {
		got := p.TopicSource(tt.topic)
		if got != tt.want {
			t.Errorf("TopicSource(%q) = %q, want %q", tt.topic, got, tt.want)
		}
	}
}

func TestDefaultPolicyUpstreamTopic(t *testing.T) {
	p := NewDefaultPolicy(".")
	got := p.UpstreamTopic("us-west.orders")
	want := "orders"
	if got != want {
		t.Errorf("UpstreamTopic = %q, want %q", got, want)
	}
}

func TestDefaultPolicyInternalTopics(t *testing.T) {
	p := NewDefaultPolicy(".")
	if !p.IsInternalTopic("mm2-offset-syncs.primary.internal") {
		t.Error("expected mm2-offset-syncs to be internal")
	}
	if !p.IsInternalTopic("primary.checkpoints.internal") {
		t.Error("expected checkpoints to be internal")
	}
	if p.IsInternalTopic("orders") {
		t.Error("orders should not be internal")
	}
}

func TestDefaultPolicyCustomSeparator(t *testing.T) {
	p := NewDefaultPolicy("_")
	got := p.FormatRemoteTopic("dc1", "events")
	want := "dc1_events"
	if got != want {
		t.Errorf("FormatRemoteTopic = %q, want %q", got, want)
	}
	src := p.TopicSource("dc1_events")
	if src != "dc1" {
		t.Errorf("TopicSource = %q, want %q", src, "dc1")
	}
}

func TestIdentityPolicy(t *testing.T) {
	p := NewIdentityPolicy()
	got := p.FormatRemoteTopic("dc1", "orders")
	if got != "orders" {
		t.Errorf("IdentityPolicy FormatRemoteTopic = %q, want %q", got, "orders")
	}
	if src := p.TopicSource("orders"); src != "" {
		t.Errorf("IdentityPolicy TopicSource = %q, want empty", src)
	}
}
