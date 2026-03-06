package config

import (
	"testing"
)

func TestParseMinimalConfig(t *testing.T) {
	yaml := `
clusters:
  primary:
    bootstrap_servers:
      - "localhost:9092"
  backup:
    bootstrap_servers:
      - "localhost:9093"
replications:
  - source: primary
    target: backup
    enabled: true
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(cfg.Clusters) != 2 {
		t.Errorf("expected 2 clusters, got %d", len(cfg.Clusters))
	}
	if len(cfg.Replications) != 1 {
		t.Errorf("expected 1 replication, got %d", len(cfg.Replications))
	}
	r := cfg.Replications[0]
	if r.Separator != "." {
		t.Errorf("expected separator '.', got %q", r.Separator)
	}
	if r.ReplicationFactor != 3 {
		t.Errorf("expected replication factor 3, got %d", r.ReplicationFactor)
	}
}

func TestValidateInvalidCluster(t *testing.T) {
	yaml := `
clusters:
  primary:
    bootstrap_servers: ["localhost:9092"]
replications:
  - source: primary
    target: nonexistent
    enabled: true
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for missing cluster")
	}
}

func TestValidateSameSourceTarget(t *testing.T) {
	yaml := `
clusters:
  primary:
    bootstrap_servers: ["localhost:9092"]
replications:
  - source: primary
    target: primary
    enabled: true
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for same source/target")
	}
}

func TestDurationParsing(t *testing.T) {
	yaml := `
clusters:
  a:
    bootstrap_servers: ["a:9092"]
  b:
    bootstrap_servers: ["b:9092"]
replications:
  - source: a
    target: b
    enabled: true
    refresh_topics_interval: "30s"
    heartbeat_interval: "500ms"
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	r := cfg.Replications[0]
	if r.RefreshTopicsInterval.Seconds() != 30 {
		t.Errorf("expected 30s, got %v", r.RefreshTopicsInterval)
	}
	if r.HeartbeatInterval.Milliseconds() != 500 {
		t.Errorf("expected 500ms, got %v", r.HeartbeatInterval)
	}
}
