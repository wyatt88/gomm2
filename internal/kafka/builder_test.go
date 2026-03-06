package kafka

import (
	"testing"

	"github.com/gomm2/gomm2/internal/config"
)

func TestBuildClientOpts_MinimalConfig(t *testing.T) {
	cc := config.ClusterConfig{
		BootstrapServers: []string{"localhost:9092"},
	}
	opts, err := BuildClientOpts(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(opts) == 0 {
		t.Fatal("expected at least one opt (seed brokers)")
	}
}

func TestBuildClientOpts_WithTLS(t *testing.T) {
	cc := config.ClusterConfig{
		BootstrapServers: []string{"localhost:9092"},
		TLS: &config.TLSConfig{
			Enabled:    true,
			SkipVerify: true,
		},
	}
	opts, err := BuildClientOpts(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should have seed brokers + TLS
	if len(opts) < 2 {
		t.Errorf("expected at least 2 opts, got %d", len(opts))
	}
}

func TestBuildClientOpts_WithSASL(t *testing.T) {
	cc := config.ClusterConfig{
		BootstrapServers: []string{"localhost:9092"},
		SASL: &config.SASLConfig{
			Mechanism: "PLAIN",
			Username:  "user",
			Password:  "pass",
		},
	}
	opts, err := BuildClientOpts(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should have seed brokers + SASL
	if len(opts) < 2 {
		t.Errorf("expected at least 2 opts, got %d", len(opts))
	}
}

func TestBuildClientOpts_WithTLSAndSASL(t *testing.T) {
	cc := config.ClusterConfig{
		BootstrapServers: []string{"broker1:9093", "broker2:9093"},
		TLS: &config.TLSConfig{
			Enabled:    true,
			SkipVerify: true,
		},
		SASL: &config.SASLConfig{
			Mechanism: "SCRAM-SHA-512",
			Username:  "admin",
			Password:  "secret",
		},
	}
	opts, err := BuildClientOpts(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should have seed brokers + TLS + SASL
	if len(opts) < 3 {
		t.Errorf("expected at least 3 opts, got %d", len(opts))
	}
}

func TestBuildClientOpts_InvalidSASL(t *testing.T) {
	cc := config.ClusterConfig{
		BootstrapServers: []string{"localhost:9092"},
		SASL: &config.SASLConfig{
			Mechanism: "INVALID",
		},
	}
	_, err := BuildClientOpts(cc)
	if err == nil {
		t.Fatal("expected error for invalid SASL mechanism")
	}
}

func TestBuildClientOpts_TLSDisabled(t *testing.T) {
	cc := config.ClusterConfig{
		BootstrapServers: []string{"localhost:9092"},
		TLS: &config.TLSConfig{
			Enabled: false,
		},
	}
	opts, err := BuildClientOpts(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only seed brokers, TLS disabled
	if len(opts) != 1 {
		t.Errorf("expected 1 opt (seed brokers only), got %d", len(opts))
	}
}
