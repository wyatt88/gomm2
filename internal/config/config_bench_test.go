package config

import (
	"testing"
)

func BenchmarkParseMinimalYAML(b *testing.B) {
	data := []byte(`
clusters:
  source:
    bootstrap_servers: ["localhost:9092"]
  target:
    bootstrap_servers: ["localhost:9093"]
replications:
  - source: source
    target: target
    enabled: true
`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Parse(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseFullYAML(b *testing.B) {
	data := []byte(`
clusters:
  primary:
    bootstrap_servers:
      - "kafka-1:9092"
      - "kafka-2:9092"
      - "kafka-3:9092"
    sasl:
      mechanism: "SCRAM-SHA-512"
      username: "user"
      password: "pass"
    tls:
      enabled: true
      ca_cert_file: "/path/to/ca.pem"
  backup:
    bootstrap_servers:
      - "backup-1:9092"
      - "backup-2:9092"
    tls:
      enabled: true
      skip_verify: true
replications:
  - source: primary
    target: backup
    enabled: true
    topic_filter:
      whitelist:
        - "^orders.*"
        - "^payments.*"
      blacklist:
        - ".*\\.test$"
    group_filter:
      whitelist: [".*"]
    replication_policy: "default"
    separator: "."
    replication_factor: 3
    sync_topic_configs: true
    emit_heartbeats: true
    emit_checkpoints: true
    emit_offset_syncs: true
    refresh_topics_interval: "30s"
    heartbeat_interval: "1s"
    checkpoint_interval: "60s"
    producer_batch_size: 65536
    producer_linger_ms: 10
    consumer_poll_timeout: "1s"
    max_poll_records: 1000
    compression: "zstd"
    read_committed: true
metrics:
  enabled: true
  address: ":9090"
logging:
  level: "info"
  format: "json"
`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Parse(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValidate(b *testing.B) {
	data := []byte(`
clusters:
  source:
    bootstrap_servers: ["localhost:9092"]
  target:
    bootstrap_servers: ["localhost:9093"]
replications:
  - source: source
    target: target
    enabled: true
    topic_filter:
      whitelist: ["^orders.*", "^events.*"]
      blacklist: [".*\\.test$"]
`)
	cfg, err := Parse(data)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cfg.Validate()
	}
}
