// Package config handles YAML-based configuration for gomm2.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level gomm2 configuration.
type Config struct {
	Clusters     map[string]ClusterConfig `yaml:"clusters"`
	Replications []ReplicationConfig      `yaml:"replications"`
	Metrics      MetricsConfig            `yaml:"metrics"`
	Logging      LoggingConfig            `yaml:"logging"`
}

// ClusterConfig defines connection parameters for a Kafka cluster.
type ClusterConfig struct {
	BootstrapServers []string    `yaml:"bootstrap_servers"`
	SASL             *SASLConfig `yaml:"sasl,omitempty"`
	TLS              *TLSConfig  `yaml:"tls,omitempty"`
}

// SASLConfig holds SASL authentication settings.
type SASLConfig struct {
	Mechanism    string `yaml:"mechanism"`               // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, AWS_MSK_IAM
	Username     string `yaml:"username"`                // for PLAIN/SCRAM: username; for AWS_MSK_IAM: access key
	Password     string `yaml:"password"`                // for PLAIN/SCRAM: password; for AWS_MSK_IAM: secret key
	SessionToken string `yaml:"session_token,omitempty"` // optional AWS session token for AWS_MSK_IAM
}

// TLSConfig holds TLS settings.
type TLSConfig struct {
	Enabled    bool   `yaml:"enabled"`
	CACertFile string `yaml:"ca_cert_file,omitempty"`
	CertFile   string `yaml:"cert_file,omitempty"`
	KeyFile    string `yaml:"key_file,omitempty"`
	SkipVerify bool   `yaml:"skip_verify,omitempty"`
}

// BuildTLSConfig creates a *tls.Config from TLSConfig settings.
func (tc *TLSConfig) BuildTLSConfig() (*tls.Config, error) {
	if tc == nil || !tc.Enabled {
		return nil, nil
	}
	tlsCfg := &tls.Config{
		InsecureSkipVerify: tc.SkipVerify,
	}
	if tc.CACertFile != "" {
		caCert, err := os.ReadFile(tc.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("read CA cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA cert from %s", tc.CACertFile)
		}
		tlsCfg.RootCAs = pool
	}
	if tc.CertFile != "" && tc.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tc.CertFile, tc.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

// ReplicationConfig defines a source→target replication flow.
type ReplicationConfig struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
	Enabled bool  `yaml:"enabled"`

	TopicFilter    TopicFilterConfig    `yaml:"topic_filter"`
	GroupFilter    GroupFilterConfig     `yaml:"group_filter"`
	ConfigFilter   ConfigFilterConfig   `yaml:"config_filter"`

	// Replication tuning
	ReplicationPolicy   string        `yaml:"replication_policy"`   // "default" or "identity"
	Separator           string        `yaml:"separator"`            // default "."
	ReplicationFactor   int16         `yaml:"replication_factor"`   // target topic replication factor
	SyncTopicConfigs    *bool          `yaml:"sync_topic_configs"`
	SyncTopicACLs       bool          `yaml:"sync_topic_acls"`
	EmitHeartbeats      *bool         `yaml:"emit_heartbeats"`
	EmitCheckpoints     *bool         `yaml:"emit_checkpoints"`
	EmitOffsetSyncs     *bool         `yaml:"emit_offset_syncs"`

	// Intervals
	RefreshTopicsInterval     Duration `yaml:"refresh_topics_interval"`
	RefreshGroupsInterval     Duration `yaml:"refresh_groups_interval"`
	SyncTopicConfigsInterval  Duration `yaml:"sync_topic_configs_interval"`
	SyncTopicACLsInterval     Duration `yaml:"sync_topic_acls_interval"`
	HeartbeatInterval         Duration `yaml:"heartbeat_interval"`
	CheckpointInterval        Duration `yaml:"checkpoint_interval"`

	// Performance tuning
	ProducerBatchSize    int           `yaml:"producer_batch_size"`
	ProducerLingerMs     int           `yaml:"producer_linger_ms"`
	ConsumerPollTimeout  Duration      `yaml:"consumer_poll_timeout"`
	MaxPollRecords       int           `yaml:"max_poll_records"`
	Compression          string        `yaml:"compression"` // none, gzip, snappy, lz4, zstd
	ReadCommitted        bool          `yaml:"read_committed"`

	// Reliability & exactly-once semantics
	ExactlyOnce          bool     `yaml:"exactly_once"`
	OffsetCommitInterval Duration `yaml:"offset_commit_interval"`
	OffsetCommitBatch    int      `yaml:"offset_commit_batch"`
	MaxRetries           int      `yaml:"max_retries"`
	RetryBackoffBase     Duration `yaml:"retry_backoff_base"`
	RetryBackoffMax      Duration `yaml:"retry_backoff_max"`
	DLQEnabled           bool     `yaml:"dlq_enabled"`
	ShutdownTimeout      Duration `yaml:"shutdown_timeout"`
}

// TopicFilterConfig defines topic filtering rules.
type TopicFilterConfig struct {
	Whitelist []string `yaml:"whitelist,omitempty"` // regex patterns
	Blacklist []string `yaml:"blacklist,omitempty"` // regex patterns
}

// GroupFilterConfig defines consumer group filtering rules.
type GroupFilterConfig struct {
	Whitelist []string `yaml:"whitelist,omitempty"`
	Blacklist []string `yaml:"blacklist,omitempty"`
}

// ConfigFilterConfig defines which topic config properties to replicate.
type ConfigFilterConfig struct {
	Whitelist []string `yaml:"whitelist,omitempty"`
	Blacklist []string `yaml:"blacklist,omitempty"`
}

// MetricsConfig defines Prometheus metrics endpoint settings.
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Address string `yaml:"address"` // e.g. ":9090"
}

// LoggingConfig defines logging settings.
type LoggingConfig struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // json, console
}

// Duration wraps time.Duration for YAML parsing.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = dur
	return nil
}

func (d Duration) MarshalYAML() (interface{}, error) {
	return d.Duration.String(), nil
}

// Load reads and parses a YAML config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %s: %w", path, err)
	}
	return Parse(data)
}

// Parse parses YAML configuration bytes.
func Parse(data []byte) (*Config, error) {
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	setDefaults(cfg)
	return cfg, nil
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	if len(c.Clusters) == 0 {
		return fmt.Errorf("no clusters defined")
	}
	if len(c.Replications) == 0 {
		return fmt.Errorf("no replications defined")
	}
	for i, r := range c.Replications {
		if !r.Enabled {
			continue
		}
		if _, ok := c.Clusters[r.Source]; !ok {
			return fmt.Errorf("replication[%d]: source cluster %q not defined", i, r.Source)
		}
		if _, ok := c.Clusters[r.Target]; !ok {
			return fmt.Errorf("replication[%d]: target cluster %q not defined", i, r.Target)
		}
		if r.Source == r.Target {
			return fmt.Errorf("replication[%d]: source and target must differ", i)
		}
		// Validate regex patterns
		for _, p := range r.TopicFilter.Whitelist {
			if _, err := regexp.Compile(p); err != nil {
				return fmt.Errorf("replication[%d]: invalid topic whitelist regex %q: %w", i, p, err)
			}
		}
		for _, p := range r.TopicFilter.Blacklist {
			if _, err := regexp.Compile(p); err != nil {
				return fmt.Errorf("replication[%d]: invalid topic blacklist regex %q: %w", i, p, err)
			}
		}
	}
	return nil
}

// BoolDefault returns the value of a *bool, defaulting to the given value if nil.
func BoolDefault(b *bool, def bool) bool {
	if b == nil {
		return def
	}
	return *b
}

func setDefaults(cfg *Config) {
	for i := range cfg.Replications {
		r := &cfg.Replications[i]
		if r.Separator == "" {
			r.Separator = "."
		}
		if r.ReplicationPolicy == "" {
			r.ReplicationPolicy = "default"
		}
		if r.ReplicationFactor == 0 {
			r.ReplicationFactor = 3
		}
		if r.RefreshTopicsInterval.Duration == 0 {
			r.RefreshTopicsInterval.Duration = 10 * time.Second
		}
		if r.RefreshGroupsInterval.Duration == 0 {
			r.RefreshGroupsInterval.Duration = 10 * time.Second
		}
		if r.SyncTopicConfigsInterval.Duration == 0 {
			r.SyncTopicConfigsInterval.Duration = 10 * time.Second
		}
		if r.SyncTopicACLsInterval.Duration == 0 {
			r.SyncTopicACLsInterval.Duration = 10 * time.Second
		}
		if r.HeartbeatInterval.Duration == 0 {
			r.HeartbeatInterval.Duration = 1 * time.Second
		}
		if r.CheckpointInterval.Duration == 0 {
			r.CheckpointInterval.Duration = 60 * time.Second
		}
		if r.ConsumerPollTimeout.Duration == 0 {
			r.ConsumerPollTimeout.Duration = 1 * time.Second
		}
		if r.ProducerBatchSize == 0 {
			r.ProducerBatchSize = 16384
		}
		if r.ProducerLingerMs == 0 {
			r.ProducerLingerMs = 5
		}
		if r.MaxPollRecords == 0 {
			r.MaxPollRecords = 500
		}
		if r.Compression == "" {
			r.Compression = "lz4"
		}
		// Bool pointer defaults — nil means "not set", default to true
		if r.EmitHeartbeats == nil {
			t := true
			r.EmitHeartbeats = &t
		}
		if r.EmitCheckpoints == nil {
			t := true
			r.EmitCheckpoints = &t
		}
		if r.EmitOffsetSyncs == nil {
			t := true
			r.EmitOffsetSyncs = &t
		}
		if r.SyncTopicConfigs == nil {
			t := true
			r.SyncTopicConfigs = &t
		}

		// Reliability defaults
		if r.OffsetCommitInterval.Duration == 0 {
			r.OffsetCommitInterval.Duration = 5 * time.Second
		}
		if r.OffsetCommitBatch == 0 {
			r.OffsetCommitBatch = 1000
		}
		if r.MaxRetries == 0 {
			r.MaxRetries = 10
		}
		if r.RetryBackoffBase.Duration == 0 {
			r.RetryBackoffBase.Duration = 100 * time.Millisecond
		}
		if r.RetryBackoffMax.Duration == 0 {
			r.RetryBackoffMax.Duration = 30 * time.Second
		}
		if r.ShutdownTimeout.Duration == 0 {
			r.ShutdownTimeout.Duration = 30 * time.Second
		}
	}
	if cfg.Metrics.Address == "" {
		cfg.Metrics.Address = ":9090"
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}
	if cfg.Logging.Format == "" {
		cfg.Logging.Format = "json"
	}
}
