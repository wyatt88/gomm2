// Package kafka provides shared utilities for building franz-go Kafka clients.
package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gomm2/gomm2/internal/config"
)

// BuildClientOpts returns a slice of kgo.Opt configured for the given ClusterConfig.
// It handles bootstrap servers, TLS, and SASL authentication so that every
// component in gomm2 uses one consistent builder instead of duplicating logic.
func BuildClientOpts(cc config.ClusterConfig) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cc.BootstrapServers...),
	}

	// TLS
	if cc.TLS != nil && cc.TLS.Enabled {
		tlsCfg, err := cc.TLS.BuildTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("build TLS config: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	// SASL
	if cc.SASL != nil {
		saslOpt, err := config.BuildSASLOpt(cc.SASL)
		if err != nil {
			return nil, fmt.Errorf("build SASL config: %w", err)
		}
		if saslOpt != nil {
			opts = append(opts, saslOpt)
		}
	}

	return opts, nil
}
