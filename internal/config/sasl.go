// Package config — sasl.go provides helpers to build kgo.Opt for SASL authentication.
package config

import (
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// BuildSASLOpt converts a SASLConfig into a kgo.Opt that configures SASL
// authentication on a franz-go client. Returns nil if the config is nil.
//
// Supported mechanisms:
//   - PLAIN
//   - SCRAM-SHA-256
//   - SCRAM-SHA-512
//   - AWS_MSK_IAM
func BuildSASLOpt(sc *SASLConfig) (kgo.Opt, error) {
	if sc == nil {
		return nil, nil
	}

	mechanism := strings.ToUpper(strings.TrimSpace(sc.Mechanism))

	var m sasl.Mechanism
	switch mechanism {
	case "PLAIN":
		m = plain.Auth{
			User: sc.Username,
			Pass: sc.Password,
		}.AsMechanism()

	case "SCRAM-SHA-256":
		m = scram.Auth{
			User: sc.Username,
			Pass: sc.Password,
		}.AsSha256Mechanism()

	case "SCRAM-SHA-512":
		m = scram.Auth{
			User: sc.Username,
			Pass: sc.Password,
		}.AsSha512Mechanism()

	case "AWS_MSK_IAM":
		m = aws.Auth{
			AccessKey:    sc.Username,
			SecretKey:    sc.Password,
			SessionToken: sc.SessionToken,
		}.AsManagedStreamingIAMMechanism()

	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, AWS_MSK_IAM)", sc.Mechanism)
	}

	return kgo.SASL(m), nil
}
