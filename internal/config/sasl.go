// Package config — sasl.go provides helpers to build kgo.Opt for SASL authentication.
package config

import (
	"context"
	"fmt"
	"strings"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
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
//   - AWS_MSK_IAM — supports both static credentials (username/password in config)
//     and the AWS default credential chain (IAM role, IRSA, EC2 instance profile, etc.).
//     When username is empty, the default credential chain is used automatically.
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
		// FIX(nexus): Bug 2 — Support AWS default credential chain for MSK IAM auth.
		// Previously only static AK/SK was supported, which breaks IAM role / IRSA /
		// EC2 instance profile deployments. Now, if username (access key) is empty,
		// we use the AWS SDK Go v2 default credential chain which covers:
		//   - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
		//   - Shared credentials file (~/.aws/credentials)
		//   - IAM roles for EKS service accounts (IRSA)
		//   - EC2 instance metadata (instance profile)
		//   - ECS task role
		//   - SSO credentials
		if sc.Username != "" {
			// Static credentials path — backward compatible
			m = aws.Auth{
				AccessKey:    sc.Username,
				SecretKey:    sc.Password,
				SessionToken: sc.SessionToken,
			}.AsManagedStreamingIAMMechanism()
		} else {
			// Default credential chain path — new behavior
			awsCfg, err := awsconfig.LoadDefaultConfig(context.Background())
			if err != nil {
				return nil, fmt.Errorf("load AWS default credential chain for MSK IAM: %w", err)
			}
			creds, err := awsCfg.Credentials.Retrieve(context.Background())
			if err != nil {
				return nil, fmt.Errorf("retrieve AWS credentials for MSK IAM: %w", err)
			}
			m = aws.Auth{
				AccessKey:    creds.AccessKeyID,
				SecretKey:    creds.SecretAccessKey,
				SessionToken: creds.SessionToken,
			}.AsManagedStreamingIAMMechanism()

			// If the credentials are temporary (e.g., from IRSA/instance profile),
			// we need a refreshing mechanism. Use aws.Auth with a custom credential
			// function to auto-refresh before expiry.
			if creds.CanExpire {
				credProvider := awsCfg.Credentials
				m = aws.Auth{}.AsManagedStreamingIAMMechanism()
				// Override with a function-based approach that fetches fresh creds each time
				m = newRefreshableIAMMechanism(credProvider)
			}
		}

	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, AWS_MSK_IAM)", sc.Mechanism)
	}

	return kgo.SASL(m), nil
}

// newRefreshableIAMMechanism creates an AWS MSK IAM SASL mechanism that
// refreshes credentials from the given provider on each authentication attempt.
// This ensures IRSA/instance profile temporary credentials are always fresh.
func newRefreshableIAMMechanism(provider awssdk.CredentialsProvider) sasl.Mechanism {
	return &refreshableIAM{provider: provider}
}

// refreshableIAM wraps aws.Auth to refresh credentials before each auth attempt.
type refreshableIAM struct {
	provider awssdk.CredentialsProvider
}

func (r *refreshableIAM) Name() string {
	return "AWS_MSK_IAM"
}

func (r *refreshableIAM) Authenticate(ctx context.Context, host string) (sasl.Session, []byte, error) {
	creds, err := r.provider.Retrieve(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("refresh AWS credentials for MSK IAM: %w", err)
	}
	inner := aws.Auth{
		AccessKey:    creds.AccessKeyID,
		SecretKey:    creds.SecretAccessKey,
		SessionToken: creds.SessionToken,
	}.AsManagedStreamingIAMMechanism()
	return inner.Authenticate(ctx, host)
}
