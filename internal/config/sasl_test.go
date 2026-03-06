package config

import (
	"testing"
)

func TestBuildSASLOpt_Nil(t *testing.T) {
	opt, err := BuildSASLOpt(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opt != nil {
		t.Error("expected nil opt for nil config")
	}
}

func TestBuildSASLOpt_Plain(t *testing.T) {
	sc := &SASLConfig{
		Mechanism: "PLAIN",
		Username:  "user",
		Password:  "pass",
	}
	opt, err := BuildSASLOpt(sc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opt == nil {
		t.Fatal("expected non-nil opt for PLAIN")
	}
}

func TestBuildSASLOpt_ScramSHA256(t *testing.T) {
	sc := &SASLConfig{
		Mechanism: "SCRAM-SHA-256",
		Username:  "user",
		Password:  "pass",
	}
	opt, err := BuildSASLOpt(sc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opt == nil {
		t.Fatal("expected non-nil opt for SCRAM-SHA-256")
	}
}

func TestBuildSASLOpt_ScramSHA512(t *testing.T) {
	sc := &SASLConfig{
		Mechanism: "SCRAM-SHA-512",
		Username:  "user",
		Password:  "pass",
	}
	opt, err := BuildSASLOpt(sc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opt == nil {
		t.Fatal("expected non-nil opt for SCRAM-SHA-512")
	}
}

func TestBuildSASLOpt_AWSMSKIAM(t *testing.T) {
	sc := &SASLConfig{
		Mechanism:    "AWS_MSK_IAM",
		Username:     "AKID123456",
		Password:     "secret",
		SessionToken: "token",
	}
	opt, err := BuildSASLOpt(sc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opt == nil {
		t.Fatal("expected non-nil opt for AWS_MSK_IAM")
	}
}

func TestBuildSASLOpt_Unsupported(t *testing.T) {
	sc := &SASLConfig{
		Mechanism: "GSSAPI",
		Username:  "user",
		Password:  "pass",
	}
	_, err := BuildSASLOpt(sc)
	if err == nil {
		t.Fatal("expected error for unsupported mechanism")
	}
}

func TestBuildSASLOpt_CaseInsensitive(t *testing.T) {
	sc := &SASLConfig{
		Mechanism: " plain ",
		Username:  "user",
		Password:  "pass",
	}
	opt, err := BuildSASLOpt(sc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opt == nil {
		t.Fatal("expected non-nil opt for lowercase 'plain'")
	}
}

func TestSASLConfigInYAML(t *testing.T) {
	yaml := `
clusters:
  primary:
    bootstrap_servers:
      - "broker1:9092"
    sasl:
      mechanism: SCRAM-SHA-256
      username: admin
      password: secret123
    tls:
      enabled: true
  backup:
    bootstrap_servers:
      - "broker2:9092"
    sasl:
      mechanism: AWS_MSK_IAM
      username: AKID123
      password: secret
      session_token: token123
replications:
  - source: primary
    target: backup
    enabled: true
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	primary := cfg.Clusters["primary"]
	if primary.SASL == nil {
		t.Fatal("expected SASL config for primary")
	}
	if primary.SASL.Mechanism != "SCRAM-SHA-256" {
		t.Errorf("mechanism = %q, want SCRAM-SHA-256", primary.SASL.Mechanism)
	}
	if primary.SASL.Username != "admin" {
		t.Errorf("username = %q, want admin", primary.SASL.Username)
	}

	backup := cfg.Clusters["backup"]
	if backup.SASL == nil {
		t.Fatal("expected SASL config for backup")
	}
	if backup.SASL.Mechanism != "AWS_MSK_IAM" {
		t.Errorf("mechanism = %q, want AWS_MSK_IAM", backup.SASL.Mechanism)
	}
	if backup.SASL.SessionToken != "token123" {
		t.Errorf("session_token = %q, want token123", backup.SASL.SessionToken)
	}
}
