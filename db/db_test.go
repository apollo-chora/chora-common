// Package db tests — RED phase first.
//
// Per .claude/rules/development-execution.md the test file precedes
// implementation; tests must fail because the implementation does not
// yet exist.
package db

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// -----------------------------------------------------------------------------
// RewriteDSNPort — unit tests
// -----------------------------------------------------------------------------

func TestRewriteDSNPort_FromPgBouncerToProxy(t *testing.T) {
	t.Parallel()
	in := "postgres://app_rw_chora_identity:secret@127.0.0.1:6432/chora_identity?sslmode=disable"
	out, err := RewriteDSNPort(in, 6432, 5432)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if !strings.Contains(out, ":5432/") {
		t.Fatalf("expected port replaced to 5432, got %q", out)
	}
	if strings.Contains(out, ":6432/") {
		t.Fatalf("port 6432 still present in %q", out)
	}
}

func TestRewriteDSNPort_NoPortPresentNoOp(t *testing.T) {
	t.Parallel()
	in := "postgres://user:pw@host/db"
	out, err := RewriteDSNPort(in, 6432, 5432)
	if err != nil {
		t.Fatalf("expected no-op success, got %v", err)
	}
	if out != in {
		t.Fatalf("expected unchanged DSN, got %q", out)
	}
}

func TestRewriteDSNPort_OnlyMatchingPort(t *testing.T) {
	t.Parallel()
	in := "postgres://user:pw@host:5433/db"
	out, err := RewriteDSNPort(in, 6432, 5432)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != in {
		t.Fatalf("expected unchanged DSN, got %q", out)
	}
}

func TestRewriteDSNPort_EmptyDSNRejected(t *testing.T) {
	t.Parallel()
	if _, err := RewriteDSNPort("", 6432, 5432); err == nil {
		t.Fatalf("expected error on empty DSN")
	}
}

// -----------------------------------------------------------------------------
// PoolConfig defaults — unit tests
// -----------------------------------------------------------------------------

func TestDefaultPoolConfig_Resilience(t *testing.T) {
	t.Parallel()
	c := DefaultPoolConfig()

	if c.MaxConns != 20 {
		t.Errorf("MaxConns = %d, want 20", c.MaxConns)
	}
	if c.MinConns != 5 {
		t.Errorf("MinConns = %d, want 5", c.MinConns)
	}
	if c.MaxConnIdleTime != 5*time.Minute {
		t.Errorf("MaxConnIdleTime = %v, want 5m", c.MaxConnIdleTime)
	}
	if c.HealthCheckPeriod != 30*time.Second {
		t.Errorf("HealthCheckPeriod = %v, want 30s", c.HealthCheckPeriod)
	}
	if c.MaxConnLifetime != 1*time.Hour {
		t.Errorf("MaxConnLifetime = %v, want 1h", c.MaxConnLifetime)
	}
}

// -----------------------------------------------------------------------------
// applyRuntimeParams — per-connection GUC startup params (fail-loud tuning)
// -----------------------------------------------------------------------------

func TestApplyRuntimeParams_MergesOntoParsedConfig(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:5432/db?sslmode=disable")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	applyRuntimeParams(cfg, map[string]string{
		"lock_timeout":                        "3s",
		"idle_in_transaction_session_timeout": "60s",
	})
	if got := cfg.ConnConfig.RuntimeParams["lock_timeout"]; got != "3s" {
		t.Errorf("lock_timeout = %q, want 3s", got)
	}
	if got := cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"]; got != "60s" {
		t.Errorf("idle_in_transaction_session_timeout = %q, want 60s", got)
	}
}

func TestApplyRuntimeParams_PreservesExistingParams(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:5432/db?sslmode=disable")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// Mirror the AppName block: an already-set param must survive the merge.
	cfg.ConnConfig.RuntimeParams["application_name"] = "chora-consumption"
	// Add-only: an existing key (here lock_timeout) is NOT overridden.
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "9s"
	applyRuntimeParams(cfg, map[string]string{
		"lock_timeout":                        "3s",
		"idle_in_transaction_session_timeout": "60s",
	})
	if got := cfg.ConnConfig.RuntimeParams["application_name"]; got != "chora-consumption" {
		t.Errorf("application_name clobbered = %q, want chora-consumption", got)
	}
	if got := cfg.ConnConfig.RuntimeParams["lock_timeout"]; got != "9s" {
		t.Errorf("existing lock_timeout overridden = %q, want 9s preserved", got)
	}
	if got := cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"]; got != "60s" {
		t.Errorf("idle_in_transaction_session_timeout = %q, want 60s added", got)
	}
}

func TestApplyRuntimeParams_NilAndEmptyAreNoOps(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:5432/db?sslmode=disable")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	before := len(cfg.ConnConfig.RuntimeParams)
	applyRuntimeParams(cfg, nil)
	applyRuntimeParams(cfg, map[string]string{})
	if after := len(cfg.ConnConfig.RuntimeParams); after != before {
		t.Errorf("no-op changed param count: %d -> %d", before, after)
	}
	// Must not panic on a nil cfg / nil ConnConfig.
	applyRuntimeParams(nil, map[string]string{"lock_timeout": "3s"})
}

// -----------------------------------------------------------------------------
// BootstrapOptions — validation
// -----------------------------------------------------------------------------

func TestBootstrap_RequiresEitherDSNOrSecret(t *testing.T) {
	t.Parallel()
	_, err := Bootstrap(nil, BootstrapOptions{})
	if err == nil {
		t.Fatalf("expected validation error on empty bootstrap options")
	}
	if !errors.Is(err, ErrInvalidBootstrap) {
		t.Errorf("expected ErrInvalidBootstrap, got %T %v", err, err)
	}
}

// -----------------------------------------------------------------------------
// ParseDSNHostPort — internal helper
// -----------------------------------------------------------------------------

func TestParseDSNHostPort(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in       string
		wantHost string
		wantPort int
	}{
		{"postgres://u:p@127.0.0.1:6432/db", "127.0.0.1", 6432},
		{"postgres://u:p@host/db", "host", 0},
		{"postgres://u:p@host:5432/db?sslmode=disable", "host", 5432},
	}
	for _, c := range cases {
		host, port, err := parseDSNHostPort(c.in)
		if err != nil {
			t.Errorf("parse(%q): %v", c.in, err)
			continue
		}
		if host != c.wantHost || port != c.wantPort {
			t.Errorf("parse(%q) = (%q, %d), want (%q, %d)",
				c.in, host, port, c.wantHost, c.wantPort)
		}
	}
}
