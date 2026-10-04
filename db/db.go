// Package db is the shared pgxpool bootstrap helper for every Chora Go
// service that talks to Cloud SQL Enterprise Plus.
//
// Per CLAUDE.md §6 (no inline config) + .claude/skills/secrets-and-env:
// services NEVER hard-code DSNs. They source them from one of:
//
//  1. CHORA_DB_DSN_SECRET_ID  — name of a Secret Manager secret whose
//     latest version contains the connection string.
//  2. CHORA_DB_DSN            — direct DSN, dev-only fallback.
//
// This package centralises:
//
//   - DSN sourcing (Secret Manager → env)
//   - DSN port rewrite (PgBouncer 6432 → Cloud SQL Auth Proxy 5432) for
//     services that bypass PgBouncer until the sidecar lands
//   - pgxpool resilience defaults (MaxConns=20, MinConns=5,
//     MaxConnIdleTime=5m, HealthCheckPeriod=30s, MaxConnLifetime=1h)
//   - context-aware Bootstrap that builds a *pgxpool.Pool ready for
//     production traffic
//
// Resilience-priority directive (`feedback_resilience_priority`): the
// pool is pre-warmed (Ping at boot) and the health-check loop is enabled
// so dropped Cloud SQL connections recover transparently — a replacement
// pod sees the pool re-establish without external supervision.
package db

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/apollo-chora/chora-common/secrets"
)

// PoolConfig captures the pgxpool resilience tuning.
type PoolConfig struct {
	MaxConns          int32         // hard ceiling; default 20
	MinConns          int32         // pre-warm floor; default 5
	MaxConnIdleTime   time.Duration // recycle idle conns; default 5m
	MaxConnLifetime   time.Duration // hard cycle; default 1h
	HealthCheckPeriod time.Duration // dead-connection sweeper; default 30s
}

// DefaultPoolConfig returns the canonical resilience tuning per
// `feedback_resilience_priority` ("dead-pod handling: pgxpool reconnects
// on broken connections; pre-warm + health checks ensure replacement
// pods come up quickly").
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxConns:          20,
		MinConns:          5,
		MaxConnIdleTime:   5 * time.Minute,
		MaxConnLifetime:   1 * time.Hour,
		HealthCheckPeriod: 30 * time.Second,
	}
}

// BootstrapOptions controls Bootstrap.
type BootstrapOptions struct {
	// DSN is the direct connection string. Takes precedence over SecretID
	// when both are set (dev convenience).
	DSN string

	// SecretID is the Secret Manager secret name (e.g.
	// `chora-dev-cloudsql-chora_identity-app_rw-dsn`). Resolved via
	// SecretFetcher if DSN is empty.
	SecretID string

	// SecretFetcher resolves SecretID → DSN. Pass a *secrets.Client in
	// production. Tests pass a stub.
	SecretFetcher SecretFetcher

	// RewriteFromPort, RewriteToPort — when non-zero, rewrite the DSN's
	// port from the source to the target. Used until PgBouncer lands:
	// stored DSNs point at 6432 (PgBouncer) but services bypass to 5432
	// (Cloud SQL Auth Proxy).
	RewriteFromPort int
	RewriteToPort   int

	// PoolConfig overrides the resilience tuning. Zero-value uses
	// DefaultPoolConfig().
	PoolConfig PoolConfig

	// AppName tags the connection for Cloud SQL Insights — e.g.
	// "chora-identity@v0.1.0". Helpful for audit + slow-query analysis.
	AppName string

	// RuntimeParams sets per-connection Postgres GUCs, sent in the startup
	// message for every pooled connection (merged into
	// cfg.ConnConfig.RuntimeParams; nil/empty is a no-op). Opt-in per
	// service — the default (no params) preserves existing behaviour.
	//
	// Used to make DB operations fail-loud instead of hanging forever:
	// e.g. `lock_timeout` bounds a row-lock wait (a blocked UPDATE errors
	// out rather than leaking the request goroutine) and
	// `idle_in_transaction_session_timeout` reaps a leaked open transaction
	// so its locks release. Keys already present (e.g. from the DSN) win —
	// this only adds keys the caller hasn't otherwise set.
	RuntimeParams map[string]string
}

// SecretFetcher is the resolver for Secret Manager secrets. Production
// uses libs/chora-go-common/secrets.Client; tests inject a stub map.
type SecretFetcher interface {
	GetSecret(ctx context.Context, name string) (string, error)
}

// Errors surfaced by Bootstrap.
var (
	// ErrInvalidBootstrap means neither DSN nor SecretID was provided.
	ErrInvalidBootstrap = errors.New("db: BootstrapOptions requires either DSN or SecretID")
)

// Bootstrap returns a fully-warmed *pgxpool.Pool. Caller is responsible
// for calling pool.Close() during graceful shutdown.
//
// On error, returns nil + a wrapped error. Resilience: if Ping fails at
// boot, returns the error so the service can fail-fast (Cloud Run will
// restart and Workload Identity propagation may have been the cause).
func Bootstrap(ctx context.Context, opts BootstrapOptions) (*pgxpool.Pool, error) {
	dsn, err := resolveDSN(ctx, opts)
	if err != nil {
		return nil, err
	}

	if opts.RewriteFromPort != 0 && opts.RewriteToPort != 0 {
		dsn, err = RewriteDSNPort(dsn, opts.RewriteFromPort, opts.RewriteToPort)
		if err != nil {
			return nil, fmt.Errorf("db.Bootstrap: rewrite port: %w", err)
		}
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("db.Bootstrap: parse: %w", err)
	}

	tuning := opts.PoolConfig
	if tuning.MaxConns == 0 {
		tuning = DefaultPoolConfig()
	}
	cfg.MaxConns = tuning.MaxConns
	cfg.MinConns = tuning.MinConns
	cfg.MaxConnIdleTime = tuning.MaxConnIdleTime
	cfg.MaxConnLifetime = tuning.MaxConnLifetime
	cfg.HealthCheckPeriod = tuning.HealthCheckPeriod

	if opts.AppName != "" {
		if cfg.ConnConfig.RuntimeParams == nil {
			cfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		cfg.ConnConfig.RuntimeParams["application_name"] = opts.AppName
	}

	applyRuntimeParams(cfg, opts.RuntimeParams)

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db.Bootstrap: new pool: %w", err)
	}

	// Pre-warm with retry-backoff. Per ATOM-1 §"Cold-start note" +
	// `feedback_d6_resilience_first_class`: a fresh node's Cloud SQL
	// Auth Proxy sidecar may not be reachable for the first ~500ms-2s
	// post-schedule even though Secret Manager has already resolved.
	// Mirrors the JWKS + Secret Manager backoff pattern at
	// libs/chora-go-common/secrets/backoff.go (commit 4c50a1d5).
	if err := pingWithBackoff(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db.Bootstrap: ping after retry: %w", err)
	}
	return pool, nil
}

// applyRuntimeParams merges caller-supplied per-connection GUC startup
// params into cfg. It is add-only: a key already present (from the DSN or
// the AppName block) is left untouched, so this never clobbers an explicit
// setting. nil/empty params — or a nil cfg — are no-ops.
func applyRuntimeParams(cfg *pgxpool.Config, params map[string]string) {
	if cfg == nil || cfg.ConnConfig == nil || len(params) == 0 {
		return
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	for k, v := range params {
		if _, exists := cfg.ConnConfig.RuntimeParams[k]; !exists {
			cfg.ConnConfig.RuntimeParams[k] = v
		}
	}
}

func resolveDSN(ctx context.Context, opts BootstrapOptions) (string, error) {
	if opts.DSN != "" {
		return opts.DSN, nil
	}
	if opts.SecretID == "" {
		return "", ErrInvalidBootstrap
	}
	if opts.SecretFetcher == nil {
		return "", fmt.Errorf("db.Bootstrap: SecretFetcher required when SecretID is set")
	}
	// Use the shared retry-backoff helper (mirrors JWKS pattern at
	// libs/chora-go-common/auth/identityplatform/identityplatform.go).
	// Per E2E-INFRA-COLD-START §3b: Secret Manager occasionally returns
	// codes.Unavailable / codes.DeadlineExceeded during cold start on a
	// fresh node while Workload Identity Federation tokens propagate.
	// The retry curve (6 attempts, ~30s budget) clears these transients
	// before failing the pod boot.
	dsn, err := secrets.FetchSecretWithBackoff(ctx, opts.SecretFetcher, opts.SecretID)
	if err != nil {
		return "", fmt.Errorf("db.Bootstrap: fetch secret %q: %w", opts.SecretID, err)
	}
	if dsn == "" {
		return "", fmt.Errorf("db.Bootstrap: secret %q resolved to empty value", opts.SecretID)
	}
	return dsn, nil
}

// RewriteDSNPort returns a copy of dsn with the host's port replaced
// from `from` to `to`. If the DSN host has no port, or the port differs
// from `from`, the DSN is returned unchanged. Empty input is rejected.
//
// Used by services bypassing PgBouncer (port 6432 in Secret Manager
// DSNs) and connecting directly to the Cloud SQL Auth Proxy on 5432.
func RewriteDSNPort(dsn string, from, to int) (string, error) {
	if dsn == "" {
		return "", errors.New("db: RewriteDSNPort: empty DSN")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("db: RewriteDSNPort: parse: %w", err)
	}
	port := u.Port()
	if port == "" {
		return dsn, nil
	}
	pNum, err := strconv.Atoi(port)
	if err != nil {
		return dsn, nil
	}
	if pNum != from {
		return dsn, nil
	}
	host := u.Hostname()
	u.Host = fmt.Sprintf("%s:%d", host, to)
	return u.String(), nil
}

// parseDSNHostPort returns the DSN host + port (port=0 when unset).
// Internal helper used by tests.
func parseDSNHostPort(dsn string) (string, int, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", 0, err
	}
	port := 0
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil {
			return u.Hostname(), 0, err
		}
		port = n
	}
	return u.Hostname(), port, nil
}
