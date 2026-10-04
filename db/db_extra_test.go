// Package db — supplementary unit tests.
//
// These drive coverage of Bootstrap's pool-construction path, resolveDSN
// secret-resolution branches, and the DSN edge cases the RED-phase
// battery did not reach. They stay hermetic: the pgxpool.Pool is built
// from a parsed config without a live Postgres, and ping failures are
// provoked against a loopback port that nothing listens on, so no test
// depends on a running database.
package db

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// strayFetcher is a programmable SecretFetcher for resolveDSN branches.
type strayFetcher struct {
	value string
	err   error
	calls int
}

func (f *strayFetcher) GetSecret(_ context.Context, _ string) (string, error) {
	f.calls++
	return f.value, f.err
}

// mustParsePoolConfig parses a structurally valid pgxpool config without
// establishing a connection.
func mustParsePoolConfig(t *testing.T, dsn string) *pgxpool.Config {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse config %q: %v", dsn, err)
	}
	return cfg
}

// ---------------------------------------------------------------------------
// resolveDSN — secret-source branches.
// ---------------------------------------------------------------------------

func TestResolveDSN_DSNTakesPrecedence(t *testing.T) {
	t.Parallel()
	dsn, err := resolveDSN(context.Background(), BootstrapOptions{
		DSN:      "postgres://u:p@h:1/db",
		SecretID: "ignored",
	})
	if err != nil {
		t.Fatalf("resolveDSN: %v", err)
	}
	if dsn != "postgres://u:p@h:1/db" {
		t.Errorf("dsn = %q, want DSN passthrough", dsn)
	}
}

func TestResolveDSN_SecretIDWithoutFetcherFails(t *testing.T) {
	t.Parallel()
	_, err := resolveDSN(context.Background(), BootstrapOptions{SecretID: "chora-x"})
	if err == nil {
		t.Fatal("expected error when SecretID is set but no fetcher is wired")
	}
	if !strings.Contains(err.Error(), "SecretFetcher required") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestResolveDSN_SecretFetchSuccess(t *testing.T) {
	t.Parallel()
	f := &strayFetcher{value: "postgres://s:3@h:1/db"}
	dsn, err := resolveDSN(context.Background(), BootstrapOptions{
		SecretID:      "chora-x",
		SecretFetcher: f,
	})
	if err != nil {
		t.Fatalf("resolveDSN: %v", err)
	}
	if dsn != "postgres://s:3@h:1/db" {
		t.Errorf("dsn = %q, want fetched value", dsn)
	}
	if f.calls != 1 {
		t.Errorf("fetcher calls = %d, want 1", f.calls)
	}
}

func TestResolveDSN_SecretFetchErrorWrapped(t *testing.T) {
	t.Parallel()
	// Plain non-gRPC error → backoff treats it as retriable; the
	// caller-supplied deadline aborts the retry inside the first sleep.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	f := &strayFetcher{err: errors.New("secret manager down")}
	_, err := resolveDSN(ctx, BootstrapOptions{SecretID: "chora-x", SecretFetcher: f})
	if err == nil {
		t.Fatal("expected error when fetch fails")
	}
	if !strings.Contains(err.Error(), `fetch secret "chora-x"`) {
		t.Errorf("expected wrapped fetch error, got: %v", err)
	}
}

func TestResolveDSN_SecretFetchEmptyValueRejected(t *testing.T) {
	t.Parallel()
	f := &strayFetcher{} // returns "" with nil error
	_, err := resolveDSN(context.Background(), BootstrapOptions{
		SecretID:      "chora-x",
		SecretFetcher: f,
	})
	if err == nil {
		t.Fatal("expected error when secret resolves to empty value")
	}
	if !strings.Contains(err.Error(), "resolved to empty value") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Bootstrap — pool construction + fail-closed ping path (no real Postgres).
// ---------------------------------------------------------------------------

// TestBootstrap_ValidDSN_FailsClosedWhenPingUnreachable — a structurally
// valid DSN parses, the pool is created with the resilience tuning, and
// the pre-warm Ping provokes failure against an unlistened loopback port.
// Resolves fail-loud (nil pool + wrapped error) per
// `feedback_no_stubs_real_wiring`; successful Pings require a live
// database and are exercised by integration tests.
func TestBootstrap_ValidDSN_FailsClosedWhenPingUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	pool, err := Bootstrap(ctx, BootstrapOptions{
		DSN:             "postgres://u:p@127.0.0.1:59999/db?sslmode=disable",
		AppName:         "chora-test@v0.0.0",
		RuntimeParams:   map[string]string{"lock_timeout": "3s"},
		RewriteFromPort: 0, // no rewrite — exercises the plain path
	})
	if err == nil {
		if pool != nil {
			pool.Close()
		}
		t.Fatal("expected failure: nothing listens on loopback :59999")
	}
	if pool != nil {
		t.Errorf("expected nil pool on failure, got non-nil")
	}
	if !strings.Contains(err.Error(), "db.Bootstrap: ping after retry") {
		t.Errorf("expected ping-after-retry wrap, got: %v", err)
	}
}

func TestBootstrap_InvalidDSNParseFailsClosed(t *testing.T) {
	t.Parallel()
	pool, err := Bootstrap(context.Background(), BootstrapOptions{DSN: "postgres://u:p@127.0.0.1:notaport/db"})
	if err == nil {
		if pool != nil {
			pool.Close()
		}
		t.Fatal("expected parse failure for invalid port")
	}
	if pool != nil {
		t.Errorf("expected nil pool on parse failure")
	}
	if !strings.Contains(err.Error(), "db.Bootstrap: parse") {
		t.Errorf("expected parse wrap, got: %v", err)
	}
}

func TestBootstrap_RewritePortFailureFailsClosed(t *testing.T) {
	t.Parallel()
	pool, err := Bootstrap(context.Background(), BootstrapOptions{
		DSN:             "://bad",
		RewriteFromPort: 6432,
		RewriteToPort:   5432,
	})
	if err == nil {
		if pool != nil {
			pool.Close()
		}
		t.Fatal("expected rewrite failure for unparsable DSN")
	}
	if !strings.Contains(err.Error(), "db.Bootstrap: rewrite port") {
		t.Errorf("expected rewrite-port wrap, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// RewriteDSNPort + parseDSNHostPort — remaining edge branches.
// ---------------------------------------------------------------------------

// TestRewriteDSNPort_NonNumericPortRejectedAtParse — net/url rejects a
// non-numeric port at parse time, so RewriteDSNPort surfaces the parse
// error: the strconv.Atoi guard inside the function is unreachable for
// URL inputs (defensive code that `go vet` would normally flag as dead).
func TestRewriteDSNPort_NonNumericPortRejectedAtParse(t *testing.T) {
	t.Parallel()
	_, err := RewriteDSNPort("postgres://u:p@host:abc/db", 6432, 5432)
	if err == nil {
		t.Fatal("expected error: net/url rejects non-numeric ports at parse")
	}
	if !strings.Contains(err.Error(), "db: RewriteDSNPort: parse") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRewriteDSNPort_UnparsableDSNFails(t *testing.T) {
	t.Parallel()
	_, err := RewriteDSNPort("://bad", 6432, 5432)
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "db: RewriteDSNPort: parse") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseDSNHostPort_InvalidURL(t *testing.T) {
	t.Parallel()
	if _, _, err := parseDSNHostPort("://bad"); err == nil {
		t.Fatal("expected error on unparsable DSN")
	}
}

func TestParseDSNHostPort_NonNumericPortReturnsError(t *testing.T) {
	t.Parallel()
	if _, _, err := parseDSNHostPort("postgres://u:p@host:abc/db"); err == nil {
		t.Fatal("expected error on non-numeric port")
	}
}

// ---------------------------------------------------------------------------
// applyRuntimeParams — nil-RuntimeParams init branch.
// ---------------------------------------------------------------------------

func TestApplyRuntimeParams_InitialisesNilRuntimeParams(t *testing.T) {
	t.Parallel()
	cfg := mustParsePoolConfig(t, "postgres://u:p@127.0.0.1:5432/db?sslmode=disable")
	cfg.ConnConfig.RuntimeParams = nil
	applyRuntimeParams(cfg, map[string]string{"lock_timeout": "3s"})
	if cfg.ConnConfig.RuntimeParams == nil {
		t.Fatal("expected RuntimeParams to be initialised")
	}
	if got := cfg.ConnConfig.RuntimeParams["lock_timeout"]; got != "3s" {
		t.Errorf("lock_timeout = %q, want 3s", got)
	}
}

// ---------------------------------------------------------------------------
// pingWithBackoff — cancelled-before-start branch (attempt 0).
// ---------------------------------------------------------------------------

// TestPingWithBackoff_CancelledBeforeFirstAttempt — callers may cancel
// before the loop reaches its first Ping; the helper must surface the
// cancellation as a wrapped ErrPingFailed naming zero completed attempts
// rather than attempting a single Ping on a dead context.
func TestPingWithBackoff_CancelledBeforeFirstAttempt(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := &retryPinger{failForever: true, failErr: context.DeadlineExceeded}
	err := pingWithBackoff(ctx, p)
	if err == nil {
		t.Fatal("expected cancellation error; got nil")
	}
	if !errors.Is(err, ErrPingFailed) {
		t.Errorf("expected wrapped ErrPingFailed, got %v", err)
	}
	if p.calls.Load() != 0 {
		t.Errorf("expected 0 Ping attempts on pre-cancelled ctx; got %d", p.calls.Load())
	}
	if !strings.Contains(err.Error(), "after 0 attempts") {
		t.Errorf("error should report zero attempts, got %q", err.Error())
	}
}
