// Package db bootstrap tests — RED phase first for ATOM-1c.
//
// Mirrors the FetchSecretWithBackoff test battery at
// chora-common/secrets/backoff_test.go (commit 4c50a1d5) for the
// db.Bootstrap Ping step.
//
// Rationale: per ATOM-1 §"Cold-start note" + `feedback_d6_resilience_first_class`:
//
//	creation: pgx pool bootstrap failed (env set, fail-loud — kubelet
//	    will CrashLoopBackOff):
//	  db.Bootstrap: ping: context deadline exceeded
//
// A fresh node has not yet warmed the Cloud SQL Auth Proxy sidecar +
// IP routing tables. The first Ping on the boot path can context-deadline
// despite Secret Manager succeeding — and pods historically restarted
// 4-6× before kubelet's CrashLoopBackOff retry happened to coincide
// with proxy readiness. Failing fast is the wrong shape: the next ping
// 500ms-2s later would succeed.
//
// This test battery drives the addition of an in-process retry-backoff
// around the Ping step that mirrors secrets.FetchSecretWithBackoff (6
// attempts / 500ms base / 8s cap / 30s budget) BEFORE the Bootstrap
// function returns the fatal "context deadline exceeded" error.
//
// The retry loop is exposed via an internal helper pingWithBackoff(ctx,
// pinger) so tests can inject a programmable mock pinger without
// spinning a real Postgres.
package db

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// retryPinger — programmable mock satisfying the pinger interface.
// -----------------------------------------------------------------------------

type retryPinger struct {
	calls       atomic.Int32
	failFirstN  int32
	failForever bool
	failErr     error
	sleepEvery  time.Duration
}

func (p *retryPinger) Ping(ctx context.Context) error {
	if p.sleepEvery > 0 {
		t := time.NewTimer(p.sleepEvery)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	n := p.calls.Add(1)
	if p.failForever {
		return p.failErr
	}
	if n <= p.failFirstN {
		return p.failErr
	}
	return nil
}

// -----------------------------------------------------------------------------
// pingWithBackoff — retry-loop happy path.
// -----------------------------------------------------------------------------

// TestPingWithBackoff_FirstAttemptSucceeds — when the proxy is already
// warm, Bootstrap MUST not pay any backoff cost on the happy path.
func TestPingWithBackoff_FirstAttemptSucceeds(t *testing.T) {
	t.Parallel()
	p := &retryPinger{} // no programmed failures
	start := time.Now()
	if err := pingWithBackoff(context.Background(), p); err != nil {
		t.Fatalf("expected first-shot success; got %v", err)
	}
	elapsed := time.Since(start)
	if p.calls.Load() != 1 {
		t.Errorf("expected exactly 1 attempt on happy path; got %d", p.calls.Load())
	}
	// Must not sleep on success.
	if elapsed > 500*time.Millisecond {
		t.Errorf("happy path took %s — should be near-zero", elapsed)
	}
}

// TestPingWithBackoff_TransientFailuresResolvedByRetry — 3× context
// deadline then OK. Mirrors the canonical
// FetchSecretWithBackoff_TransientFailures test. Expect success after
// >= 4 attempts within retry budget.
func TestPingWithBackoff_TransientFailuresResolvedByRetry(t *testing.T) {
	p := &retryPinger{
		failFirstN: 3,
		failErr:    context.DeadlineExceeded,
	}
	start := time.Now()
	err := pingWithBackoff(context.Background(), p)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected success after transient failures; got %v (after %s, %d attempts)",
			err, elapsed, p.calls.Load())
	}
	if p.calls.Load() < 4 {
		t.Fatalf("expected >= 4 attempts (3 transient + 1 success); got %d", p.calls.Load())
	}
	if elapsed > pingRetryTotalBudget+2*time.Second {
		t.Errorf("retry-backoff exceeded budget: elapsed %s budget %s", elapsed, pingRetryTotalBudget)
	}
}

// TestPingWithBackoff_PersistentFailureExhaustsBudget — Ping always
// times out. Expect wrapped ErrPingFailed after the attempt budget.
func TestPingWithBackoff_PersistentFailureExhaustsBudget(t *testing.T) {
	p := &retryPinger{
		failForever: true,
		failErr:     context.DeadlineExceeded,
	}
	err := pingWithBackoff(context.Background(), p)
	if err == nil {
		t.Fatalf("expected error on persistent failure; got nil")
	}
	if !errors.Is(err, ErrPingFailed) {
		t.Fatalf("expected errors.Is(err, ErrPingFailed); got %v", err)
	}
	if p.calls.Load() < 2 {
		t.Errorf("expected retry to run at least twice; got %d attempts", p.calls.Load())
	}
	if !strings.Contains(err.Error(), "attempts") {
		t.Errorf("error should mention attempt count, got %q", err.Error())
	}
}

// TestPingWithBackoff_HonoursContextCancel — Caller cancels mid-backoff.
// Loop must abort promptly + bubble up ctx.Err() wrapped in
// ErrPingFailed.
func TestPingWithBackoff_HonoursContextCancel(t *testing.T) {
	p := &retryPinger{
		failForever: true,
		failErr:     context.DeadlineExceeded,
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := pingWithBackoff(ctx, p)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected context-cancel error; got nil")
	}
	if !errors.Is(err, ErrPingFailed) {
		t.Errorf("expected errors.Is(err, ErrPingFailed); got %v", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("ctx-cancel did not abort backoff promptly: elapsed %s", elapsed)
	}
}

// TestPingWithBackoff_RespectsCallerDeadline — When the caller supplies
// its OWN context deadline (e.g. db.Bootstrap passes a 90s
// CHORA_BOOTSTRAP_TIMEOUT_SECONDS ctx), the helper MUST respect it as a
// wall-clock bound and not over-run on its own internal retry budget.
func TestPingWithBackoff_RespectsCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	p := &retryPinger{
		failForever: true,
		failErr:     context.DeadlineExceeded,
	}
	start := time.Now()
	err := pingWithBackoff(ctx, p)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected error; got nil")
	}
	if !errors.Is(err, ErrPingFailed) {
		t.Errorf("expected errors.Is(err, ErrPingFailed); got %v", err)
	}
	if elapsed > 600*time.Millisecond {
		t.Errorf("elapsed %s > caller deadline 250ms — helper ignored caller deadline", elapsed)
	}
	if elapsed < 200*time.Millisecond {
		t.Errorf("elapsed %s < caller deadline 250ms — helper failed to use the full deadline", elapsed)
	}
}

// TestPingWithBackoff_NilPingerFailsFast — Nil pinger is a programmer
// error; surface it immediately.
func TestPingWithBackoff_NilPingerFailsFast(t *testing.T) {
	t.Parallel()
	err := pingWithBackoff(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected error on nil pinger; got nil")
	}
}

// TestPingWithBackoff_PlainErrorIsRetriable — A plain non-context error
// (e.g. network blip surfaced by pgx as a wrapped IO error) is retried
// the same as a context.DeadlineExceeded. Persistent plain errors still
// exhaust the budget and return ErrPingFailed.
func TestPingWithBackoff_PlainErrorIsRetriable(t *testing.T) {
	p := &retryPinger{
		failForever: true,
		failErr:     errors.New("dial tcp 127.0.0.1:5432: connect: connection refused"),
	}
	err := pingWithBackoff(context.Background(), p)
	if err == nil {
		t.Fatalf("expected error; got nil")
	}
	if !errors.Is(err, ErrPingFailed) {
		t.Fatalf("expected errors.Is(err, ErrPingFailed); got %v", err)
	}
	if p.calls.Load() < 2 {
		t.Errorf("expected backoff to retry plain errors; got %d attempts", p.calls.Load())
	}
}

// TestPingWithBackoff_LogsRecoveryAfterRetry — Smoke check: after a
// transient run the helper succeeds. We don't assert on log content
// (slog), but the call MUST complete without error and report the
// expected attempt count.
func TestPingWithBackoff_LogsRecoveryAfterRetry(t *testing.T) {
	p := &retryPinger{
		failFirstN: 2,
		failErr:    context.DeadlineExceeded,
	}
	if err := pingWithBackoff(context.Background(), p); err != nil {
		t.Fatalf("expected success; got %v", err)
	}
	if p.calls.Load() < 3 {
		t.Errorf("expected >= 3 attempts (2 transient + 1 success); got %d", p.calls.Load())
	}
}

// -----------------------------------------------------------------------------
// Constants sanity — guard the tuning curve from drift.
// -----------------------------------------------------------------------------

// TestPingRetryTuning_MirrorsSecretsBackoff — The Ping retry curve MUST
// match the secrets.FetchSecretWithBackoff curve so the platform has
// ONE backoff shape to reason about across cold-start surfaces.
func TestPingRetryTuning_MirrorsSecretsBackoff(t *testing.T) {
	t.Parallel()
	if pingRetryAttempts != 6 {
		t.Errorf("pingRetryAttempts = %d, want 6 (mirror secrets backoff)", pingRetryAttempts)
	}
	if pingRetryBaseDelay != 500*time.Millisecond {
		t.Errorf("pingRetryBaseDelay = %v, want 500ms", pingRetryBaseDelay)
	}
	if pingRetryMaxDelay != 8*time.Second {
		t.Errorf("pingRetryMaxDelay = %v, want 8s", pingRetryMaxDelay)
	}
	if pingRetryTotalBudget != 30*time.Second {
		t.Errorf("pingRetryTotalBudget = %v, want 30s", pingRetryTotalBudget)
	}
	if pingAttemptTimeout != 5*time.Second {
		t.Errorf("pingAttemptTimeout = %v, want 5s", pingAttemptTimeout)
	}
}
