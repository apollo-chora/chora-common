// Package db — retry-backoff for the Bootstrap Ping step.
//
// This file is the ATOM-1c §"Cold-start note" remediation. It mirrors
// the retry-backoff pattern at
// chora-common/secrets/backoff.go (commit 4c50a1d5) for
// pgxpool.Pool.Ping calls during cold start.
//
// Rationale: per ATOM-1 + `feedback_d6_resilience_first_class`:
//
//	creation: pgx pool bootstrap failed (env set, fail-loud — kubelet
//	    will CrashLoopBackOff):
//	  db.Bootstrap: ping: context deadline exceeded
//
// A fresh node has not yet warmed the Postgres sidecar
// + IP routing tables. The first Ping on the boot path can
// context-deadline despite secret resolution succeeding — and pods
// historically restarted 4-6× before kubelet's CrashLoopBackOff retry
// happened to coincide with proxy readiness.
//
// Failing fast is the wrong shape: the next Ping ~500ms-2s later
// would succeed. This helper retries up to pingRetryAttempts times
// with exponential delay (pingRetryBaseDelay × 2^attempt, capped at
// pingRetryMaxDelay). Total wall budget capped at
// pingRetryTotalBudget when caller supplies an unbounded ctx; caller
// deadlines take precedence so the operator's CHORA_BOOTSTRAP_TIMEOUT
// envelope is never tightened.
//
// Fail-loud contract per `feedback_no_stubs_real_wiring`: if the
// retry window exhausts without success the function returns a wrapped
// ErrPingFailed. NEVER falls back silently.
package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Retry tuning — these mirror retryAttempts/retryBaseDelay/etc. in
// chora-common/secrets/backoff.go so the platform has ONE
// backoff curve to reason about across cold-start resilience surfaces.
// Total worst-case sleep: 500ms + 1s + 2s + 4s + 8s = 15.5s, leaving
// headroom for ~5 RPC round-trips inside the 30s budget.
const (
	pingRetryAttempts    = 6
	pingRetryBaseDelay   = 500 * time.Millisecond
	pingRetryMaxDelay    = 8 * time.Second
	pingRetryTotalBudget = 30 * time.Second
	// pingAttemptTimeout bounds a single Ping call so a hung proxy can
	// not eat the full budget. Tight enough for a healthy proxy
	// (typical Ping < 50ms) + tolerant of cold-start metadata-server
	// jitter.
	pingAttemptTimeout = 5 * time.Second
)

// ErrPingFailed is the sentinel returned when pingWithBackoff exhausts
// the retry budget without a successful Ping. Wraps the final error so
// callers can inspect via errors.Unwrap.
var ErrPingFailed = errors.New("db: ping failed after retry-backoff")

// pinger is the surface satisfied by *pgxpool.Pool.Ping. Extracted as
// an interface so bootstrap_test.go can inject a programmable mock
// without spinning a real Postgres.
type pinger interface {
	Ping(ctx context.Context) error
}

// pingWithBackoff retries pool.Ping on transient failures with
// exponential backoff and fails fast on context cancellation.
//
// The retry loop respects the caller's context entirely — it does NOT
// impose its own deadline on top. If the caller supplies an unbounded
// ctx, the helper applies pingRetryTotalBudget as a defensive ceiling
// so a runaway loop never outlives the pod's startup probe window.
//
// On success, returns nil.
// On context cancellation mid-backoff, returns ctx.Err() wrapped in
// ErrPingFailed.
// On persistent failure across the attempt budget, returns a wrapped
// ErrPingFailed naming the final attempt count and elapsed time.
func pingWithBackoff(ctx context.Context, p pinger) error {
	if p == nil {
		return errors.New("db: pinger required")
	}

	// Bound the loop by the smaller of (caller deadline,
	// pingRetryTotalBudget). When the caller has a deadline (as
	// db.Bootstrap does — typically 90s via
	// CHORA_BOOTSTRAP_TIMEOUT_SECONDS), we respect it verbatim. When
	// no deadline, we add pingRetryTotalBudget so a runaway never
	// outlives the pod startup window.
	bootCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		bootCtx, cancel = context.WithTimeout(ctx, pingRetryTotalBudget)
		defer cancel()
	}

	start := time.Now()
	var lastErr error
	delay := pingRetryBaseDelay
	for attempt := 1; attempt <= pingRetryAttempts; attempt++ {
		if err := bootCtx.Err(); err != nil {
			return fmt.Errorf("%w: backoff cancelled after %d attempts (%s): %v",
				ErrPingFailed, attempt-1, time.Since(start), err)
		}

		// Bound each Ping so a single hung proxy cannot eat the budget.
		// Per-attempt timeout is capped to whatever remains of bootCtx.
		attemptCtx, attemptCancel := context.WithTimeout(bootCtx, pingAttemptTimeout)
		err := p.Ping(attemptCtx)
		attemptCancel()

		if err == nil {
			if attempt > 1 {
				slog.Info("db: ping recovered after retry",
					"attempt", attempt,
					"total_elapsed", time.Since(start).String())
			}
			return nil
		}
		lastErr = err

		if attempt < pingRetryAttempts {
			slog.Info("db: ping failed; retry-backoff scheduled",
				"attempt", attempt,
				"max_attempts", pingRetryAttempts,
				"next_delay", delay.String(),
				"err", err.Error())
			t := time.NewTimer(delay)
			select {
			case <-bootCtx.Done():
				t.Stop()
				return fmt.Errorf("%w: backoff cancelled mid-sleep after %d attempts (%s): %v",
					ErrPingFailed, attempt, time.Since(start), bootCtx.Err())
			case <-t.C:
			}
			delay *= 2
			if delay > pingRetryMaxDelay {
				delay = pingRetryMaxDelay
			}
		}
	}

	return fmt.Errorf("%w: exhausted %d attempts (%s): %v",
		ErrPingFailed, pingRetryAttempts, time.Since(start), lastErr)
}
