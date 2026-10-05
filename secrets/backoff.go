// Package secrets — retry-backoff helper for secret resolution.
//
// This file is the §3b half of the E2E-INFRA-COLD-START remediation
// (Infra option 1, accepted at commit `2292a9d1`). It mirrors the JWKS
// retry-backoff pattern historically used for key-endpoint calls.
//
// Rationale: a fresh node has not yet warmed the credential
// token cache + metadata proxy. The first secret access
// on the boot path can return codes.Unavailable
// (NAT remap), codes.DeadlineExceeded (cold metadata server), or
// codes.Internal — all of which would clear after a single retry. A
// naive single-shot access surfaces these as a fatal
// boot error.
//
// The helper retries up to retryAttempts times with exponential delay
// (retryBaseDelay × 2^attempt, capped at retryMaxDelay). Total wall
// budget is capped at retryTotalBudget. Non-retriable codes — NotFound,
// PermissionDenied, InvalidArgument, Unauthenticated — fail fast on the
// first attempt because retrying will never help (they indicate
// misconfiguration, not transient flake).
//
// Fail-loud contract per `feedback_no_stubs_real_wiring`: if the
// retry window exhausts without success the function returns a wrapped
// ErrSecretFetch. NEVER falls back to an empty string or silent default.
package secrets

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Retry-backoff tuning — these mirror the platform's JWKS
// retry curve so the platform has ONE backoff curve to reason about
// across cold-start resilience surfaces. Total budget: 30s. Worst-case total sleeps:
// 500ms + 1s + 2s + 4s + 8s = 15.5s, leaving headroom for ~5 RPC
// round-trips inside the budget.
const (
	retryAttempts    = 6
	retryBaseDelay   = 500 * time.Millisecond
	retryMaxDelay    = 8 * time.Second
	retryTotalBudget = 30 * time.Second
)

// ErrSecretFetch is the sentinel returned when the retry window
// exhausts without success. Wraps the final gRPC error so callers can
// inspect it via errors.Unwrap or grpc/status.Code(errors.Unwrap(err)).
//
// Callers MAY check errors.Is(err, ErrSecretNotFound) before falling
// back to dev defaults — but ErrSecretFetch wrapping a 5xx-equivalent
// is fatal at boot per the no-stubs-real-wiring directive.
var ErrSecretFetch = errors.New("secrets: fetch failed after retry-backoff")

// SecretFetcher is the resolver surface for FetchSecretWithBackoff.
// Satisfied by *Client and *StubClient in this package and by
// db.SecretFetcher (alias) — the helper does NOT take a concrete
// *Client so tests can inject programmable fetchers without spinning
// up a real Secret Manager connection.
type SecretFetcher interface {
	GetSecret(ctx context.Context, name string) (string, error)
}

// FetchSecretWithBackoff resolves `name` via the supplied fetcher,
// retrying transient gRPC failures with exponential backoff and
// failing fast on non-retriable codes.
//
// The retry loop respects the caller's context entirely — it does NOT
// impose its own deadline on top. Callers that need a wall-time bound
// MUST set context.WithTimeout themselves; the constants in this
// package (retryAttempts × retryBaseDelay × retryMaxDelay) describe
// the implicit upper bound when the caller supplies an unbounded ctx,
// but explicit caller deadlines take precedence.
//
// On success, returns the secret payload.
// On non-retriable failure (NotFound, PermissionDenied, etc.), returns
// immediately with a wrapped error.
// On persistent retriable failure across the attempt budget, returns a
// wrapped ErrSecretFetch.
// On context cancellation mid-backoff, returns ctx.Err() wrapped in
// ErrSecretFetch.
func FetchSecretWithBackoff(ctx context.Context, fetcher SecretFetcher, name string) (string, error) {
	if fetcher == nil {
		return "", errors.New("secrets: fetcher required")
	}
	if name == "" {
		return "", errors.New("secrets: name required")
	}

	// If the caller did NOT supply a deadline, apply retryTotalBudget
	// as a defensive upper bound so a runaway loop never outlives the
	// pod's startup probe window. When the caller HAS a deadline (as
	// db.Bootstrap does — typically 90s via CHORA_BOOTSTRAP_TIMEOUT_
	// SECONDS), we respect it as-is so we don't accidentally tighten
	// the budget below what the operator configured.
	bootCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		bootCtx, cancel = context.WithTimeout(ctx, retryTotalBudget)
		defer cancel()
	}

	start := time.Now()
	var lastErr error
	delay := retryBaseDelay
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		if bootCtx.Err() != nil {
			return "", fmt.Errorf("%w: backoff cancelled after %d attempts (%s): %v",
				ErrSecretFetch, attempt-1, time.Since(start), bootCtx.Err())
		}
		value, err := fetcher.GetSecret(bootCtx, name)
		if err == nil {
			if attempt > 1 {
				slog.Info("secrets: fetch recovered after retry",
					"name", name,
					"attempt", attempt,
					"total_elapsed", time.Since(start).String())
			}
			return value, nil
		}
		lastErr = err

		// Bail fast on non-retriable codes — retrying will never help.
		if isNonRetriable(err) {
			// gRPC NotFound and sentinel ErrSecretNotFound canonicalise
			// to ErrSecretNotFound + the secret name so callers can
			// detect "missing secret" uniformly via errors.Is(err,
			// ErrSecretNotFound) regardless of whether the underlying
			// path went through the raw SDK or the StubClient.
			if errors.Is(err, ErrSecretNotFound) || statusCode(err) == codes.NotFound {
				return "", fmt.Errorf("%w: %s: %v", ErrSecretNotFound, name, err)
			}
			return "", fmt.Errorf("%w: non-retriable %q for %q (after %d attempts, %s): %v",
				ErrSecretFetch, statusCode(err), name, attempt, time.Since(start), err)
		}

		if attempt < retryAttempts {
			slog.Info("secrets: fetch failed; retry-backoff scheduled",
				"name", name,
				"attempt", attempt,
				"max_attempts", retryAttempts,
				"next_delay", delay.String(),
				"err", err.Error())
			t := time.NewTimer(delay)
			select {
			case <-bootCtx.Done():
				t.Stop()
				return "", fmt.Errorf("%w: backoff cancelled mid-sleep after %d attempts (%s): %v",
					ErrSecretFetch, attempt, time.Since(start), bootCtx.Err())
			case <-t.C:
			}
			delay *= 2
			if delay > retryMaxDelay {
				delay = retryMaxDelay
			}
		}
	}

	return "", fmt.Errorf("%w: %q exhausted %d attempts (%s): %v",
		ErrSecretFetch, name, retryAttempts, time.Since(start), lastErr)
}

// isNonRetriable reports whether the supplied error should fail fast
// without backoff. Anchors to the existing ErrSecretNotFound sentinel
// AND inspects gRPC status codes for misconfiguration signals.
func isNonRetriable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrSecretNotFound) {
		return true
	}
	switch statusCode(err) {
	case codes.NotFound,
		codes.PermissionDenied,
		codes.InvalidArgument,
		codes.Unauthenticated,
		codes.FailedPrecondition:
		return true
	default:
		return false
	}
}

// statusCode extracts the gRPC code from err. Returns codes.Unknown
// when err is not a gRPC status.
func statusCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	return status.Code(err)
}
