// Package bootstrap provides service-startup helpers that decouple slow,
// failure-tolerant init paths (OTLP exporter wiring, future async warm-up)
// from latency-sensitive ones (pgx pool init, Pub/Sub client init).
//
// # The bootstrap-budget problem
//
// Pre-fix call pattern (tracker #151, C(a).S1):
//
//	ctx, cancel := context.WithTimeout(ctx, BootstrapTimeout)
//	defer cancel()
//
//	otelShutdown, err := otel.Init(ctx, serviceName, version)   // blocks
//	if err != nil { log.Fatal(...) }
//
//	dbPool, err := pgxpool.New(ctx, dsn)                        // shares ctx
//	if err != nil { log.Fatal(...) }
//
// Under a 4-container PgBouncer-sidecar cold-start the metadata-server
// saturates; the OTLP exporter init (ADC token mint + Cloud Trace TLS
// handshake) can swallow the bulk of the bootstrap deadline. pgx pool
// init then races a near-empty context and crash-loops the pod.
//
// # The fix (path b)
//
// OTLP exporter init now runs in its own goroutine with its own
// (shorter, env-tunable) deadline + fail-soft semantics: timeout +
// init-error degrade to a no-op shutdown so the service continues
// booting without traces (traces are recoverable on next pod; pgx
// pool failure crash-loops the pod and matters more).
//
// pgx pool init keeps the FULL bootstrap deadline; the caller owns
// that part since pgx-bootstrap signatures vary across services.
//
// # Public API contract
//
//   - Backward-compatible — existing observability.InitOTLP /
//     otel.Init callers continue to work without edits.
//   - Caller migration is OPTIONAL — services migrate at their own
//     cadence by swapping their main() to call
//     bootstrap.StartOTLPAsync(...) and use handle.Wait(...).
//
// Env-var knobs:
//
//	CHORA_OTLP_INIT_TIMEOUT_SECONDS — OTLP init deadline (default 15s,
//	                                  clamped to ≥ 1s; <=0 / garbage
//	                                  falls back to default).
//
// Aligned with: Architecture Review locked 2026-05-07 Tier 3 D13
// (OTLP everywhere) + .claude/rules/development-execution.md TDD
// + agentic-resilience-d6 fail-soft observability semantics.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

// DefaultOTLPInitTimeout is the fallback OTLP-init deadline when
// CHORA_OTLP_INIT_TIMEOUT_SECONDS is unset / garbage. 15s is long
// enough to absorb a typical Cloud Trace TLS handshake but short
// enough that a slow metadata-server can't hold the rest of boot
// hostage. Operators tune via env var per CLAUDE.md no-inline-config
// rule.
const DefaultOTLPInitTimeout = 15 * time.Second

// InitFunc is the OTLP-init function the helper drives. Matches
// chora-common/otel.Init's signature so callers can pass it
// directly:
//
//	bootstrap.StartOTLPAsync(ctx, bootstrap.OTLPOptions{
//	  InitFunc: func(c context.Context) (func(context.Context) error, error) {
//	    return otel.Init(c, "chora-tenancy", version)
//	  },
//	})
type InitFunc func(ctx context.Context) (func(context.Context) error, error)

// OTLPOptions configures StartOTLPAsync. InitFunc is the only required
// field; everything else has sensible defaults.
type OTLPOptions struct {
	// InitFunc is the OTLP-init function to drive. When nil, the helper
	// degrades to a no-op (Initialized=false, Shutdown=no-op, no error).
	InitFunc InitFunc

	// InitTimeout overrides the OTLP-init deadline. When 0, picks up
	// CHORA_OTLP_INIT_TIMEOUT_SECONDS (or DefaultOTLPInitTimeout).
	InitTimeout time.Duration

	// Logf is the log function used for fail-soft diagnostics. Defaults
	// to log.Printf when nil; tests override with a buffer-writer.
	Logf func(string)
}

// OTLPResult is the outcome of an OTLP-init attempt. Shutdown is
// always non-nil — a no-op when init failed/timed out so callers can
// `defer res.Shutdown(ctx)` unconditionally.
type OTLPResult struct {
	// Shutdown is the trace-provider shutdown func to defer in main().
	// Non-nil; a no-op when Initialized is false.
	Shutdown func(context.Context) error

	// Initialized is true ONLY when InitFunc returned a real shutdown.
	// false on timeout, init error, nil InitFunc, or cancelled ctx.
	Initialized bool

	// TimedOut is true when InitFunc didn't return within InitTimeout.
	TimedOut bool

	// InitError surfaces the underlying error for structured logging /
	// diagnostics. Wait() callers should treat this as informational —
	// the helper's contract is to NOT bubble OTLP failures up as fatal.
	InitError error

	// Err is reserved for protocol-level errors in the helper itself
	// (currently always nil — the fail-soft contract guarantees the
	// caller never sees a non-nil Err here).
	Err error
}

// OTLPHandle is the future-like handle StartOTLPAsync returns. Callers
// either Wait(maxWait) before doing latency-sensitive bootstrap work,
// or call WaitContext(ctx) to bound the wait by another context.
type OTLPHandle struct {
	mu         sync.Mutex
	done       chan struct{}
	settled    bool
	res        OTLPResult
	timeoutDur time.Duration
}

// OTLPTimeoutFromEnv reads CHORA_OTLP_INIT_TIMEOUT_SECONDS and returns
// the resolved timeout. Falls back to DefaultOTLPInitTimeout when:
//
//   - env var is unset or empty
//   - value is not a valid integer
//   - value is zero or negative
//
// Exported so operators + tests can verify env-parsing without invoking
// the full helper.
func OTLPTimeoutFromEnv() time.Duration {
	raw := os.Getenv("CHORA_OTLP_INIT_TIMEOUT_SECONDS")
	if raw == "" {
		return DefaultOTLPInitTimeout
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return DefaultOTLPInitTimeout
	}
	return time.Duration(n) * time.Second
}

// StartOTLPAsync kicks off OTLP init in its own goroutine with its own
// deadline + fail-soft. Returns immediately with a handle the caller
// uses to retrieve the shutdown closure when it's ready (or has fallen
// back to a no-op).
//
// The caller's ctx is observed for cancellation but is NOT used as
// the deadline for InitFunc — InitFunc runs under its own context with
// timeout = opts.InitTimeout (or CHORA_OTLP_INIT_TIMEOUT_SECONDS).
//
// Cancellation semantics:
//
//   - opts.InitFunc nil → handle resolves immediately to no-op
//     (Initialized=false). Lets test/dev mains skip OTLP cleanly.
//   - parent ctx cancelled before InitFunc returns → InitFunc's own
//     ctx is also cancelled (we chain), but Wait still returns a
//     no-op shutdown (fail-soft).
//   - InitFunc panic → recovered + logged + no-op shutdown.
func StartOTLPAsync(ctx context.Context, opts OTLPOptions) *OTLPHandle {
	logf := opts.Logf
	if logf == nil {
		logf = func(msg string) { log.Print(msg) }
	}

	timeout := opts.InitTimeout
	if timeout <= 0 {
		timeout = OTLPTimeoutFromEnv()
	}

	h := &OTLPHandle{
		done:       make(chan struct{}),
		timeoutDur: timeout,
		res: OTLPResult{
			Shutdown: noopShutdown,
		},
	}

	if opts.InitFunc == nil {
		h.settle(OTLPResult{
			Shutdown:    noopShutdown,
			Initialized: false,
		})
		return h
	}

	// initCtx chains opts.InitTimeout off the caller's ctx so the helper
	// honours both deadlines (whichever fires first). The inner ctx is
	// cancelled when this function returns OR when initFn finishes.
	initCtx, cancel := context.WithTimeout(ctx, timeout)

	go func() {
		defer cancel()

		type initOutcome struct {
			shutdown func(context.Context) error
			err      error
		}
		outcome := make(chan initOutcome, 1)

		go func() {
			defer func() {
				if r := recover(); r != nil {
					outcome <- initOutcome{
						err: fmt.Errorf("otel: init panicked: %v", r),
					}
				}
			}()
			shutdown, err := opts.InitFunc(initCtx)
			outcome <- initOutcome{shutdown: shutdown, err: err}
		}()

		select {
		case o := <-outcome:
			if o.err != nil {
				logf(fmt.Sprintf("otel: init failed (continuing without traces): %v", o.err))
				h.settle(OTLPResult{
					Shutdown:    noopShutdown,
					Initialized: false,
					InitError:   o.err,
				})
				return
			}
			if o.shutdown == nil {
				h.settle(OTLPResult{
					Shutdown:    noopShutdown,
					Initialized: false,
					InitError:   errors.New("otel: init returned nil shutdown"),
				})
				return
			}
			h.settle(OTLPResult{
				Shutdown:    o.shutdown,
				Initialized: true,
			})

		case <-initCtx.Done():
			err := initCtx.Err()
			timedOut := errors.Is(err, context.DeadlineExceeded)
			if timedOut {
				logf(fmt.Sprintf("otel: init deadline %v exceeded (continuing without traces)", timeout))
			} else {
				logf(fmt.Sprintf("otel: init cancelled before completion (%v); continuing without traces", err))
			}
			h.settle(OTLPResult{
				Shutdown:    noopShutdown,
				Initialized: false,
				TimedOut:    timedOut,
				InitError:   err,
			})
		}
	}()

	return h
}

// Wait blocks until OTLP init finishes or maxWait elapses. When
// maxWait is 0, blocks until completion (useful in tests).
//
// Wait is idempotent and concurrency-safe — repeated calls return
// the same result without re-running InitFunc.
func (h *OTLPHandle) Wait(maxWait time.Duration) OTLPResult {
	if maxWait <= 0 {
		<-h.done
		return h.snapshot()
	}
	select {
	case <-h.done:
		return h.snapshot()
	case <-time.After(maxWait):
		// Best-effort return. The underlying goroutine continues and
		// settles asynchronously; the caller gets a no-op shutdown
		// snapshot reflecting "not yet ready".
		return OTLPResult{
			Shutdown:    noopShutdown,
			Initialized: false,
			TimedOut:    true,
		}
	}
}

// WaitContext is the ctx-bounded variant of Wait — useful when the
// caller already has a bootstrap-deadline context.
func (h *OTLPHandle) WaitContext(ctx context.Context) OTLPResult {
	select {
	case <-h.done:
		return h.snapshot()
	case <-ctx.Done():
		return OTLPResult{
			Shutdown:    noopShutdown,
			Initialized: false,
			TimedOut:    errors.Is(ctx.Err(), context.DeadlineExceeded),
		}
	}
}

func (h *OTLPHandle) settle(r OTLPResult) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.settled {
		return
	}
	if r.Shutdown == nil {
		r.Shutdown = noopShutdown
	}
	h.res = r
	h.settled = true
	close(h.done)
}

func (h *OTLPHandle) snapshot() OTLPResult {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.res
}

// noopShutdown is the always-non-nil sentinel returned when OTLP init
// fails / times out / wasn't requested. Lets callers defer the
// shutdown unconditionally without nil-checks.
func noopShutdown(context.Context) error { return nil }
