# `libs/chora-go-common/bootstrap`

Service-startup helpers that decouple slow, failure-tolerant init paths
(OTLP exporter wiring) from latency-sensitive ones (pgx pool init,
Pub/Sub client init).

Aligned with tracker #151 (C(a).S1 path (b)) and CLAUDE.md §1 +
.claude/rules/development-execution.md TDD enforcement.

---

## Why

Pre-fix call pattern (pre-2026-05-14):

```go
ctx, cancel := context.WithTimeout(ctx, BootstrapTimeout)
defer cancel()

otelShutdown, err := otel.Init(ctx, serviceName, version)  // blocks
if err != nil { log.Fatal(...) }

dbPool, err := pgxpool.New(ctx, dsn)  // shares the same ctx; if otel ate 80s, pgx has 10s left
if err != nil { log.Fatal(...) }
```

Under a 4-container PgBouncer-sidecar cold-start the metadata-server
saturates; the OTLP exporter init (ADC token mint + Cloud Trace TLS
handshake) can swallow the bulk of the bootstrap deadline. pgx pool
init then races a near-empty context and the pod CrashLoopBackOffs.

## What

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

// Kick off OTLP init in its own goroutine with its own deadline +
// fail-soft. Does NOT block.
otlpHandle := observability.InitOTLPAsync(ctx, serviceName, version)
defer func() {
    shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
    defer c()
    res := otlpHandle.WaitContext(shutdownCtx)
    if err := res.Shutdown(shutdownCtx); err != nil {
        log.Printf("trace shutdown error: %v", err)
    }
}()

// pgx pool init now races OTLP for nothing — they have independent
// deadlines. pgx gets the FULL bootstrap budget.
pool, poolShutdown := bootstrapDBPool(ctx)
```

Timeouts and init errors degrade to a **no-op shutdown** so the rest
of bootstrap continues. Traces are recoverable on the next pod;
pgx pool failure crash-loops the pod and matters more.

## Public API

```go
package bootstrap

// DefaultOTLPInitTimeout is the fallback OTLP-init deadline (15s).
const DefaultOTLPInitTimeout = 15 * time.Second

// InitFunc matches libs/chora-go-common/otel.Init's signature so
// callers can pass it directly.
type InitFunc func(ctx context.Context) (func(context.Context) error, error)

type OTLPOptions struct {
    InitFunc    InitFunc       // nil → no-op handle
    InitTimeout time.Duration  // 0 → read CHORA_OTLP_INIT_TIMEOUT_SECONDS
    Logf        func(string)   // nil → log.Printf
}

type OTLPResult struct {
    Shutdown    func(context.Context) error // always non-nil; no-op on failure
    Initialized bool                        // true only on success
    TimedOut    bool
    InitError   error                       // surfaces underlying error
    Err         error                       // reserved; always nil today
}

type OTLPHandle struct { /* opaque */ }

func StartOTLPAsync(ctx context.Context, opts OTLPOptions) *OTLPHandle
func OTLPTimeoutFromEnv() time.Duration
func (h *OTLPHandle) Wait(maxWait time.Duration) OTLPResult
func (h *OTLPHandle) WaitContext(ctx context.Context) OTLPResult
```

And in `libs/chora-go-common/observability`:

```go
// Thin wrapper that delegates to bootstrap.StartOTLPAsync, preserving
// the existing global TracerProvider semantics + shutdown closure.
func InitOTLPAsync(ctx context.Context, serviceName, version string) *bootstrap.OTLPHandle
```

The synchronous `observability.InitOTLP(name, version)` function is
**unchanged** — callers migrate at their own cadence.

## Env-var knobs

| Var | Default | Purpose |
|---|---|---|
| `CHORA_OTLP_INIT_TIMEOUT_SECONDS` | `15` | OTLP-init deadline. `0` / garbage / negative falls back to default. Clamp it down (e.g. `5`) on regions where metadata-server is slow but Cloud Trace is fast. |

Per CLAUDE.md no-inline-config rule the value is sourced from env →
Terraform (non-secret) at deployment time. Do **NOT** bake it into
`deployment.yaml` as a literal — set it via the same env-var
resolution path the rest of the service uses.

## Migration recipe (per service)

```diff
-traceShutdown, err := cgcobservability.InitOTLP(serviceName, serviceVersion)
-if err != nil {
-    log.Printf("svc: observability init failed (non-fatal): %v", err)
-}
-defer func() {
-    if traceShutdown == nil { return }
-    if err := traceShutdown(); err != nil {
-        log.Printf("svc: trace shutdown error: %v", err)
-    }
-}()
-
-ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
-defer stop()
+ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
+defer stop()
+
+otlpHandle := cgcobservability.InitOTLPAsync(ctx, serviceName, serviceVersion)
+defer func() {
+    shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
+    defer c()
+    res := otlpHandle.WaitContext(shutdownCtx)
+    if err := res.Shutdown(shutdownCtx); err != nil {
+        log.Printf("svc: trace shutdown error: %v", err)
+    }
+}()
```

`chora-tenancy` (cmd/server/main.go) is the working example —
diff: ~13 lines net.

## Tests

```bash
go test -coverprofile=/tmp/cover.out ./bootstrap/...
go tool cover -func=/tmp/cover.out
```

Coverage gate: 85% (domain). Currently **92.5%**.

## References

- Tracker #151 — C(a).S1 root-cause analysis
- CLAUDE.md §1 — OTLP-everywhere policy
- `.claude/rules/development-execution.md` — TDD enforcement
- `libs/chora-go-common/otel/otel.go` — underlying Cloud Trace exporter wiring
