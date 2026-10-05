# chora-common

Shared Go library consolidating cross-cutting patterns used across the 14 Chora Go services.

> Aligned with **Architecture Review locked 2026-05-07**. Source: `docs/architecture-review-inputs-2026-05-07.md` Tier 2 + Tier 3 + memory `feedback_no_inline_config`.

## Purpose

Every Chora Go service repeats the same plumbing:

- Loading + validating env vars
- Wiring OTLP traces via the OTel Collector
- Propagating W3C `traceparent` across HTTP boundaries
- Structured logging
- Typed errors with codes
- Building event envelopes per CLAUDE.md §6
- Calling other services with retry + timeout

This module extracts those into a single dependency. Services opt-in by importing `github.com/apollo-chora/chora-common`.

**Library is OPT-IN.** Existing services were not refactored to use it — that work is deferred to a follow-up task.

## Packages

| Package | Purpose |
|---|---|
| [`env`](./env) | `MustGet` / `GetOrDefault` / `LoadStruct` — fail-fast env loading per `feedback_no_inline_config` |
| [`otel`](./otel) | OTLP trace setup (`Init`, `Tracer`) via `OTEL_EXPORTER_OTLP_ENDPOINT`; falls back to stdout in dev |
| [`tracing`](./tracing) | `Middleware()` (Chi-compat) + `Inject(req)` + ctx helpers (traceparent / tenant_id / gcid) |
| [`log`](./log) | zap-based structured logger with `WithContext` for trace correlation |
| [`errors`](./errors) | `chora.Error` typed error (code + reason + traceID) |
| [`envelope`](./envelope) | `Build` + `Validate` + `ValidateStrict` + `CanonicaliseImdaDimension` for the mandatory event envelope (per CLAUDE.md §6 + ADR-141) |
| [`eventbus`](./eventbus) | Schema-validating Publisher contract + InMemoryBus test fixture + ReorderBuffer + JetStream adapter (NATS) |
| [`outbox`](./outbox) | Transactional outbox primitive: atomic state-write + outbox-row write; long-running Relay drains pending rows to the bus with reorder buffer + DLQ + retry |
| [`idempotent`](./idempotent) | Idempotency-key store for at-least-once subscribers: `Process(ctx, key, ttl, fn)` + Postgres-backed `Mark`/`Seen`/`CleanupExpired` |
| [`secrets`](./secrets) | Env-backed secret resolver (`Client`, `FetchSecretWithBackoff`) — no cloud SDK |
| [`modelarmor`](./modelarmor) | Guardrail `Screener` port + local regex/deny-list substitute (`LocalScreener`) + `StubScreener` |
| [`httpclient`](./httpclient) | Pre-configured HTTP client with retry, timeout, and traceparent injection |
| [`tests`](./tests) | Cross-package integration tests (outbox round-trip + chaos resume, env→envelope flow) |

## Usage

### Bootstrap a service

```go
package main

import (
    "context"

    chenv "github.com/apollo-chora/chora-common/env"
    chlog "github.com/apollo-chora/chora-common/log"
    chotel "github.com/apollo-chora/chora-common/otel"
)

type Config struct {
    Port  string `env:"CHORA_PORT,default=8080"`
    DBURL string `env:"CHORA_DB_URL,required"`
}

func main() {
    var cfg Config
    if err := chenv.LoadStruct(&cfg); err != nil {
        panic(err)
    }
    logger := chlog.New("chora-creation")
    defer logger.Sync()

    ctx := context.Background()
    shutdown, err := chotel.Init(ctx, "chora-creation", "v0.1.0")
    if err != nil {
        logger.Error("otel init", chlog.Err(err))
        return
    }
    defer shutdown(ctx)

    // ... wire chi router, mount tracing.Middleware(), serve on cfg.Port
}
```

### Wire the HTTP middleware

```go
import (
    "github.com/go-chi/chi/v5"
    "github.com/apollo-chora/chora-common/tracing"
)

r := chi.NewRouter()
r.Use(tracing.Middleware())
```

### Build a event envelope

```go
import (
    "github.com/apollo-chora/chora-common/envelope"
)

env := envelope.Build(ctx, envelope.BuildOpts{
    EventType:     "atom_published",
    SchemaVersion: 1,
    SourceProject: "chora-content",
    SourceService: "chora-creation",
})
if err := envelope.Validate(env); err != nil {
    return err
}
// ... marshal to chora-contracts proto, publish to chora.creation.atom.published.v1
```

### Call another service

```go
import (
    chenv "github.com/apollo-chora/chora-common/env"
    "github.com/apollo-chora/chora-common/httpclient"
)

base := chenv.MustGet("CHORA_BFF_GATEWAY_URL") // no inline config
c, err := httpclient.New(base)
// c.Get(ctx, "/health") — traceparent auto-propagated
```

## Conventions enforced

- **No inline config** — every URL/secret/topic name MUST flow through env (per `feedback_no_inline_config`). The httpclient panics on empty baseURL.
- **OTLP everywhere** — `otel.Init` is mandatory in every service `main`.
- **Trace context across the bus** — `envelope.Build` auto-fills `traceparent` from ctx; `Validate` rejects envelopes missing it.
- **TDD** — every public function has table-driven tests in `*_test.go`.

## Testing

```sh
go test ./... -coverprofile=cover.out
go tool cover -func=cover.out
```

Coverage gate: 85% (domain). Current: 91.4% total.

## Dependency position

This module is a **leaf**: it depends only on vendor-neutral libraries
(OpenTelemetry, zap, pgx, NATS, AWS S3 SDK) — no Google Cloud SDKs, no
Chora-internal packages. Any service can adopt it without
circular-dependency concerns.

Services depend on this lib via `go.work` (workspace mode) or via published module path once tagged.
