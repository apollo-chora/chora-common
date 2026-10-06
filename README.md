# chora-common

## About

`chora-common` is the shared Go module for cross-cutting infrastructure used by Chora services. It provides packages for configuration loading, OpenTelemetry and W3C trace propagation, structured logging and typed errors, event envelopes and NATS JetStream transport, transactional outbox handling, idempotency, HTTP and gRPC clients, object storage, authentication metadata, and database-related helpers. The module is imported by services that need these primitives rather than run as a standalone application.

## Quick start

Requires Go 1.26.1, as declared in `go.mod`.

Clone the repository:

```sh
git clone https://github.com/apollo-chora/chora-common.git
cd chora-common
```

Download dependencies, build all packages, and run the test suite:

```sh
go mod download
go build ./...
go test ./...
```

The repository's CI also runs `gofmt`, `go mod tidy` consistency checks, `go vet ./...`, and `go test ./...`.

Use the module from another Go project with its module path:

```sh
go get github.com/apollo-chora/chora-common
```

## Usage

The module is organized as independent packages. Import only the packages a service needs.

| Package | Purpose |
| --- | --- |
| `env` | Read required and optional environment variables and populate tagged string configuration structs. |
| `otel` | Initialize the global OpenTelemetry tracer provider and export traces through OTLP/gRPC, with stdout export when no endpoint is configured. |
| `tracing` | W3C `traceparent` propagation, HTTP middleware, request injection, and tenant/GCID context helpers. |
| `observability` | Shared OTel helpers for spans, HTTP and gRPC propagation, event-envelope trace context, AI span attributes, and structured logging. |
| `log` | Structured logging built on Zap, including context-aware fields. |
| `errors` | `chora.Error` with a machine-readable code, human-readable reason, optional trace ID, and wrapped cause. |
| `envelope` | Build and validate the canonical Chora event envelope, including UUIDv7 event IDs, trace context, source metadata, and IMDA labels. |
| `eventbus` | Broker-neutral publisher/handler contracts, an in-memory test bus, and a NATS JetStream implementation with durable consumers, retry, and DLQ handling. |
| `outbox` | Transactional outbox recording plus a relay that claims pending rows, retries publication, supports reorder buffering, and marks exhausted rows as dead-lettered. |
| `idempotent` | Idempotency-key processing and PostgreSQL-backed storage for at-least-once subscribers. |
| `httpclient` | HTTP client with a 10-second default timeout, bounded retries on network errors and 5xx responses, exponential backoff, and trace propagation. |
| `grpcconn` | gRPC dial/server option helpers with keepalive settings. |
| `objectstore` | S3-compatible object storage client for Put, Get, Delete, Exists, and presigned GET operations. |
| `secrets` | Environment-backed secret resolution and retry/backoff helpers. |
| `modelarmor` | Guardrail screener interfaces, local screening, and test stubs. |
| `rls` | PostgreSQL row-level-security session helpers and validation for tenant, GCID, and role identifiers. |
| `durabilityguard` | Boot-time classification of wired adapters as durable, in-memory, or unknown, with report or enforce modes. |
| `auth/chorasession` | Validation of Chora session JWTs. |
| `auth/servicemesh` | Canonical HTTP headers and middleware for Chora GCID, tenant, and role metadata propagation. |
| `agentengine` | Client support for hosted Chora reasoning-engine resources, including authenticated HTTP/SSE interactions. |
| `dek` | Per-user data-encryption-key lifecycle types and encryption helpers. |
| `uiprefs` | Shared UI preference vocabularies such as dashboard wrapper keys. |

### Environment configuration

The `env` package supports required and defaulted string fields:

```go
type Config struct {
    Port  string `env:"CHORA_PORT,default=8080"`
    DBURL string `env:"CHORA_DB_URL,required"`
}

var cfg Config
if err := env.LoadStruct(&cfg); err != nil {
    return err
}
```

`env.MustGet("NAME")` fails fast when a value is missing or empty. `env.GetOrDefault("NAME", "fallback")` returns the environment value when present and otherwise uses the supplied fallback.

### HTTP tracing

Mount the shared middleware on a `net/http` or Chi router:

```go
r := chi.NewRouter()
r.Use(tracing.Middleware())
```

The middleware extracts inbound W3C trace context, starts a server span, attaches tenant and GCID attributes when present, and propagates trace context to downstream handlers and the response. Use `tracing.Inject(req)` on outbound requests.

### Event envelopes

Build an envelope from the request context and service metadata:

```go
env := envelope.Build(ctx, envelope.BuildOpts{
    EventType:     "atom_published",
    SchemaVersion: 1,
    SourceProject: "chora-content",
    SourceService: "chora-creation",
})
if err := envelope.Validate(env); err != nil {
    return err
}
```

`envelope.ValidateStrict` additionally enforces the canonical IMDA dimension vocabulary. The builder derives tenant and trace information from the context and generates a UUIDv7 event ID.

### NATS JetStream event bus

Use the JetStream implementation when the service has a provisioned `CHORA_EVENTS` stream:

```go
bus, err := eventbus.NewJetStream(eventbus.JetStreamConfig{
    URL: "nats://nats:4222",
})
if err != nil {
    return err
}
defer bus.Close()
```

The default stream name is `CHORA_EVENTS`. `Publish` validates the subject and envelope and places the envelope metadata in NATS headers. Consumers use durable JetStream consumers; successful handlers are acknowledged, failed handlers are retried up to the configured delivery limit, and exhausted messages are sent to the DLQ.

For unit tests and local in-process wiring:

```go
bus := eventbus.NewInMemoryBus(
    eventbus.WithSynchronousDelivery(),
)
```

### Transactional outbox

Record a domain event through `outbox.Outbox` inside the same database transaction as the domain write:

```go
o := outbox.NewWithRecorder(recorder)

err := o.Publish(ctx, tx, outbox.PublishOpts{
    AggregateType: "atom",
    AggregateID:   atomID,
    EventType:     "atom.created.v1",
    Topic:         "chora.creation.atom.created.v1",
    Payload:       payload,
    Envelope:      env,
})
if err != nil {
    return err
}
```

The associated `Relay` claims pending rows and publishes them through the configured `Publisher`. The default relay configuration uses batches of 100, a 100 ms poll interval, a maximum of five retries, exponential backoff, and an optional reorder window.

### HTTP service clients

Create an `httpclient.Client` with a base URL sourced from service configuration:

```go
client, err := httpclient.New(baseURL)
if err != nil {
    return err
}

resp, err := client.Get(ctx, "/health")
if err != nil {
    return err
}
defer resp.Body.Close()
```

The client defaults to a 10-second timeout and two retries. It retries network failures and HTTP 5xx responses, but not 4xx responses or context cancellation.

### S3-compatible object storage

Configure the object store from environment variables:

```go
store, err := objectstore.New(objectstore.ConfigFromEnv())
if err != nil {
    return err
}
defer store.Close()
```

`ConfigFromEnv` reads `S3_ENDPOINT`, `S3_ACCESS_KEY_ID` or the legacy `S3_ACCESS_KEY`, `S3_SECRET_ACCESS_KEY` or `S3_SECRET_KEY`, `S3_REGION`, `S3_BUCKET`, and `S3_FORCE_PATH_STYLE`. The default signing region is `us-east-1`, path-style addressing defaults to enabled, and non-streaming operations use a 30-second timeout by default.

### Service-mesh identity propagation

Convert identity claims to the canonical mesh headers:

```go
headers := servicemesh.MarshalToHeaders(servicemesh.MeshClaims{
    GCID:     gcid,
    TenantID: tenantID,
    Roles:    roles,
})
```

On the receiving side, use `servicemesh.Middleware` to attach claims to the request context, or `servicemesh.RequireClaims` to reject requests that lack both GCID and tenant ID.

## Development

Run the repository checks locally:

```sh
gofmt -l .
go mod tidy
go vet ./...
go test ./...
```

For coverage:

```sh
go test ./... -coverprofile=cover.out
go tool cover -func=cover.out
```

The package tree is organized by capability:

```text
env/                    Environment configuration helpers
otel/                   OpenTelemetry SDK setup
tracing/                W3C trace-context propagation and HTTP middleware
observability/          Shared tracing, logging, HTTP, gRPC, and event helpers
log/                    Structured logging
errors/                 Typed Chora errors
envelope/               Canonical event envelope
eventbus/               In-memory and NATS JetStream event bus
outbox/                 Transactional outbox and relay
idempotent/             Idempotency processing and PostgreSQL store
httpclient/             Retrying outbound HTTP client
grpcconn/               gRPC connection and keepalive helpers
objectstore/            S3-compatible object storage
secrets/                Environment-backed secret helpers
modelarmor/             Guardrail ports and local/test implementations
rls/                    PostgreSQL row-level-security helpers
durabilityguard/        Runtime durability checks
auth/                   Chora session and service-mesh authentication helpers
agentengine/            Hosted reasoning-engine client
dek/                    Data-encryption-key lifecycle
uiprefs/                Shared UI preference contracts
tests/                  Cross-package integration tests
```

The repository contains no service entrypoint. Build outputs are produced from the packages themselves, and the CI workflow runs formatting, module-consistency, vet, and test checks on every push and pull request targeting `main`.
