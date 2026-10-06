# chora-common

## About

`chora-common` is the shared Go module for cross-cutting infrastructure used by Chora services. It provides reusable packages for configuration, tracing and observability, event envelopes and NATS JetStream, transactional outboxes, idempotency, HTTP and gRPC clients, object storage, authentication metadata, encryption-related helpers, and database utilities. It is a library module, not a standalone service.

## Quick start

Requires Go 1.26.1, as declared in `go.mod`.

Clone the repository:

```sh
git clone https://github.com/apollo-chora/chora-common.git
cd chora-common
```

Download dependencies and verify the module:

```sh
go mod download
go mod tidy
go vet ./...
go test ./...
```

Use the module from another Go project with its module path:

```sh
go get github.com/apollo-chora/chora-common
```

No executable is provided by this repository. Consumers import the packages they need.

## Usage

The repository is organized as independent Go packages.

| Package | Purpose |
| --- | --- |
| `env` | Environment-backed configuration helpers: `MustGet`, `GetOrDefault`, and tagged `LoadStruct`. |
| `otel` | OpenTelemetry SDK setup with OTLP/gRPC export and stdout export for local development. |
| `tracing` | W3C `traceparent` handling, HTTP middleware, outbound propagation, and tenant/GCID context helpers. |
| `observability` | Shared tracing, HTTP, gRPC, event-envelope, AI-span, and structured-logging helpers. |
| `log` | Structured logging built on Zap. |
| `errors` | `chora.Error` with machine-readable codes, human-readable reasons, trace IDs, and wrapped causes. |
| `envelope` | Canonical Chora event envelope construction, validation, and IMDA label normalization. |
| `eventbus` | Broker-neutral event interfaces, an in-memory bus for tests/local use, and a NATS JetStream implementation. |
| `outbox` | Transactional outbox recording and relay support with retries, reordering, and dead-letter handling. |
| `idempotent` | Idempotency-key processing and PostgreSQL-backed persistence. |
| `httpclient` | Outbound HTTP client with a 10-second default timeout, retries, exponential backoff, and trace propagation. |
| `grpcconn` | gRPC connection, client keepalive, and server option helpers. |
| `objectstore` | S3-compatible object storage client with Put, Get, Delete, Exists, and presigned GET operations. |
| `secrets` | Environment-backed secret lookup and retry/backoff helpers. |
| `modelarmor` | Guardrail screening interfaces, local screening, and test stubs. |
| `rls` | PostgreSQL row-level-security session setup and identifier validation. |
| `durabilityguard` | Runtime classification of adapters as durable, in-memory, or unknown, with report/enforce modes. |
| `auth/chorasession` | Validation of Chora session JWTs, including HS256 signature, issuer, audience, and time checks. |
| `auth/servicemesh` | Canonical Chora GCID, tenant, role, and role-summary headers plus HTTP middleware. |
| `agentengine` | Client support for Chora reasoning-engine resources, including authenticated REST/SSE access. |
| `dek` | Per-user data-encryption-key lifecycle types and cryptographic helpers. |
| `uiprefs` | Shared UI-preference contracts, including dashboard wrapper keys. |
| `db` | Database connection and query helpers used by Chora services. |
| `tests` | Cross-package integration coverage for common event, outbox, and environment flows. |

### Environment configuration

The `env` package reads configuration from process environment variables:

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

`env.MustGet` panics when the requested variable is unset or empty. `env.GetOrDefault` uses the supplied fallback when the variable is unset or empty. `LoadStruct` supports required fields and string defaults through the `env` struct tag.

### HTTP tracing

The tracing middleware works with `net/http` handlers and Chi routers:

```go
r := chi.NewRouter()
r.Use(tracing.Middleware())
```

It extracts inbound W3C trace context, starts an OpenTelemetry server span, attaches tenant and GCID attributes when supplied by the request, and propagates the resulting context to downstream handlers. Use `tracing.Inject(req)` for outbound HTTP requests.

### Event envelopes

Build and validate the common event envelope from request context and service metadata:

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

The builder supplies a UUIDv7 event ID, timestamps, tenant context, and a traceparent. `ValidateStrict` additionally checks that an IMDA dimension is one of the canonical values.

### NATS JetStream

The event bus includes an in-memory implementation for tests and a NATS JetStream implementation:

```go
bus, err := eventbus.NewJetStream(eventbus.JetStreamConfig{
    URL: "nats://nats:4222",
})
if err != nil {
    return err
}
defer bus.Close()
```

The default JetStream stream is `CHORA_EVENTS`. The JetStream consumer implementation creates durable consumers, acknowledges successful handlers, retries failures according to the consumer configuration, and routes exhausted messages to a DLQ.

For in-process tests:

```go
bus := eventbus.NewInMemoryBus(
    eventbus.WithSynchronousDelivery(),
)
```

### Transactional outbox

Record the domain event inside the same database transaction as the domain-state change:

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

The `Relay` drains pending rows through a `Publisher`, retries publish failures, optionally reorders rows by event time, and marks rows dead-lettered after the configured retry ceiling.

### HTTP client

Create the shared HTTP client with a base URL supplied by service configuration:

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

The default timeout is 10 seconds and the default retry limit is two attempts after the initial request. Network failures and HTTP 5xx responses are retryable; 4xx responses are returned to the caller.

### S3-compatible object storage

Configure the object store from environment variables:

```go
store, err := objectstore.New(objectstore.ConfigFromEnv())
if err != nil {
    return err
}
defer store.Close()
```

`ConfigFromEnv` reads `S3_ENDPOINT`, `S3_ACCESS_KEY_ID` or `S3_ACCESS_KEY`, `S3_SECRET_ACCESS_KEY` or `S3_SECRET_KEY`, `S3_REGION`, `S3_BUCKET`, and `S3_FORCE_PATH_STYLE`. The default signing region is `us-east-1`; path-style addressing defaults to enabled.

### Service-mesh identity

Convert Chora identity claims to canonical HTTP headers:

```go
headers := servicemesh.MarshalToHeaders(servicemesh.MeshClaims{
    GCID:     gcid,
    TenantID: tenantID,
    Roles:    roles,
})
```

On inbound requests, `servicemesh.Middleware` parses the headers and stores the claims in request context. `servicemesh.RequireClaims` adds a 401 check for requests that lack both GCID and tenant ID.

### OpenTelemetry

Initialize tracing once during service startup:

```go
shutdown, err := otel.Init(ctx, "chora-creation", "v1.0.0")
if err != nil {
    return err
}
defer shutdown(ctx)
```

When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, traces are exported over OTLP/gRPC. With no endpoint configured, the package uses stdout tracing for local development.

## Development

Format, vet, and test the module from the repository root:

```sh
gofmt -w .
go mod tidy
go vet ./...
go test ./...
```

Run the test suite with coverage reporting:

```sh
go test ./... -coverprofile=cover.out
go tool cover -func=cover.out
```

Cross-package tests live under `tests/`. Package-specific tests sit beside their implementation files as `*_test.go`.

The main source tree is:

```text
ackafterprocessing/     Subscriber ack/nack wrapper
agentengine/            Reasoning-engine client
auth/                   Chora session and service-mesh auth helpers
db/                     Database helpers
dek/                    Data-encryption-key helpers
durabilityguard/        Runtime adapter durability checks
env/                    Environment configuration
envelope/               Event-envelope model and validation
errors/                 Typed Chora errors
eventbus/               In-memory and NATS JetStream event bus
grpcconn/               gRPC connection helpers
httpclient/              Retrying HTTP client
idempotent/              Idempotency store
log/                     Structured logging
modelarmor/              Guardrail ports and local/test implementations
objectstore/             S3-compatible object storage
observability/            Shared tracing and telemetry helpers
otel/                    OpenTelemetry initialization
outbox/                  Transactional outbox and relay
rls/                     PostgreSQL row-level-security helpers
secrets/                 Environment-backed secret helpers
tracing/                 W3C trace-context propagation
uiprefs/                 Shared UI-preference contracts
tests/                   Cross-package integration tests
go.mod                   Go module definition
go.sum                   Dependency checksums
```

The GitHub Actions CI workflow runs formatting checks, `go mod tidy` consistency checks, `go vet ./...`, and `go test ./...` for changes targeting `main`.
