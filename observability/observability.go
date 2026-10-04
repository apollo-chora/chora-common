// Package observability is the shared OTLP-everywhere helper library used by
// every Chora Go service. Wraps the OpenTelemetry SDK with:
//
//  1. InitOTLP — bootstrap helper that initialises the OTLP/gRPC exporter
//     pointing at Cloud Trace via OTEL_EXPORTER_OTLP_ENDPOINT (env-only,
//     per CLAUDE.md no-inline-config rule). Returns a shutdown closure
//     that services MUST defer in main(). Stdout fallback in dev.
//
//  2. StartSpan — wraps tracer.Start() with the chora.* + gen_ai.* attribute
//     namespace conventions (OpenInference / OpenLLMetry).
//
//  3. SetAISpanAttributes — applies the full OpenInference attribute set
//     (gen_ai.system, gen_ai.request.model, gen_ai.usage.prompt_tokens, etc.)
//     plus the chora.* extensions for tenant + agent + IMDA dimension.
//
//  4. HTTPMiddleware — net/http middleware that extracts the W3C
//     traceparent header, mints one when absent, and propagates it to
//     downstream handlers + the response header.
//
//  5. PubsubInjectEnvelope / PubsubExtractEnvelope — write/read W3C
//     trace context into the EventEnvelope (per .claude/skills/event-driven).
//
//  6. GRPCClientInterceptor / GRPCServerInterceptor — propagate trace
//     context across the Python orchestrator ↔ Go executor boundary.
//
//  7. NewSlogLogger — JSON slog handler that adds traceparent + the
//     Cloud Logging logging.googleapis.com/trace correlation field, so
//     log lines auto-link to Cloud Trace.
//
// Design choices:
//
//   - Direct OTLP to Cloud Trace; NO OTel Collector unless trip-wired
//     (per Tier 3 D12 + ai-observability-cloud-trace skill).
//   - Endpoint is env-only — local dev falls back to stdouttrace.
//   - Insecure transport at the wire level; mTLS provided by Cloud
//     Service Mesh between services in production.
//
// Aligned with: Architecture Review locked 2026-05-07 Tier 3 D12,
// Post-Review Addendum #2.
package observability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	gcemetadata "cloud.google.com/go/compute/metadata"
	cloudtrace "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/5007-Capstone/chora/libs/chora-go-common/bootstrap"
	"github.com/5007-Capstone/chora/libs/chora-go-common/tracing"
)

// -----------------------------------------------------------------------------
// OpenInference / OpenLLMetry semantic-conventions attribute names.
//
// Per .claude/skills/ai-observability-cloud-trace/SKILL.md "Mandatory AI span
// attributes". Standardised here so call sites never typo a literal.
// -----------------------------------------------------------------------------

const (
	// gen_ai.* — OpenInference / OpenLLMetry (vendor-neutral GenAI conventions).
	AttrGenAISystem           = "gen_ai.system"
	AttrGenAIRequestModel     = "gen_ai.request.model"
	AttrGenAIResponseModel    = "gen_ai.response.model"
	AttrGenAIRequestTemp      = "gen_ai.request.temperature"
	AttrGenAIRequestMaxTokens = "gen_ai.request.max_tokens"
	AttrGenAIPromptTokens     = "gen_ai.usage.prompt_tokens"
	AttrGenAICompletionTokens = "gen_ai.usage.completion_tokens"
	AttrGenAICachedTokens     = "gen_ai.usage.cached_tokens"
	AttrGenAIPrompt           = "gen_ai.prompt"
	AttrGenAICompletion       = "gen_ai.completion"
	AttrGenAIToolCalls        = "gen_ai.tool.calls"

	// chora.* — Chora-specific extensions (tenant + agent + IMDA).
	AttrChoraAgentID          = "chora.agent.id"
	AttrChoraAgentRunID       = "chora.agent.run_id"
	AttrChoraTenantID         = "chora.tenant.id"
	AttrChoraGCID             = "chora.gcid"
	AttrChoraAdapterVersion   = "chora.adapter.version"
	AttrChoraGuardrailOutcome = "chora.guardrail.outcome"
	AttrChoraIsEvalRun        = "chora.is_eval_run"
	AttrChoraImdaDimension    = "chora.imda.dimension"
	AttrChoraImdaLifecycle    = "chora.imda.lifecycle_stage"
)

// -----------------------------------------------------------------------------
// InitOTLP — bootstrap helper used by every service in main()
// -----------------------------------------------------------------------------

// initState protects the global TracerProvider against double-init within
// the same process. Idempotent re-call is safe in tests.
type initState struct {
	mu       sync.Mutex
	provider *sdktrace.TracerProvider
}

var globalInit = &initState{}

// exporterFactory builds a SpanExporter for the given endpoint hint.
// Swappable in tests via SwapExporterFactoryForTest.
//
// 2026-05-14 (Wave B / tracker #146): default factory now uses the Cloud
// Trace exporter from GoogleCloudPlatform/opentelemetry-operations-go.
// Previously this lib called otlptracegrpc.New(..., WithInsecure()) which
// silently failed TLS against telemetry.googleapis.com:443. cloudtrace.New
// handles TLS + ADC bearer-token auth natively, and the endpoint env var
// remains honoured as a log-only hint for operator debugging.
type exporterFactoryFn func(ctx context.Context, endpoint, serviceName, version string) (sdktrace.SpanExporter, error)

var exporterFactory exporterFactoryFn = defaultExporterFactory

// InitOTLP wires the trace exporter and registers a global TracerProvider.
// Returns a shutdown func the caller MUST defer in main(). When in
// dev (no GOOGLE_CLOUD_PROJECT / ADC / endpoint configured), falls back to
// stdouttrace.
//
// serviceName MUST be non-empty. version is the build SHA / semver tag.
//
// Public API stable per Wave B: existing callers `observability.InitOTLP(name, version)`
// keep working with no edits required.
func InitOTLP(serviceName, version string) (func() error, error) {
	if serviceName == "" {
		return nil, errors.New("observability: serviceName must be non-empty")
	}

	globalInit.mu.Lock()
	defer globalInit.mu.Unlock()

	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exp, err := exporterFactory(ctx, endpoint, serviceName, version)
	if err != nil {
		return nil, err
	}

	res, err := buildResource(ctx, serviceName, version)
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(parseSampler()),
		sdktrace.WithResource(res),
		// Wrap the exporter so its OWN BatchWriteSpans / Export RPC spans are
		// filtered out at export time (where the otelgrpc-assigned span name is
		// final) rather than re-exported in a self-referential flood. A
		// name-matching sampler cannot do this: otelgrpc sets the gRPC client
		// span name AFTER sdktrace.Sampler.ShouldSample runs, so the predicate
		// never matched at sample time (307 BatchWriteSpans traces observed in
		// chora-489812 on 2026-05-29 despite the sampler being installed).
		sdktrace.WithBatcher(newFilteringExporter(exp), sdktrace.WithBatchTimeout(5*time.Second)),
	)
	otel.SetTracerProvider(tp)
	globalInit.provider = tp

	// Global W3C TraceContext + Baggage propagator. Without this, HTTPMiddleware
	// + every other prop.Extract(...) call sees a no-op propagator and starts
	// fresh root spans instead of continuing inbound traceparent. Closes the
	// "spans emit but parent=- everywhere" gap surfaced 2026-05-17 via
	// FE-coord E2E-BE-AI-ASSIST-TRACE-EXPORT-PERM.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func() error {
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		if err := tp.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("trace shutdown: %w", err)
		}
		return nil
	}, nil
}

// InitOTLPAsync is the fail-soft non-blocking variant of InitOTLP per
// C(a).S1 path (b) — tracker #151. It kicks off the synchronous
// InitOTLP path in its own goroutine with a configurable deadline
// (CHORA_OTLP_INIT_TIMEOUT_SECONDS, default 15s) and returns a handle
// the caller can Wait on. Timeout / init-error degrade to a no-op
// shutdown so the rest of bootstrap (pgx pool, Pub/Sub clients) gets
// the FULL bootstrap deadline.
//
// Usage in main():
//
//	handle := observability.InitOTLPAsync(ctx, serviceName, serviceVersion)
//	// kick off OTLP init; don't block yet
//	... boot pgx pool with full deadline ...
//	res := handle.Wait(0) // block to completion before serving traffic
//	defer res.Shutdown(context.Background())
//
// Backward-compatible: the synchronous InitOTLP function is unchanged.
// Services migrate at their own cadence.
func InitOTLPAsync(ctx context.Context, serviceName, version string) *bootstrap.OTLPHandle {
	return bootstrap.StartOTLPAsync(ctx, bootstrap.OTLPOptions{
		InitFunc: func(initCtx context.Context) (func(context.Context) error, error) {
			// Delegate to the synchronous InitOTLP — preserves the
			// global TracerProvider registration + shutdown semantics
			// that callers already rely on. The legacy shutdown sig
			// (func() error) is adapted to the bootstrap helper's
			// ctx-aware shape so callers can pass a deadline on
			// graceful shutdown.
			done := make(chan struct {
				shutdown func() error
				err      error
			}, 1)
			go func() {
				shutdown, err := InitOTLP(serviceName, version)
				done <- struct {
					shutdown func() error
					err      error
				}{shutdown, err}
			}()
			select {
			case <-initCtx.Done():
				return nil, initCtx.Err()
			case r := <-done:
				if r.err != nil {
					return nil, r.err
				}
				return func(context.Context) error { return r.shutdown() }, nil
			}
		},
	})
}

// defaultExporterFactory mirrors libs/chora-go-common/otel — Cloud Trace
// exporter in production, stdouttrace in dev. The endpoint hint is logged
// but not used (cloudtrace exporter has a fixed endpoint internally;
// project discovery happens via ADC).
func defaultExporterFactory(ctx context.Context, endpoint, serviceName, version string) (sdktrace.SpanExporter, error) {
	metadataProjectID := func() (string, bool) { return gceMetadataProjectID(ctx) }
	if isDevExport(os.Getenv, metadataProjectID) {
		log.Printf("observability: dev mode; spans -> stdout (service=%s version=%s)",
			serviceName, version)
		exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("stdouttrace: %w", err)
		}
		return exp, nil
	}

	projectID := resolveProjectID(os.Getenv, metadataProjectID)
	log.Printf("observability: cloudtrace exporter (project=%s endpoint_hint=%s) service=%s version=%s",
		projectIDForLog(projectID), endpoint, serviceName, version)

	opts := []cloudtrace.Option{cloudtrace.WithContext(ctx)}
	if projectID != "" {
		opts = append(opts, cloudtrace.WithProjectID(projectID))
	}
	exp, err := cloudtrace.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("cloudtrace: %w", err)
	}
	return exp, nil
}

// resolveProjectID picks the GCP project id the cloudtrace exporter should
// use, in precedence order:
//
//  1. GOOGLE_CLOUD_PROJECT (existing behavior — unchanged, still wins).
//  2. GCP_PROJECT — every Chora service manifest sets this under Workload
//     Identity (chora-a2a-gateway confirmed 2026-07-01), while
//     GOOGLE_CLOUD_PROJECT is frequently absent.
//  3. The GCE/GKE metadata server project id, via metadataProjectID.
//  4. "" — deliberately never a fabricated value. cloudtrace.New() falls
//     back to its own ADC lookup (google.FindDefaultCredentials) when
//     projectID is empty; if THAT also can't find a project it fails loudly
//     ("stackdriver: no project found with application default
//     credentials") and InitOTLP returns that error. resolveProjectID must
//     never swallow that failure signal by guessing a project id.
//
// getenv and metadataProjectID are injected so the precedence is unit-
// testable without a real metadata server. metadataProjectID may be nil
// (treated as "no metadata source available").
func resolveProjectID(getenv func(string) string, metadataProjectID func() (string, bool)) string {
	if v := strings.TrimSpace(getenv("GOOGLE_CLOUD_PROJECT")); v != "" {
		return v
	}
	if v := strings.TrimSpace(getenv("GCP_PROJECT")); v != "" {
		return v
	}
	if metadataProjectID != nil {
		if v, ok := metadataProjectID(); ok {
			if v = strings.TrimSpace(v); v != "" {
				return v
			}
		}
	}
	return ""
}

// gceMetadataProjectID resolves the project id from the GCE/GKE metadata
// server — the mechanism Workload Identity relies on. Returns ("", false)
// when not running on GCE, or on any lookup error/empty result; either way
// resolveProjectID treats that as "no metadata source, keep falling
// through" rather than an error.
func gceMetadataProjectID(ctx context.Context) (string, bool) {
	if !gcemetadata.OnGCEWithContext(ctx) {
		return "", false
	}
	id, err := gcemetadata.ProjectIDWithContext(ctx)
	if err != nil || strings.TrimSpace(id) == "" {
		return "", false
	}
	return id, true
}

// isDevExport reports whether defaultExporterFactory should fall back to
// stdout-dev-mode export instead of the cloudtrace path.
//
// Bug context (2026-07-01, same incident as resolveProjectID above):
// isDevExport used to gate its project-presence leg on GOOGLE_CLOUD_PROJECT
// alone, out of sync with resolveProjectID's GOOGLE_CLOUD_PROJECT -> GCP_PROJECT
// -> GCE/GKE metadata precedence. A service that resolves a project only via
// GCP_PROJECT or metadata (and sets no OTEL_EXPORTER_OTLP_ENDPOINT) would
// satisfy isDevExport's old AND-condition and silently degrade to stdout
// export — never reaching the cloudtrace path resolveProjectID resolves a
// project for. isDevExport now delegates project-presence to resolveProjectID
// so the two checks can't drift out of precedence sync again.
//
// getenv and metadataProjectID are injected exactly like resolveProjectID so
// this stays unit-testable without a real metadata server; metadataProjectID
// may be nil.
func isDevExport(getenv func(string) string, metadataProjectID func() (string, bool)) bool {
	if strings.EqualFold(strings.TrimSpace(getenv("OTEL_EXPORTER")), "stdout") {
		return true
	}
	if getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" &&
		resolveProjectID(getenv, metadataProjectID) == "" &&
		getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		return true
	}
	return false
}

func projectIDForLog(p string) string {
	if p == "" {
		return "<adc-default>"
	}
	return p
}

func buildResource(ctx context.Context, serviceName, version string) (*resource.Resource, error) {
	env := strings.TrimSpace(os.Getenv("DEPLOYMENT_ENVIRONMENT"))
	if env == "" {
		env = "dev"
	}
	attrs := []attribute.KeyValue{
		attribute.String("service.name", serviceName),
		attribute.String("service.version", version),
		attribute.String("service.namespace", "chora"),
		attribute.String("deployment.environment", env),
	}
	if region := strings.TrimSpace(os.Getenv("CLOUD_REGION")); region != "" {
		attrs = append(attrs, attribute.String("cloud.region", region))
	}
	return resource.New(ctx,
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
	)
}

// newFilteringExporter wraps the real exporter but DROPS spans for the trace-
// exporter's OWN outbound RPCs (Cloud Trace BatchWriteSpans / OTLP collector
// Export) before delegating the rest. The cloudtrace exporter's underlying
// Google Cloud Go client (and any OTLP collector client) auto-creates a client
// span for each export RPC once a global TracerProvider is registered; those
// spans are themselves exported, producing a self-referential flood (307
// BatchWriteSpans traces observed in chora-489812 on 2026-05-29) that buries
// real per-service traces.
//
// Why filter at the EXPORTER, not the sampler: otelgrpc assigns the gRPC client
// span name AFTER sdktrace.Sampler.ShouldSample has already run, so a name-
// matching sampler never observes "BatchWriteSpans" / "Export" and never fires.
// By the time ExportSpans is invoked the span name is final, so the predicate
// matches reliably. This stops the export loop at the source without disabling
// any application tracing.
func newFilteringExporter(delegate sdktrace.SpanExporter) sdktrace.SpanExporter {
	return &filteringExporter{delegate: delegate}
}

type filteringExporter struct{ delegate sdktrace.SpanExporter }

func (e *filteringExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	filtered := spans[:0:0]
	for _, s := range spans {
		if s != nil && isTraceExportSpan(s.Name()) {
			continue
		}
		filtered = append(filtered, s)
	}
	if len(filtered) == 0 {
		return nil
	}
	return e.delegate.ExportSpans(ctx, filtered)
}

func (e *filteringExporter) Shutdown(ctx context.Context) error {
	return e.delegate.Shutdown(ctx)
}

// isTraceExportSpan matches the span names the trace export path emits for its
// own RPCs: the Cloud Trace v2 BatchWriteSpans call and the OTLP collector
// Export call.
func isTraceExportSpan(name string) bool {
	return strings.Contains(name, "cloudtrace.v2.TraceService/BatchWriteSpans") ||
		strings.Contains(name, "TraceService/Export")
}

func parseSampler() sdktrace.Sampler {
	switch strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER")) {
	case "parentbased_traceidratio":
		ratio := 1.0
		if v := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER_ARG")); v != "" {
			if f, err := parseRatio(v); err == nil {
				ratio = f
			}
		}
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	case "always_off":
		return sdktrace.NeverSample()
	default:
		return sdktrace.AlwaysSample()
	}
}

func parseRatio(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	if err != nil {
		return 0, err
	}
	if f < 0 || f > 1 {
		return 0, fmt.Errorf("ratio out of range: %v", f)
	}
	return f, nil
}

// SwapExporterFactoryForTest replaces the package-level exporter factory and
// returns a restore func. Test-only — never call from production code paths.
func SwapExporterFactoryForTest(fn func(ctx context.Context, endpoint, serviceName, version string) (sdktrace.SpanExporter, error)) func() {
	prev := exporterFactory
	exporterFactory = exporterFactoryFn(fn)
	return func() { exporterFactory = prev }
}

// -----------------------------------------------------------------------------
// StartSpan — convenience wrapper around tracer.Start with Chora defaults.
// -----------------------------------------------------------------------------

// StartSpan returns a context + span scoped to the global tracer. Equivalent
// to otel.Tracer("chora").Start(ctx, name, opts...) but pulled into one helper
// so call sites never accidentally read the TracerProvider before InitOTLP
// runs in main().
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer("chora").Start(ctx, name, opts...)
}

// -----------------------------------------------------------------------------
// AIAttrs / SetAISpanAttributes — OpenInference / OpenLLMetry shape.
// -----------------------------------------------------------------------------

// AIAttrs is the call-site shape for OpenInference + chora.* attributes on an
// AI span. Zero-valued fields are skipped (i.e., only set non-empty values).
type AIAttrs struct {
	System             string // e.g. "vertex_ai_gemini" / "self_hosted_gemma" / "byoa_openai"
	RequestModel       string
	ResponseModel      string
	RequestTemperature float64
	RequestMaxTokens   int64
	PromptTokens       int64
	CompletionTokens   int64
	CachedTokens       int64
	Prompt             string // redacted body or hash; never raw PII
	Completion         string // redacted body or hash
	ToolCalls          string // tool call array (function name + arg keys)

	AgentID          string
	AgentRunID       string
	AgentRole        string // optional (atom_validator / classifier / familiar_persona)
	TenantID         string
	GCID             string
	AdapterVersion   string // per gemma-lora-tenant; "none" if base-only
	GuardrailOutcome string // "pass" / "block" / "redact"
	IsEvalRun        bool   // per Tier 3 D13
	ImdaDimension    string // per ADR-141 canonical labels
	LifecycleStage   string // ci_pre_merge / pre_deploy / runtime / post_deploy
}

// SetAISpanAttributes applies the full OpenInference attribute set to the
// span. Safe on a nil span (no-op).
func SetAISpanAttributes(span trace.Span, a AIAttrs) {
	if span == nil {
		return
	}
	if a.System != "" {
		span.SetAttributes(attribute.String(AttrGenAISystem, a.System))
	}
	if a.RequestModel != "" {
		span.SetAttributes(attribute.String(AttrGenAIRequestModel, a.RequestModel))
	}
	if a.ResponseModel != "" {
		span.SetAttributes(attribute.String(AttrGenAIResponseModel, a.ResponseModel))
	}
	if a.RequestTemperature != 0 {
		span.SetAttributes(attribute.Float64(AttrGenAIRequestTemp, a.RequestTemperature))
	}
	if a.RequestMaxTokens != 0 {
		span.SetAttributes(attribute.Int64(AttrGenAIRequestMaxTokens, a.RequestMaxTokens))
	}
	if a.PromptTokens != 0 {
		span.SetAttributes(attribute.Int64(AttrGenAIPromptTokens, a.PromptTokens))
	}
	if a.CompletionTokens != 0 {
		span.SetAttributes(attribute.Int64(AttrGenAICompletionTokens, a.CompletionTokens))
	}
	if a.CachedTokens != 0 {
		span.SetAttributes(attribute.Int64(AttrGenAICachedTokens, a.CachedTokens))
	}
	if a.Prompt != "" {
		span.SetAttributes(attribute.String(AttrGenAIPrompt, a.Prompt))
	}
	if a.Completion != "" {
		span.SetAttributes(attribute.String(AttrGenAICompletion, a.Completion))
	}
	if a.ToolCalls != "" {
		span.SetAttributes(attribute.String(AttrGenAIToolCalls, a.ToolCalls))
	}
	if a.AgentID != "" {
		span.SetAttributes(attribute.String(AttrChoraAgentID, a.AgentID))
	}
	if a.AgentRunID != "" {
		span.SetAttributes(attribute.String(AttrChoraAgentRunID, a.AgentRunID))
	}
	if a.AgentRole != "" {
		span.SetAttributes(attribute.String("chora.agent.role", a.AgentRole))
	}
	if a.TenantID != "" {
		span.SetAttributes(attribute.String(AttrChoraTenantID, a.TenantID))
	}
	if a.GCID != "" {
		span.SetAttributes(attribute.String(AttrChoraGCID, a.GCID))
	}
	if a.AdapterVersion != "" {
		span.SetAttributes(attribute.String(AttrChoraAdapterVersion, a.AdapterVersion))
	}
	if a.GuardrailOutcome != "" {
		span.SetAttributes(attribute.String(AttrChoraGuardrailOutcome, a.GuardrailOutcome))
	}
	if a.IsEvalRun {
		span.SetAttributes(attribute.Bool(AttrChoraIsEvalRun, true))
	}
	if a.ImdaDimension != "" {
		span.SetAttributes(attribute.String(AttrChoraImdaDimension, a.ImdaDimension))
	}
	if a.LifecycleStage != "" {
		span.SetAttributes(attribute.String(AttrChoraImdaLifecycle, a.LifecycleStage))
	}
}

// -----------------------------------------------------------------------------
// HTTP middleware — alias of tracing.Middleware for convenience.
// -----------------------------------------------------------------------------

// HTTPMiddleware re-exports the tracing.Middleware so services importing only
// chora-go-common/observability get the full set in one package.
func HTTPMiddleware() func(next http.Handler) http.Handler {
	return tracing.Middleware()
}

// -----------------------------------------------------------------------------
// Pub/Sub envelope hooks — write/read W3C trace context.
// -----------------------------------------------------------------------------

// PubsubInjectEnvelope writes the W3C traceparent + tracestate into the
// envelope-like map provided by the caller. Use at the publisher: the
// EventEnvelope has dedicated traceparent + tracestate fields per Tier 2 D8.
func PubsubInjectEnvelope(traceparent, tracestate string, envelope map[string]string) {
	if envelope == nil {
		return
	}
	if traceparent != "" {
		envelope["traceparent"] = traceparent
	}
	if tracestate != "" {
		envelope["tracestate"] = tracestate
	}
}

// PubsubExtractEnvelope reads traceparent + tracestate from the envelope.
// Returns ("", "") if envelope is nil/missing the keys.
func PubsubExtractEnvelope(envelope map[string]string) (traceparent, tracestate string) {
	if envelope == nil {
		return "", ""
	}
	return envelope["traceparent"], envelope["tracestate"]
}

// -----------------------------------------------------------------------------
// gRPC client + server interceptors — propagate trace context.
// -----------------------------------------------------------------------------

// GRPCClientInterceptor returns a unary client interceptor that injects the
// current traceparent into the gRPC metadata so the server can pick it up.
//
// Per .claude/skills/ai-observability-cloud-trace/SKILL.md, gRPC metadata
// propagator is auto-configured by OTel SDK; this interceptor adds Chora's
// W3C-compatible traceparent header for non-OTel-aware peers (e.g. Python
// orchestrator → Go executor boundary in early stages).
func GRPCClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		tp := tracing.TraceparentFromContext(ctx)
		if tp == "" {
			tp = tracing.EnsureTraceparent("")
		}
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.MD{}
		}
		md.Set("traceparent", tp)
		ctx = metadata.NewOutgoingContext(ctx, md)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// GRPCServerInterceptor returns a unary server interceptor that extracts
// the inbound traceparent from gRPC metadata and stamps it on the request
// context, so handlers can read it via tracing.TraceparentFromContext.
func GRPCServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req any, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			tps := md.Get("traceparent")
			if len(tps) > 0 && tps[0] != "" {
				ctx = tracing.WithTraceparent(ctx, tps[0])
			}
		}
		return handler(ctx, req)
	}
}

// -----------------------------------------------------------------------------
// slog handler — JSON output + Cloud Logging trace correlation.
// -----------------------------------------------------------------------------

// WithTraceparent stamps the traceparent on the context. Re-exported alias of
// tracing.WithTraceparent for ergonomic call sites that import only this pkg.
func WithTraceparent(ctx context.Context, tp string) context.Context {
	return tracing.WithTraceparent(ctx, tp)
}

// TraceparentFromContext returns the traceparent stored on ctx, or "".
// Re-exported alias of tracing.TraceparentFromContext for the same reason.
func TraceparentFromContext(ctx context.Context) string {
	return tracing.TraceparentFromContext(ctx)
}

// chora-trace-handler decorates a JSON slog handler with traceparent +
// logging.googleapis.com/trace from the active context.
type traceHandler struct {
	inner slog.Handler
}

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	tp := tracing.TraceparentFromContext(ctx)
	if tp != "" {
		r.AddAttrs(slog.String("traceparent", tp))
		// Cloud Logging convention: logging.googleapis.com/trace links the
		// log line to the corresponding Cloud Trace span. Format:
		// projects/{project_id}/traces/{trace_id}.
		// We populate the trace_id portion from the W3C traceparent
		// (00-{trace_id-32hex}-{span_id-16hex}-{flags}).
		traceID := extractTraceIDFromW3C(tp)
		if traceID != "" {
			project := strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_PROJECT"))
			if project == "" {
				project = "chora-489812"
			}
			r.AddAttrs(slog.String("logging.googleapis.com/trace",
				fmt.Sprintf("projects/%s/traces/%s", project, traceID)))
		}
	}
	if tenant := tracing.TenantIDFromContext(ctx); tenant != "" {
		r.AddAttrs(slog.String("tenant_id", tenant))
	}
	if gcid := tracing.GCIDFromContext(ctx); gcid != "" {
		r.AddAttrs(slog.String("gcid", gcid))
	}
	return h.inner.Handle(ctx, r)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{inner: h.inner.WithGroup(name)}
}

// NewSlogLogger returns a JSON slog logger whose handler enriches every entry
// with the active traceparent + Cloud Trace correlation field. Pass io.Writer
// (typically os.Stdout for Cloud Run) or a buffer in tests.
func NewSlogLogger(w io.Writer) *slog.Logger {
	jsonHandler := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(&traceHandler{inner: jsonHandler})
}

// extractTraceIDFromW3C parses the W3C traceparent and returns the 32-hex
// trace_id. Empty string on malformed input.
func extractTraceIDFromW3C(tp string) string {
	parts := strings.Split(tp, "-")
	if len(parts) != 4 {
		return ""
	}
	if len(parts[1]) != 32 {
		return ""
	}
	return parts[1]
}
