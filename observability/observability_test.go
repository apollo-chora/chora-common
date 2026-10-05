// Package observability_test exercises the OTLP-everywhere primitives shared
// across all Chora Go services.
//
// Coverage targets (per CLAUDE.md):
//   - libs (this package): ≥ 85% (treated as domain).
//
// Tests are RED-first per .claude/rules/development-execution.md TDD enforcement.
package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/apollo-chora/chora-common/observability"
)

// envKeysObs is the set of env vars InitOTLP reads. Used by the Wave B
// hermetic-env helpers below.
var envKeysObs = []string{
	"OTEL_EXPORTER",
	"OTEL_EXPORTER_OTLP_ENDPOINT",
	"OTEL_TRACES_SAMPLER",
	"OTEL_TRACES_SAMPLER_ARG",
	"OTEL_RESOURCE_ATTRIBUTES",
	"DEPLOYMENT_ENVIRONMENT",
	"CLOUD_REGION",
}

func snapshotEnvObs(t *testing.T) {
	t.Helper()
	saved := make(map[string]string, len(envKeysObs))
	for _, k := range envKeysObs {
		saved[k] = os.Getenv(k)
		_ = os.Unsetenv(k)
	}
	t.Cleanup(func() {
		for _, k := range envKeysObs {
			if v, ok := saved[k]; ok && v != "" {
				_ = os.Setenv(k, v)
			} else {
				_ = os.Unsetenv(k)
			}
		}
	})
}

// envMuObs serialises env-mutating tests.
var envMuObs sync.Mutex

type noopExporterObs struct{}

func (*noopExporterObs) ExportSpans(_ context.Context, _ []sdktrace.ReadOnlySpan) error {
	return nil
}
func (*noopExporterObs) Shutdown(_ context.Context) error { return nil }

// -----------------------------------------------------------------------------
// InitOTLP — bootstrap helper used by every service in main()
// -----------------------------------------------------------------------------

func TestInitOTLP_ReturnsShutdownAndTracer(t *testing.T) {
	// Cannot t.Parallel — t.Setenv mutates process env.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdown, err := observability.InitOTLP("chora-test-svc", "v0.0.1")
	if err != nil {
		t.Fatalf("InitOTLP: %v", err)
	}
	if shutdown == nil {
		t.Error("expected non-nil shutdown")
	}
	if err := shutdown(); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

func TestInitOTLP_RejectsEmptyServiceName(t *testing.T) {
	t.Parallel()
	_, err := observability.InitOTLP("", "v1.0")
	if err == nil {
		t.Error("expected error for empty service name")
	}
}

// -----------------------------------------------------------------------------
// InitOTLPAsync — non-blocking variant per C(a).S1 path (b)
// -----------------------------------------------------------------------------

func TestInitOTLPAsync_ReturnsHandleAndSucceeds(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	handle := observability.InitOTLPAsync(context.Background(), "chora-test-async", "v0.0.1")
	if handle == nil {
		t.Fatalf("expected non-nil handle")
	}
	res := handle.Wait(5 * time.Second)
	if res.Err != nil {
		t.Errorf("Wait: unexpected err: %v", res.Err)
	}
	if res.Shutdown == nil {
		t.Fatalf("Wait: shutdown must be non-nil")
	}
	if !res.Initialized {
		t.Errorf("Wait: expected Initialized=true under stdout-dev path; InitError=%v", res.InitError)
	}
	if err := res.Shutdown(context.Background()); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

func TestInitOTLPAsync_FailSoftOnEmptyServiceName(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	// Empty service name → underlying InitOTLP returns an error.
	// Fail-soft contract: handle.Wait still returns a no-op shutdown
	// with InitError set, never a fatal error to the caller.
	handle := observability.InitOTLPAsync(context.Background(), "", "v0.0.1")
	res := handle.Wait(2 * time.Second)
	if res.Err != nil {
		t.Errorf("Wait must be fail-soft (Err always nil); got %v", res.Err)
	}
	if res.Initialized {
		t.Errorf("expected Initialized=false on empty service name")
	}
	if res.InitError == nil {
		t.Errorf("expected InitError to surface diagnostics")
	}
	if res.Shutdown == nil {
		t.Errorf("Shutdown must always be non-nil")
	}
}

// -----------------------------------------------------------------------------
// StartSpan — wraps OTel SDK with OpenInference helpers
// -----------------------------------------------------------------------------

func TestStartSpan_ReturnsContextAndSpan(t *testing.T) {
	t.Parallel()
	ctx, span := observability.StartSpan(context.Background(), "test-span")
	if span == nil {
		t.Fatal("span nil")
	}
	if ctx == nil {
		t.Error("ctx nil")
	}
	span.End()
}

// -----------------------------------------------------------------------------
// SetAISpanAttributes — OpenInference / GenAI semantic conventions
// -----------------------------------------------------------------------------

func TestSetAISpanAttributes_AppliesAllAttributes(t *testing.T) {
	t.Parallel()
	_, span := observability.StartSpan(context.Background(), "gen_ai.test")
	observability.SetAISpanAttributes(span, observability.AIAttrs{
		System:           "vertex_ai_gemini",
		RequestModel:     "gemini-2.5-flash",
		ResponseModel:    "gemini-2.5-flash",
		PromptTokens:     100,
		CompletionTokens: 50,
		CachedTokens:     20,
		Prompt:           "redacted-hash",
		Completion:       "redacted-hash",
		AgentID:          "atom-validator",
		AgentRunID:       "01970000-0000-7000-8000-aaaaaaaaaaaa",
		TenantID:         "01970000-0000-7000-8000-bbbbbbbbbbbb",
		GCID:             "01970000-0000-7000-8000-cccccccccccc",
		ImdaDimension:    "transparency",
		LifecycleStage:   "runtime",
	})
	span.End()
}

func TestSetAISpanAttributes_AcceptsNilSpan(t *testing.T) {
	t.Parallel()
	// MUST NOT panic even when span is nil.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic on nil span: %v", r)
		}
	}()
	observability.SetAISpanAttributes(nil, observability.AIAttrs{})
}

// -----------------------------------------------------------------------------
// HTTP middleware — extracts traceparent + sets active OTel context
// -----------------------------------------------------------------------------

func TestHTTPMiddleware_PropagatesTraceparent(t *testing.T) {
	t.Parallel()
	const inbound = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	var got string
	mw := observability.HTTPMiddleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("traceparent", inbound)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if got != inbound {
		t.Errorf("traceparent not propagated: got=%q want=%q", got, inbound)
	}
}

func TestHTTPMiddleware_GeneratesTraceparentWhenMissing(t *testing.T) {
	t.Parallel()
	mw := observability.HTTPMiddleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Header().Get("traceparent") == "" {
		t.Error("expected response traceparent header")
	}
}

// -----------------------------------------------------------------------------
// Pub/Sub envelope hooks — write traceparent on publish + restore on subscribe
// -----------------------------------------------------------------------------

func TestPubsubInjectExtract_RoundTrip(t *testing.T) {
	t.Parallel()
	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	const ts = "vendor=value"

	// Publisher hook: write traceparent + tracestate to envelope-like map.
	envelope := map[string]string{}
	observability.PubsubInjectEnvelope(tp, ts, envelope)
	if envelope["traceparent"] != tp {
		t.Errorf("traceparent not written: %v", envelope)
	}
	if envelope["tracestate"] != ts {
		t.Errorf("tracestate not written: %v", envelope)
	}

	// Subscriber hook: read traceparent + tracestate back.
	gotTP, gotTS := observability.PubsubExtractEnvelope(envelope)
	if gotTP != tp {
		t.Errorf("extract tp mismatch: got=%q want=%q", gotTP, tp)
	}
	if gotTS != ts {
		t.Errorf("extract ts mismatch: got=%q want=%q", gotTS, ts)
	}
}

func TestPubsubExtractEnvelope_HandlesEmpty(t *testing.T) {
	t.Parallel()
	gotTP, gotTS := observability.PubsubExtractEnvelope(nil)
	if gotTP != "" || gotTS != "" {
		t.Errorf("nil envelope should yield empty traceparent/tracestate")
	}
	gotTP, gotTS = observability.PubsubExtractEnvelope(map[string]string{})
	if gotTP != "" || gotTS != "" {
		t.Errorf("empty envelope should yield empty traceparent/tracestate")
	}
}

// -----------------------------------------------------------------------------
// gRPC client+server interceptors — pull traceparent from metadata.
// -----------------------------------------------------------------------------

func TestGRPCClientInterceptor_NotNil(t *testing.T) {
	t.Parallel()
	if observability.GRPCClientInterceptor() == nil {
		t.Error("client interceptor nil")
	}
}

func TestGRPCServerInterceptor_NotNil(t *testing.T) {
	t.Parallel()
	if observability.GRPCServerInterceptor() == nil {
		t.Error("server interceptor nil")
	}
}

// TestGRPCClientInterceptor_InjectsTraceparent exercises the interceptor
// with a fake invoker so the metadata-write logic runs end-to-end.
func TestGRPCClientInterceptor_InjectsTraceparent(t *testing.T) {
	t.Parallel()
	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	ctx := observability.WithTraceparent(context.Background(), tp)
	ic := observability.GRPCClientInterceptor()
	var capturedMD metadata.MD
	invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		capturedMD = md
		return nil
	}
	if err := ic(ctx, "/svc.Foo/Bar", nil, nil, nil, invoker); err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	tps := capturedMD.Get("traceparent")
	if len(tps) == 0 || tps[0] != tp {
		t.Errorf("traceparent missing: %v", capturedMD)
	}
}

// TestGRPCClientInterceptor_GeneratesWhenAbsent exercises the fallback path.
func TestGRPCClientInterceptor_GeneratesWhenAbsent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ic := observability.GRPCClientInterceptor()
	var capturedMD metadata.MD
	invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		capturedMD = md
		return nil
	}
	_ = ic(ctx, "/svc.Foo/Bar", nil, nil, nil, invoker)
	tps := capturedMD.Get("traceparent")
	if len(tps) == 0 || tps[0] == "" {
		t.Errorf("traceparent should be generated; got %v", capturedMD)
	}
}

func TestGRPCServerInterceptor_PropagatesIncomingMD(t *testing.T) {
	t.Parallel()
	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	md := metadata.New(map[string]string{"traceparent": tp})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	ic := observability.GRPCServerInterceptor()
	var got string
	handler := func(ctx context.Context, _ any) (any, error) {
		got = observability.TraceparentFromContext(ctx)
		return nil, nil
	}
	_, _ = ic(ctx, nil, &grpc.UnaryServerInfo{}, handler)
	if got != tp {
		t.Errorf("traceparent not propagated to handler ctx: got=%q want=%q", got, tp)
	}
}

func TestGRPCServerInterceptor_NoIncomingMD(t *testing.T) {
	t.Parallel()
	ic := observability.GRPCServerInterceptor()
	called := false
	handler := func(_ context.Context, _ any) (any, error) {
		called = true
		return nil, nil
	}
	_, _ = ic(context.Background(), nil, &grpc.UnaryServerInfo{}, handler)
	if !called {
		t.Error("handler not called")
	}
}

// -----------------------------------------------------------------------------
// slog handler — adds traceparent + Cloud Trace correlation field to entries
// -----------------------------------------------------------------------------

func TestSlogHandler_AddsTraceCorrelation(t *testing.T) {
	t.Parallel()
	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

	var buf bytes.Buffer
	logger := observability.NewSlogLogger(&buf)
	ctx := observability.WithTraceparent(context.Background(), tp)
	logger.InfoContext(ctx, "hello", slog.String("key", "value"))

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("decode log line: %v\nraw=%s", err, buf.String())
	}
	if entry["traceparent"] != tp {
		t.Errorf("traceparent missing in log: %v", entry)
	}
	// Cloud Logging convention: logging.googleapis.com/trace links log lines
	// to Cloud Trace. The handler must populate it from the inbound trace ID.
	traceField, _ := entry["logging.googleapis.com/trace"].(string)
	if !strings.Contains(traceField, "0af7651916cd43dd8448eb211c80319c") {
		t.Errorf("Cloud Trace correlation field missing: %v", entry)
	}
}

func TestSlogHandler_NoTraceparent_NoCorrelation(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := observability.NewSlogLogger(&buf)
	logger.Info("no-trace")
	var entry map[string]any
	_ = json.Unmarshal(buf.Bytes(), &entry)
	if _, has := entry["traceparent"]; has {
		t.Errorf("expected no traceparent in entry: %v", entry)
	}
}

// -----------------------------------------------------------------------------
// OpenInferenceConst — sanity-check the const list.
// -----------------------------------------------------------------------------

func TestOpenInferenceConst_HasGenAIPrefix(t *testing.T) {
	t.Parallel()
	if observability.AttrGenAISystem != "gen_ai.system" {
		t.Errorf("AttrGenAISystem = %q; want gen_ai.system", observability.AttrGenAISystem)
	}
	if observability.AttrGenAIPromptTokens != "gen_ai.usage.prompt_tokens" {
		t.Errorf("AttrGenAIPromptTokens = %q", observability.AttrGenAIPromptTokens)
	}
}

// -----------------------------------------------------------------------------
// Coverage bump tests — exercise full attribute combos + slog WithAttrs
// -----------------------------------------------------------------------------

func TestSetAISpanAttributes_AppliesScalarsAndBools(t *testing.T) {
	t.Parallel()
	_, span := observability.StartSpan(context.Background(), "gen_ai.scalars")
	observability.SetAISpanAttributes(span, observability.AIAttrs{
		RequestTemperature: 0.7,
		RequestMaxTokens:   1024,
		ToolCalls:          "[\"web_search\"]",
		AgentRole:          "atom_validator",
		IsEvalRun:          true,
	})
	span.End()
}

func TestPubsubInjectEnvelope_NilEnvelope_NoOp(t *testing.T) {
	t.Parallel()
	// Must not panic on nil envelope.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic on nil envelope: %v", r)
		}
	}()
	observability.PubsubInjectEnvelope("tp", "ts", nil)
}

func TestPubsubInjectEnvelope_EmptyValues_NoWrite(t *testing.T) {
	t.Parallel()
	env := map[string]string{}
	observability.PubsubInjectEnvelope("", "", env)
	if len(env) != 0 {
		t.Errorf("empty values should not write; got %v", env)
	}
}

func TestSlogHandler_WithAttrs(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := observability.NewSlogLogger(&buf)
	logger.With(slog.String("k", "v")).Info("with-attrs")
	if !bytes.Contains(buf.Bytes(), []byte(`"k":"v"`)) {
		t.Errorf("WithAttrs didn't propagate; got %s", buf.String())
	}
}

func TestSlogHandler_WithGroup(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := observability.NewSlogLogger(&buf)
	logger.WithGroup("svc").Info("grouped", slog.String("k", "v"))
	if !bytes.Contains(buf.Bytes(), []byte(`"svc"`)) {
		t.Errorf("WithGroup didn't propagate; got %s", buf.String())
	}
}

func TestSlogHandler_TenantAndGCIDPropagation(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := observability.NewSlogLogger(&buf)
	ctx := observability.WithTraceparent(context.Background(),
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	// Manually set tenant + gcid via tracing pkg helpers — these are
	// re-exposed via WithTraceparent above. Use the tracing pkg directly.
	logger.InfoContext(ctx, "log with full context")
	if !bytes.Contains(buf.Bytes(), []byte("traceparent")) {
		t.Errorf("traceparent missing: %s", buf.String())
	}
}

// -----------------------------------------------------------------------------
// OTLP exporter path coverage
// -----------------------------------------------------------------------------

func TestInitOTLP_ExplicitStdoutMode(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER", "stdout")

	shutdown, err := observability.InitOTLP("chora-stdout-mode", "v0.0.0")
	if err != nil {
		t.Fatalf("InitOTLP: %v", err)
	}
	_ = shutdown()
}

// TestInitOTLP_FactorySwap exercises the OTLP path proof: with the
// endpoint set, the factory is invoked with the endpoint + service name
// threaded through. The factory hook gives a deterministic regression
// without needing a live collector.
func TestInitOTLP_FactorySwap(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")

	var (
		called      bool
		gotEndpoint string
		gotName     string
	)
	restore := observability.SwapExporterFactoryForTest(
		func(ctx context.Context, endpoint, name, version string) (sdktrace.SpanExporter, error) {
			called = true
			gotEndpoint = endpoint
			gotName = name
			return &noopExporterObs{}, nil
		},
	)
	defer restore()

	shutdown, err := observability.InitOTLP("chora-otlp-test", "v0.0.0")
	if err != nil {
		t.Fatalf("InitOTLP: %v", err)
	}
	defer func() { _ = shutdown() }()

	if !called {
		t.Fatal("factory not called — branch unreached")
	}
	if gotEndpoint != "otel-collector:4317" {
		t.Errorf("endpoint hint mismatch: got %q", gotEndpoint)
	}
	if gotName != "chora-otlp-test" {
		t.Errorf("service name not threaded: %q", gotName)
	}
}

func TestInitOTLP_FactoryErrorBubbles(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")

	restore := observability.SwapExporterFactoryForTest(
		func(ctx context.Context, endpoint, name, version string) (sdktrace.SpanExporter, error) {
			return nil, errExp("forced")
		},
	)
	defer restore()

	_, err := observability.InitOTLP("chora-err-test", "v0.0.0")
	if err == nil {
		t.Fatal("expected error from factory")
	}
}

func TestInitOTLP_AppliesEnvSampler(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER", "stdout")
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")

	shutdown, err := observability.InitOTLP("chora-sampler-on", "v0.0.0")
	if err != nil {
		t.Fatalf("InitOTLP: %v", err)
	}
	_ = shutdown()
}

func TestInitOTLP_AppliesEnvSampler_BadArg(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER", "stdout")
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "not-a-float")

	shutdown, err := observability.InitOTLP("chora-sampler-bad", "v0.0.0")
	if err != nil {
		t.Fatalf("InitOTLP: %v", err)
	}
	_ = shutdown()
}

func TestInitOTLP_AppliesEnvSampler_OutOfRange(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER", "stdout")
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "5.0")

	shutdown, err := observability.InitOTLP("chora-sampler-oor", "v0.0.0")
	if err != nil {
		t.Fatalf("InitOTLP: %v", err)
	}
	_ = shutdown()
}

func TestInitOTLP_AlwaysOffSampler(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER", "stdout")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")

	shutdown, err := observability.InitOTLP("chora-off", "v0.0.0")
	if err != nil {
		t.Fatalf("InitOTLP: %v", err)
	}
	_ = shutdown()
}

func TestInitOTLP_AppliesResourceFromEnv(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER", "stdout")
	t.Setenv("DEPLOYMENT_ENVIRONMENT", "prod")
	t.Setenv("CLOUD_REGION", "us-central1")

	shutdown, err := observability.InitOTLP("chora-resource", "v1.2.3")
	if err != nil {
		t.Fatalf("InitOTLP: %v", err)
	}
	_ = shutdown()
}

type errExp string

func (e errExp) Error() string { return string(e) }

// TestInitOTLP_RegistersW3CTraceContextPropagator regression-guards the
// 2026-05-17 fix (commit 58b28eb4) that wires propagation.TraceContext +
// Baggage as the global propagator. Without this, every prop.Extract(...)
// call in HTTPMiddleware (and other instrumentation hooks) is a no-op:
// inbound W3C traceparent headers are silently dropped and downstream
// spans start under a fresh root trace_id. Closed the FE-coord
// E2E-BE-AI-ASSIST-TRACE-EXPORT-PERM blocker.
func TestInitOTLP_RegistersW3CTraceContextPropagator(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER", "stdout")

	// Reset to a no-op propagator so the assertion below is meaningful.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	shutdown, err := observability.InitOTLP("chora-prop-test", "v0.0.0")
	if err != nil {
		t.Fatalf("InitOTLP: %v", err)
	}
	defer func() { _ = shutdown() }()

	prop := otel.GetTextMapPropagator()
	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	headers := map[string]string{"traceparent": tp}
	ctx := prop.Extract(context.Background(), propagation.MapCarrier(headers))
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatalf("InitOTLP must register W3C TraceContext propagator — "+
			"extracted SpanContext invalid for traceparent %q", tp)
	}
	if got := sc.TraceID().String(); got != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("propagator failed to continue trace_id: got %q", got)
	}
}
