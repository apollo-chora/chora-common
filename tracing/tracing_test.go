package tracing_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/apollo-chora/chora-common/tracing"
)

// installTestProvider wires an in-memory SpanRecorder so tests can assert
// what spans the middleware actually emits. Required for span-emission
// assertions per FE-coord E2E-BE-AI-ASSIST-TRACE-EXPORT-PERM 2026-05-17.
func installTestProvider(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return rec
}

func TestEnsureTraceparent_GeneratesWhenMissing(t *testing.T) {
	got := tracing.EnsureTraceparent("")
	if got == "" {
		t.Fatal("expected generated traceparent")
	}
	parts := strings.Split(got, "-")
	if len(parts) != 4 || len(parts[1]) != 32 || len(parts[2]) != 16 {
		t.Errorf("traceparent shape wrong: %q", got)
	}
}

func TestEnsureTraceparent_PreservesValid(t *testing.T) {
	in := "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	got := tracing.EnsureTraceparent(in)
	if got != in {
		t.Errorf("traceparent mutated: %q -> %q", in, got)
	}
}

func TestEnsureTraceparent_RegeneratesMalformed(t *testing.T) {
	cases := []string{
		"not-a-traceparent",
		"00-tooshort-shortspan-01",
		"00-00000000000000000000000000000000-b7ad6b7169203331-01",
		"00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
	}
	for _, c := range cases {
		got := tracing.EnsureTraceparent(c)
		if got == c {
			t.Errorf("expected regeneration for %q", c)
		}
	}
}

func TestMiddleware_StampsTraceparentOnContext(t *testing.T) {
	var captured string
	mw := tracing.Middleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = tracing.TraceparentFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if captured == "" {
		t.Error("expected non-empty traceparent on context")
	}
	if resp.Header.Get("traceparent") == "" {
		t.Error("expected traceparent in response header")
	}
}

func TestMiddleware_PreservesInboundTraceparent(t *testing.T) {
	const inbound = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	var captured string
	mw := tracing.Middleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = tracing.TraceparentFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("traceparent", inbound)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if captured != inbound {
		t.Errorf("captured=%q want %q", captured, inbound)
	}
}

func TestInject_AddsTraceparentToOutgoing(t *testing.T) {
	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	ctx := tracing.WithTraceparent(context.Background(), tp)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	tracing.Inject(req)

	if got := req.Header.Get("traceparent"); got != tp {
		t.Errorf("outgoing traceparent=%q want %q", got, tp)
	}
}

func TestInject_GeneratesWhenContextEmpty(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "http://example.test", nil)
	tracing.Inject(req)
	if req.Header.Get("traceparent") == "" {
		t.Error("Inject should mint a traceparent when ctx empty")
	}
}

func TestContextHelpers_GettersReturnEmptyWhenAbsent(t *testing.T) {
	ctx := context.Background()
	if v := tracing.TraceparentFromContext(ctx); v != "" {
		t.Errorf("TraceparentFromContext=%q want empty", v)
	}
	if v := tracing.TenantIDFromContext(ctx); v != "" {
		t.Errorf("TenantIDFromContext=%q want empty", v)
	}
	if v := tracing.GCIDFromContext(ctx); v != "" {
		t.Errorf("GCIDFromContext=%q want empty", v)
	}
}

func TestContextHelpers_RoundTrip(t *testing.T) {
	ctx := context.Background()
	ctx = tracing.WithTraceparent(ctx, "tp-val")
	ctx = tracing.WithTenantID(ctx, "tenant-1")
	ctx = tracing.WithGCID(ctx, "gcid-1")

	if v := tracing.TraceparentFromContext(ctx); v != "tp-val" {
		t.Errorf("traceparent=%q", v)
	}
	if v := tracing.TenantIDFromContext(ctx); v != "tenant-1" {
		t.Errorf("tenant_id=%q", v)
	}
	if v := tracing.GCIDFromContext(ctx); v != "gcid-1" {
		t.Errorf("gcid=%q", v)
	}
}

// ---------------------------------------------------------------------------
// Span-emission tests — Middleware must START an OTel server-side span per
// request so Cloud Trace sees the call (was missing pre-2026-05-17; only
// W3C header management was wired). Mirrors the chora-identity local
// middleware contract that was working in prod.
// ---------------------------------------------------------------------------

func TestMiddleware_EmitsServerSpanPerRequest(t *testing.T) {
	rec := installTestProvider(t)

	mw := tracing.Middleware()
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/widgets", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	spans := rec.Ended()
	if len(spans) == 0 {
		t.Fatal("expected at least one span emitted by Middleware()")
	}
	got := spans[0]
	if !strings.Contains(got.Name(), "GET") {
		t.Errorf("span name should include http method, got %q", got.Name())
	}
	if !strings.Contains(got.Name(), "/api/v1/widgets") {
		t.Errorf("span name should include http target, got %q", got.Name())
	}

	attrs := map[string]string{}
	for _, kv := range got.Attributes() {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	if attrs["http.method"] != http.MethodGet {
		t.Errorf("http.method attr=%q want %q", attrs["http.method"], http.MethodGet)
	}
	if attrs["http.target"] != "/api/v1/widgets" {
		t.Errorf("http.target attr=%q", attrs["http.target"])
	}
	if attrs["http.status_code"] != "200" {
		t.Errorf("http.status_code attr=%q want 200", attrs["http.status_code"])
	}
}

func TestMiddleware_PropagatesInboundOTelTraceContext(t *testing.T) {
	rec := installTestProvider(t)

	mw := tracing.Middleware()
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Browser/upstream sends a W3C traceparent — the middleware must
	// continue that trace (same trace_id), not start a fresh root.
	const inbound = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("traceparent", inbound)
	h.ServeHTTP(httptest.NewRecorder(), req)

	spans := rec.Ended()
	if len(spans) == 0 {
		t.Fatal("expected a span")
	}
	gotTraceID := spans[0].SpanContext().TraceID().String()
	if gotTraceID != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("trace_id=%q want continuation of inbound (0af765…); started new root",
			gotTraceID)
	}
}

func TestMiddleware_StampsTenantAndGCIDFromHeaders(t *testing.T) {
	rec := installTestProvider(t)

	mw := tracing.Middleware()
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/atoms/ai-assist", nil)
	req.Header.Set("X-Tenant-Id", "11111111-1111-7111-8111-111111111111")
	req.Header.Set("gcid", "00000000-0000-7000-8000-000000001999")
	h.ServeHTTP(httptest.NewRecorder(), req)

	spans := rec.Ended()
	if len(spans) == 0 {
		t.Fatal("expected a span")
	}
	attrs := map[string]string{}
	for _, kv := range spans[0].Attributes() {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	if attrs["chora.tenant.id"] != "11111111-1111-7111-8111-111111111111" {
		t.Errorf("chora.tenant.id=%q", attrs["chora.tenant.id"])
	}
	if attrs["chora.gcid"] != "00000000-0000-7000-8000-000000001999" {
		t.Errorf("chora.gcid=%q", attrs["chora.gcid"])
	}
}

func TestMiddleware_MarksSpanErrorOn5xx(t *testing.T) {
	rec := installTestProvider(t)

	mw := tracing.Middleware()
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	spans := rec.Ended()
	if len(spans) == 0 {
		t.Fatal("expected a span")
	}
	if spans[0].Status().Code != codes.Error {
		t.Errorf("expected Error status on 5xx, got %v", spans[0].Status().Code)
	}
}

// ---------------------------------------------------------------------------
// Context-traceparent ⇄ exported-span reconciliation. The value returned by
// TraceparentFromContext is stamped verbatim into Pub/Sub event envelopes
// (e.g. chora-creation's ai_assist.started.v2) and forwarded by outbound
// Inject(). It MUST reference the OTel server span Cloud Trace actually
// receives — otherwise a consumer that continues the trace from the envelope
// (the AI-kernel orchestrator's qgen_crew.handle_started) is parented to a
// span no service ever exported and Cloud Trace renders a synthetic
// "Missing span". Before reconciliation the middleware minted/forwarded a
// span id INDEPENDENT of tracer.Start, so the envelope pointed at a phantom.
// ---------------------------------------------------------------------------

func TestMiddleware_ContextTraceparentMatchesExportedSpan(t *testing.T) {
	rec := installTestProvider(t)

	var captured string
	mw := tracing.Middleware()
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = tracing.TraceparentFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	// No inbound traceparent — the legacy manual path used to mint a fresh
	// random root here, decoupled from the exported span (the live
	// Missing-span repro: chora-creation publishes with this value).
	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodPost, "/api/atoms/ai-assist", nil))

	spans := rec.Ended()
	if len(spans) == 0 {
		t.Fatal("expected an exported server span")
	}
	sc := spans[0].SpanContext()

	parts := strings.Split(captured, "-")
	if len(parts) != 4 || len(parts[1]) != 32 || len(parts[2]) != 16 {
		t.Fatalf("captured is not a W3C traceparent: %q", captured)
	}
	if parts[1] != sc.TraceID().String() {
		t.Errorf("traceparent trace_id=%s, want exported span's trace %s",
			parts[1], sc.TraceID())
	}
	if parts[2] != sc.SpanID().String() {
		t.Errorf("traceparent span_id=%s, want EXPORTED span %s — an envelope stamped\n"+
			"from TraceparentFromContext must reference a span Cloud Trace received,\n"+
			"else the orchestrator continuation shows a Missing-span parent",
			parts[2], sc.SpanID())
	}
}

func TestMiddleware_ContextTraceparentContinuesInboundTraceWithOwnSpan(t *testing.T) {
	rec := installTestProvider(t)
	const inbound = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

	var captured string
	mw := tracing.Middleware()
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = tracing.TraceparentFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("traceparent", inbound)
	h.ServeHTTP(httptest.NewRecorder(), req)

	spans := rec.Ended()
	if len(spans) == 0 {
		t.Fatal("expected an exported server span")
	}
	sc := spans[0].SpanContext()

	parts := strings.Split(captured, "-")
	if len(parts) != 4 {
		t.Fatalf("captured is not a W3C traceparent: %q", captured)
	}
	// Same trace as the upstream (continuation) ...
	if parts[1] != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("captured trace_id=%s, want continuation of inbound 0af765…", parts[1])
	}
	// ... but THIS service's own exported span id, never the upstream's.
	if parts[2] == "b7ad6b7169203331" {
		t.Errorf("captured span_id equals the UPSTREAM span; envelopes must carry\n" +
			"this service's own exported span so consumers nest under it")
	}
	if parts[2] != sc.SpanID().String() {
		t.Errorf("captured span_id=%s, want this service's exported span %s",
			parts[2], sc.SpanID())
	}
}
