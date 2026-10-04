// Package tracing wires W3C trace-context propagation across HTTP boundaries
// and exposes context helpers for traceparent / tenant_id / gcid.
//
// Per CLAUDE.md §6 ("Trace context across Pub/Sub" + "OTLP everywhere"),
// every service must propagate W3C traceparent on inbound + outbound calls
// and include it in Pub/Sub event envelopes (see envelope package).
package tracing

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
)

// HeaderTraceparent is the W3C trace-context header name.
const HeaderTraceparent = "traceparent"

// EnsureTraceparent returns a W3C traceparent header for the request. If the
// inbound value is missing or malformed, a fresh root traceparent is minted.
// The returned value is safe to forward to upstream calls.
func EnsureTraceparent(inbound string) string {
	t := strings.TrimSpace(inbound)
	if isValidTraceparent(t) {
		return t
	}
	return newTraceparent()
}

// isValidTraceparent does a structural W3C check: 4 dash-separated hex chunks
// of expected lengths, with non-zero trace_id and span_id.
func isValidTraceparent(s string) bool {
	parts := strings.Split(s, "-")
	if len(parts) != 4 {
		return false
	}
	expectedLens := []int{2, 32, 16, 2}
	for i, want := range expectedLens {
		if len(parts[i]) != want {
			return false
		}
		if _, err := hex.DecodeString(parts[i]); err != nil {
			return false
		}
	}
	if strings.Trim(parts[1], "0") == "" {
		return false
	}
	if strings.Trim(parts[2], "0") == "" {
		return false
	}
	return true
}

// newTraceparent mints a fresh W3C traceparent using crypto/rand.
func newTraceparent() string {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	_, _ = rand.Read(traceID)
	_, _ = rand.Read(spanID)
	if isAllZero(traceID) {
		traceID[0] = 1
	}
	if isAllZero(spanID) {
		spanID[0] = 1
	}
	return fmt.Sprintf("00-%s-%s-01", hex.EncodeToString(traceID), hex.EncodeToString(spanID))
}

func isAllZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

// statusRecorder captures the status + bytes written by an inner http.Handler
// so the middleware can stamp them on the OTel span. It also FORWARDS the
// optional http.Hijacker + http.Flusher interfaces to the wrapped writer —
// because it embeds the http.ResponseWriter *interface* (not the concrete
// type), Go cannot auto-promote those methods, so an unwrapped middleware
// silently masks them. Without Hijack delegation, WebSocket-upgrade proxies
// (chora-gateway's rplus-delivery WS hijack) fail with "response writer is not
// a Hijacker" even over HTTP/1.1; without Flush, SSE streaming buffers.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.status = code
	sr.ResponseWriter.WriteHeader(code)
}

func (sr *statusRecorder) Write(p []byte) (int, error) {
	if sr.status == 0 {
		sr.status = http.StatusOK
	}
	n, err := sr.ResponseWriter.Write(p)
	sr.bytes += int64(n)
	return n, err
}

// Hijack delegates to the wrapped ResponseWriter when it supports hijacking
// (HTTP/1.1 net/http servers do). Returns an error when the underlying writer
// is not a Hijacker (e.g. HTTP/2), so callers get a precise diagnostic instead
// of the wrapper silently swallowing the interface.
func (sr *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := sr.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, errors.New("tracing: underlying ResponseWriter does not support Hijack (HTTP/2?)")
}

// Flush delegates to the wrapped ResponseWriter's Flusher when present (SSE).
func (sr *statusRecorder) Flush() {
	if fl, ok := sr.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

// Unwrap exposes the wrapped ResponseWriter so http.NewResponseController can
// traverse this middleware wrapper to the underlying connection. Without it the
// controller stops at statusRecorder and returns ErrNotSupported, which would
// silently defeat any per-route SetReadDeadline/SetWriteDeadline (the AI-Assist
// upload deadline extension) behind this middleware.
func (sr *statusRecorder) Unwrap() http.ResponseWriter {
	return sr.ResponseWriter
}

// Middleware returns a Chi-compatible HTTP middleware that:
//
//  1. Ensures every request carries a valid W3C traceparent on its context
//     + response header (header-only path, preserved from M1-M9).
//  2. **Extracts inbound W3C TraceContext via the OTel propagator and
//     starts an OTel server-side span per request** (added 2026-05-17 per
//     FE-coord E2E-BE-AI-ASSIST-TRACE-EXPORT-PERM — previously only
//     chora-identity had this via a local middleware; every other Go
//     service using commonobs.HTTPMiddleware was missing server-side
//     request spans in Cloud Trace).
//
// Span shape:
//
//   - Name: “{METHOD} {path}“ (semantic-convention friendly)
//   - Attrs: http.method / http.target / http.status_code /
//     http.response_size_bytes / chora.tenant.id / chora.gcid
//   - Status: Error on 5xx; client_error attr on 4xx
//
// The tracer is named “chora-go-common/tracing“; service.name on the
// resource (set by observability.InitOTLP) is what Cloud Trace shows
// in the service filter.
//
// Usage with chi:
//
//	r := chi.NewRouter()
//	r.Use(tracing.Middleware())
//
// Usage with stdlib mux is identical (signature is std http.Handler).
func Middleware() func(next http.Handler) http.Handler {
	tracer := otel.Tracer("chora-go-common/tracing")
	prop := otel.GetTextMapPropagator()
	if prop == nil {
		prop = propagation.TraceContext{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// W3C traceparent header bookkeeping (legacy path — kept so the
			// existing context helpers + outbound Inject() still work).
			tp := EnsureTraceparent(r.Header.Get(HeaderTraceparent))
			w.Header().Set(HeaderTraceparent, tp)
			ctx := WithTraceparent(r.Context(), tp)

			// OTel propagator extract — continues an inbound trace_id when
			// the upstream is OTel-aware (browser, chora-gateway), starts a
			// fresh root otherwise.
			ctx = prop.Extract(ctx, propagation.HeaderCarrier(r.Header))

			spanName := r.Method + " " + r.URL.Path
			ctx, span := tracer.Start(ctx, spanName)
			defer span.End()

			// Reconcile the context-stored W3C traceparent with the REAL
			// OTel span Cloud Trace receives. EnsureTraceparent above
			// mints/forwards a span id that is INDEPENDENT of tracer.Start's
			// span; without this step the value returned by
			// TraceparentFromContext — stamped verbatim into Pub/Sub event
			// envelopes (e.g. chora-creation's ai_assist.started.v2) and
			// forwarded by outbound Inject() — references a span Cloud Trace
			// never exported. A consumer that continues the trace from the
			// envelope (the AI-kernel orchestrator's qgen_crew.handle_started)
			// is then parented to a phantom and Cloud Trace renders a
			// synthetic "Missing span". Re-deriving from span.SpanContext()
			// points the propagated traceparent at THIS service's own
			// exported server span (same trace_id as any inbound upstream;
			// this service's own span_id). Guarded by IsValid() so a no-op
			// tracer (SDK not yet wired) falls back to the legacy manual
			// value unchanged.
			if sc := span.SpanContext(); sc.IsValid() {
				tp = "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-" + sc.TraceFlags().String()
				ctx = WithTraceparent(ctx, tp)
				w.Header().Set(HeaderTraceparent, tp)
			}

			span.SetAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.target", r.URL.Path),
			)
			if tenantID := r.Header.Get("X-Tenant-Id"); tenantID != "" {
				span.SetAttributes(attribute.String("chora.tenant.id", tenantID))
			}
			if gcid := r.Header.Get("gcid"); gcid != "" {
				span.SetAttributes(attribute.String("chora.gcid", gcid))
			} else if gcid := r.Header.Get("X-Chora-GCID"); gcid != "" {
				span.SetAttributes(attribute.String("chora.gcid", gcid))
			}

			// Inject trace context into the response so downstream callers
			// can correlate (mirrors chora-identity behaviour).
			prop.Inject(ctx, propagation.HeaderCarrier(w.Header()))

			sr := &statusRecorder{ResponseWriter: w, status: 0}
			next.ServeHTTP(sr, r.WithContext(ctx))

			// Default to 200 if the handler returned without WriteHeader.
			status := sr.status
			if status == 0 {
				status = http.StatusOK
			}
			span.SetAttributes(
				attribute.Int("http.status_code", status),
				attribute.String("http.status_code.str", strconv.Itoa(status)),
				attribute.Int64("http.response_size_bytes", sr.bytes),
			)
			if status >= 500 {
				span.SetStatus(codes.Error, "5xx response")
			} else if status >= 400 {
				span.SetAttributes(attribute.Bool("http.client_error", true))
			}
		})
	}
}

// Inject stamps the request's traceparent header from the context. If the
// context has no traceparent, a fresh one is minted (defence-in-depth — every
// outbound call MUST carry one).
func Inject(req *http.Request) {
	tp := TraceparentFromContext(req.Context())
	if tp == "" {
		tp = newTraceparent()
	}
	req.Header.Set(HeaderTraceparent, tp)
}

// ----------------------------------------------------------------------------
// Context helpers
// ----------------------------------------------------------------------------

type ctxKey string

const (
	ctxKeyTraceparent ctxKey = "chora.traceparent"
	ctxKeyTenantID    ctxKey = "chora.tenant_id"
	ctxKeyGCID        ctxKey = "chora.gcid"
	ctxKeyUserRoles   ctxKey = "chora.user_roles"
)

// WithTraceparent returns a child context carrying the traceparent.
func WithTraceparent(ctx context.Context, tp string) context.Context {
	return context.WithValue(ctx, ctxKeyTraceparent, tp)
}

// WithTenantID returns a child context carrying the tenant ID.
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, ctxKeyTenantID, tenantID)
}

// WithGCID returns a child context carrying the actor GCID (or AGID for agents).
func WithGCID(ctx context.Context, gcid string) context.Context {
	return context.WithValue(ctx, ctxKeyGCID, gcid)
}

// TraceparentFromContext returns the traceparent stored on ctx, or "".
func TraceparentFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyTraceparent).(string); ok {
		return v
	}
	return ""
}

// TenantIDFromContext returns the tenant_id stored on ctx, or "".
func TenantIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyTenantID).(string); ok {
		return v
	}
	return ""
}

// GCIDFromContext returns the gcid stored on ctx, or "".
func GCIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyGCID).(string); ok {
		return v
	}
	return ""
}

// WithUserRoles returns a child context carrying the caller's role set (the raw
// comma-separated mesh role list, e.g. "instructor,training-admin"). Consumed by
// rls.ApplySession → SET LOCAL chora.user_roles for role-aware RLS policies.
func WithUserRoles(ctx context.Context, roles string) context.Context {
	return context.WithValue(ctx, ctxKeyUserRoles, roles)
}

// UserRolesFromContext returns the caller's role set stored on ctx, or "".
func UserRolesFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyUserRoles).(string); ok {
		return v
	}
	return ""
}
