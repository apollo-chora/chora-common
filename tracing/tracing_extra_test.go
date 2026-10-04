package tracing_test

// tracing_extra_test.go — statement-coverage extension for
// tracing/traceparent.go: the statusRecorder Write path (body bytes +
// default-200 status), 4xx client-error span attribution, the
// X-Chora-GCID fallback header, the user-roles context round-trip, and
// the non-hex traceparent regeneration branch. Test-only; does not
// weaken existing assertions in tracing_test.go / hijack_test.go.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/5007-Capstone/chora/libs/chora-go-common/tracing"
)

// TestMiddleware_RecordsBodyBytesAndDefaultStatus drives the
// statusRecorder.Write path: a handler that never calls WriteHeader must
// still be attributed status 200, and the byte count must land on the
// http.response_size_bytes span attribute.
func TestMiddleware_RecordsBodyBytesAndDefaultStatus(t *testing.T) {
	rec := installTestProvider(t)

	mw := tracing.Middleware()
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("hello"))
	}))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/stream", nil))

	spans := rec.Ended()
	if len(spans) == 0 {
		t.Fatal("expected a span")
	}
	attrs := map[string]string{}
	for _, kv := range spans[0].Attributes() {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	if attrs["http.status_code"] != "200" {
		t.Errorf("http.status_code = %q, want 200 (defaulted from Write)", attrs["http.status_code"])
	}
	if attrs["http.response_size_bytes"] != "5" {
		t.Errorf("http.response_size_bytes = %q, want 5", attrs["http.response_size_bytes"])
	}
	if got := w.Body.String(); got != "hello" {
		t.Errorf("response body = %q, want hello", got)
	}
	if w.Code != http.StatusOK {
		t.Errorf("recorder code = %d, want 200", w.Code)
	}
}

// TestMiddleware_MarksClientErrorOn4xx covers the >=400 client-error
// branch (attribute, not Error status).
func TestMiddleware_MarksClientErrorOn4xx(t *testing.T) {
	rec := installTestProvider(t)

	mw := tracing.Middleware()
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/missing", nil))

	spans := rec.Ended()
	if len(spans) == 0 {
		t.Fatal("expected a span")
	}
	attrs := map[string]string{}
	for _, kv := range spans[0].Attributes() {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	if attrs["http.client_error"] != "true" {
		t.Errorf("http.client_error = %q, want true", attrs["http.client_error"])
	}
}

// TestMiddleware_GcidFromFallbackHeader covers the X-Chora-GCID header
// alias when the canonical "gcid" header is absent.
func TestMiddleware_GcidFromFallbackHeader(t *testing.T) {
	rec := installTestProvider(t)

	mw := tracing.Middleware()
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("X-Chora-GCID", "fallback-gcid-42")
	h.ServeHTTP(httptest.NewRecorder(), req)

	spans := rec.Ended()
	if len(spans) == 0 {
		t.Fatal("expected a span")
	}
	attrs := map[string]string{}
	for _, kv := range spans[0].Attributes() {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	if attrs["chora.gcid"] != "fallback-gcid-42" {
		t.Errorf("chora.gcid = %q, want fallback-gcid-42", attrs["chora.gcid"])
	}
}

// TestUserRoles_Context covers WithUserRoles / UserRolesFromContext,
// both the round-trip and the absent-call empty return.
func TestUserRoles_Context(t *testing.T) {
	ctx := context.Background()
	if got := tracing.UserRolesFromContext(ctx); got != "" {
		t.Errorf("UserRolesFromContext(empty) = %q, want empty", got)
	}
	const roles = "instructor,training-admin"
	ctx = tracing.WithUserRoles(ctx, roles)
	if got := tracing.UserRolesFromContext(ctx); got != roles {
		t.Errorf("UserRolesFromContext = %q, want %q", got, roles)
	}
}

// TestEnsureTraceparent_RegeneratesNonHex covers the hex.DecodeString
// rejection in isValidTraceparent: correct chunk LENGTHS but a non-hex
// character must not be accepted as a W3C traceparent.
func TestEnsureTraceparent_RegeneratesNonHex(t *testing.T) {
	// Exactly-32-char trace chunk with a non-hex first character, so
	// isValidTraceparent passes the length check and fails hex decoding.
	bad := "00-" + "g" + strings.Repeat("0", 31) + "-b7ad6b7169203331-01"
	got := tracing.EnsureTraceparent(bad)
	if got == bad {
		t.Errorf("non-hex traceparent must be regenerated, got %q", got)
	}
	parts := strings.Split(got, "-")
	if len(parts) != 4 || len(parts[1]) != 32 || len(parts[2]) != 16 {
		t.Errorf("regenerated value is not a well-formed W3C traceparent: %q", got)
	}
}
