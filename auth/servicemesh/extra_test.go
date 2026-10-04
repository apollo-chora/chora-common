// Package servicemesh — supplementary middleware 400-path test.
package servicemesh_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/auth/servicemesh"
)

// TestMiddleware_RejectsMalformedRoleSummary — a role-summary header that
// is not valid JSON must yield HTTP 400 and must NOT invoke the next
// handler.
func TestMiddleware_RejectsMalformedRoleSummary(t *testing.T) {
	t.Parallel()
	nextCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/x", nil)
	req.Header.Set(servicemesh.HeaderGCID, "01970000-0000-7000-8000-0000000000aa")
	req.Header.Set(servicemesh.HeaderTenantID, "01970000-0000-7000-8000-0000000000bb")
	req.Header.Set(servicemesh.HeaderRoleSummary, "not-json{")

	rec := httptest.NewRecorder()
	servicemesh.Middleware(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "malformed") {
		t.Errorf("body = %q, want malformed-role-summary message", rec.Body.String())
	}
	if nextCalled {
		t.Error("next handler must not run on malformed role summary")
	}
}
