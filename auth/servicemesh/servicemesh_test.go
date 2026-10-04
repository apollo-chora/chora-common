// Package servicemesh_test holds the RED-phase TDD specs for the
// Cloud-Service-Mesh-bound metadata propagation library. Per S3.6 spec:
//
//   chora-bff-gateway → backend services: gRPC metadata `chora-gcid`,
//   `chora-tenant-id`, `chora-role-summary` (JSON). Backend services trust
//   mTLS-bound metadata (Cloud Service Mesh asserts caller identity).
//
// chora-bff-gateway = upstream — calls Marshal() to attach claims as gRPC
// outbound metadata.
//
// Backend services = downstream — call Middleware() (HTTP) or
// UnaryServerInterceptor() (gRPC) to extract metadata + populate context.
//
// Hexagonal note: this lib is shared infra; both BFF and backend depend on
// it but neither calls the other directly.
package servicemesh_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/5007-Capstone/chora/libs/chora-go-common/auth/servicemesh"
)

// ─────────────────────────────────────────────────────────────────────────────
// MeshClaims marshalling
// ─────────────────────────────────────────────────────────────────────────────

func TestMarshalUnmarshalClaims_RoundTrip(t *testing.T) {
	t.Parallel()
	in := servicemesh.MeshClaims{
		GCID:     "01970000-0000-7000-8000-0000000000aa",
		TenantID: "01970000-0000-7000-8000-0000000000bb",
		RoleSummary: map[string]any{
			"learner":    []string{"any"},
			"instructor": []string{"course-1"},
		},
	}
	hdrs := servicemesh.MarshalToHeaders(in)
	if hdrs.Get("chora-gcid") != in.GCID {
		t.Errorf("chora-gcid = %q", hdrs.Get("chora-gcid"))
	}
	if hdrs.Get("chora-tenant-id") != in.TenantID {
		t.Errorf("chora-tenant-id = %q", hdrs.Get("chora-tenant-id"))
	}
	rsRaw := hdrs.Get("chora-role-summary")
	if rsRaw == "" {
		t.Errorf("chora-role-summary missing")
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(rsRaw), &decoded); err != nil {
		t.Errorf("chora-role-summary not valid JSON: %v", err)
	}

	out, err := servicemesh.UnmarshalFromHeaders(hdrs)
	if err != nil {
		t.Fatalf("UnmarshalFromHeaders: %v", err)
	}
	if out.GCID != in.GCID || out.TenantID != in.TenantID {
		t.Errorf("round-trip mismatch: %+v vs %+v", in, out)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP middleware — extracts mesh metadata, populates context.
// ─────────────────────────────────────────────────────────────────────────────

func TestMiddleware_PopulatesContextFromMeshMetadata(t *testing.T) {
	t.Parallel()
	var captured *servicemesh.MeshClaims
	handler := servicemesh.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		c, ok := servicemesh.ClaimsFromContext(r.Context())
		if ok {
			captured = c
		}
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/anything", nil)
	req.Header.Set("chora-gcid", "01970000-0000-7000-8000-0000000000aa")
	req.Header.Set("chora-tenant-id", "01970000-0000-7000-8000-0000000000bb")
	req.Header.Set("chora-role-summary", `{"learner":["any"]}`)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if captured == nil {
		t.Fatalf("expected claims in context")
	}
	if captured.GCID != "01970000-0000-7000-8000-0000000000aa" {
		t.Errorf("GCID = %q", captured.GCID)
	}
	if captured.TenantID != "01970000-0000-7000-8000-0000000000bb" {
		t.Errorf("TenantID = %q", captured.TenantID)
	}
	if captured.RoleSummary["learner"] == nil {
		t.Errorf("RoleSummary missing learner")
	}
}

func TestMiddleware_PassesThroughWhenNoMeshMetadata(t *testing.T) {
	t.Parallel()
	called := false
	handler := servicemesh.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		called = true
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/anything", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if !called {
		t.Errorf("expected handler to be called even without mesh metadata")
	}
}

func TestMiddleware_RequireClaimsRejects401WhenMissing(t *testing.T) {
	t.Parallel()
	handler := servicemesh.RequireClaims(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/anything", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestMiddleware_RequireClaimsAcceptsWithGCID(t *testing.T) {
	t.Parallel()
	handler := servicemesh.RequireClaims(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/anything", nil)
	req.Header.Set("chora-gcid", "01970000-0000-7000-8000-0000000000aa")
	req.Header.Set("chora-tenant-id", "01970000-0000-7000-8000-0000000000bb")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Marshal — copy values back into a downstream HTTP request.
// ─────────────────────────────────────────────────────────────────────────────

func TestMarshalToHeaders_OmitsRoleSummaryWhenEmpty(t *testing.T) {
	t.Parallel()
	hdrs := servicemesh.MarshalToHeaders(servicemesh.MeshClaims{
		GCID:     "g",
		TenantID: "t",
	})
	if hdrs.Get("chora-role-summary") != "" {
		t.Errorf("expected empty chora-role-summary, got %q", hdrs.Get("chora-role-summary"))
	}
}

func TestUnmarshalFromHeaders_RejectsMalformedRoleSummary(t *testing.T) {
	t.Parallel()
	h := http.Header{}
	h.Set("chora-gcid", "g")
	h.Set("chora-tenant-id", "t")
	h.Set("chora-role-summary", "{not-json")
	if _, err := servicemesh.UnmarshalFromHeaders(h); err == nil {
		t.Fatalf("expected malformed chora-role-summary error")
	}
}

// Bucket 4: Roles[] propagates via X-Mesh-User-Roles header.
func TestMarshalUnmarshal_Roles_RoundTrip(t *testing.T) {
	t.Parallel()
	in := servicemesh.MeshClaims{
		GCID:     "g",
		TenantID: "t",
		Roles:    []string{"learner", "author", "instructor"},
	}
	hdrs := servicemesh.MarshalToHeaders(in)
	if hdrs.Get(servicemesh.HeaderUserRoles) != "learner,author,instructor" {
		t.Errorf("HeaderUserRoles = %q, want comma-joined", hdrs.Get(servicemesh.HeaderUserRoles))
	}
	out, err := servicemesh.UnmarshalFromHeaders(hdrs)
	if err != nil {
		t.Fatalf("UnmarshalFromHeaders: %v", err)
	}
	if len(out.Roles) != 3 {
		t.Fatalf("Roles len = %d, want 3", len(out.Roles))
	}
	want := []string{"learner", "author", "instructor"}
	for i, r := range want {
		if out.Roles[i] != r {
			t.Errorf("Roles[%d] = %q want %q", i, out.Roles[i], r)
		}
	}
}

func TestMarshalToHeaders_OmitsUserRolesWhenEmpty(t *testing.T) {
	t.Parallel()
	hdrs := servicemesh.MarshalToHeaders(servicemesh.MeshClaims{
		GCID:     "g",
		TenantID: "t",
	})
	if hdrs.Get(servicemesh.HeaderUserRoles) != "" {
		t.Errorf("expected empty x-mesh-user-roles, got %q", hdrs.Get(servicemesh.HeaderUserRoles))
	}
}

func TestMarshalToHeaders_TrimsBlankRolesAndDropsEmpties(t *testing.T) {
	t.Parallel()
	hdrs := servicemesh.MarshalToHeaders(servicemesh.MeshClaims{
		GCID:     "g",
		TenantID: "t",
		Roles:    []string{" learner ", "", "  ", "author"},
	})
	got := hdrs.Get(servicemesh.HeaderUserRoles)
	if got != "learner,author" {
		t.Errorf("HeaderUserRoles = %q, want learner,author (trimmed + emptied)", got)
	}
}
