// Package rls_test exercises the in-process behaviour of the RLS helper —
// the part that does NOT require a Postgres connection.
//
// Postgres-bound integration tests live in:
//   - tests/rls-isolation-active/ (cross-service leak harness, build tag
//     `integration`)
//   - services/{service}/internal/adapter/repo/...test.go (per-service
//     integration tests)
//
// This file deliberately stays mock-only so it can run on any developer
// machine + CI without docker.
package rls_test

import (
	"context"
	"errors"
	"testing"

	"github.com/5007-Capstone/chora/libs/chora-go-common/rls"
	"github.com/5007-Capstone/chora/libs/chora-go-common/tracing"
)

// fakeExecer is a tx-shaped recorder that captures the SET LOCAL statements
// the helper would issue, so tests can assert exact wire content.
type fakeExecer struct {
	calls   []string
	failOn  string // substring; if execed query contains this, return failErr
	failErr error
}

func (f *fakeExecer) Exec(ctx context.Context, sql string, args ...any) (rls.CommandTag, error) {
	f.calls = append(f.calls, sql)
	if f.failOn != "" && contains(sql, f.failOn) {
		return rls.CommandTag{}, f.failErr
	}
	return rls.CommandTag{}, nil
}

func contains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// TestApplySession_TenantOnly proves the helper issues exactly one
// SET LOCAL chora.tenant_id statement when only tenant_id is in ctx.
func TestApplySession_TenantOnly(t *testing.T) {
	t.Parallel()

	ctx := tracing.WithTenantID(context.Background(), "11111111-1111-7111-8111-111111111111")
	fe := &fakeExecer{}

	if err := rls.ApplySession(ctx, fe); err != nil {
		t.Fatalf("ApplySession: %v", err)
	}

	if got, want := len(fe.calls), 1; got != want {
		t.Fatalf("call count = %d, want %d (calls=%v)", got, want, fe.calls)
	}
	want := "SET LOCAL chora.tenant_id = '11111111-1111-7111-8111-111111111111'"
	if fe.calls[0] != want {
		t.Errorf("call[0] = %q, want %q", fe.calls[0], want)
	}
}

// TestApplySession_TenantAndGCID proves dual-scoped tables (per ADR-143
// per-user KG) get both SET LOCALs in a single transaction.
func TestApplySession_TenantAndGCID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctx = tracing.WithTenantID(ctx, "22222222-2222-7222-8222-222222222222")
	ctx = tracing.WithGCID(ctx, "33333333-3333-7333-8333-333333333333")
	fe := &fakeExecer{}

	if err := rls.ApplySession(ctx, fe); err != nil {
		t.Fatalf("ApplySession: %v", err)
	}

	if got, want := len(fe.calls), 2; got != want {
		t.Fatalf("call count = %d, want %d (calls=%v)", got, want, fe.calls)
	}
	wantTenant := "SET LOCAL chora.tenant_id = '22222222-2222-7222-8222-222222222222'"
	wantGCID := "SET LOCAL chora.user_gcid = '33333333-3333-7333-8333-333333333333'"
	if fe.calls[0] != wantTenant {
		t.Errorf("tenant call = %q, want %q", fe.calls[0], wantTenant)
	}
	if fe.calls[1] != wantGCID {
		t.Errorf("gcid call = %q, want %q", fe.calls[1], wantGCID)
	}
}

// TestApplySession_WithUserRoles proves a role-aware context emits all three
// SET LOCALs (tenant + gcid + user_roles) in order.
func TestApplySession_WithUserRoles(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctx = tracing.WithTenantID(ctx, "22222222-2222-7222-8222-222222222222")
	ctx = tracing.WithGCID(ctx, "33333333-3333-7333-8333-333333333333")
	ctx = tracing.WithUserRoles(ctx, "instructor,training-admin")
	fe := &fakeExecer{}

	if err := rls.ApplySession(ctx, fe); err != nil {
		t.Fatalf("ApplySession: %v", err)
	}
	if got, want := len(fe.calls), 3; got != want {
		t.Fatalf("call count = %d, want %d (calls=%v)", got, want, fe.calls)
	}
	wantRoles := "SET LOCAL chora.user_roles = 'instructor,training-admin'"
	if fe.calls[2] != wantRoles {
		t.Errorf("roles call = %q, want %q", fe.calls[2], wantRoles)
	}
}

// TestApplySession_RolesWithoutGCID proves roles are set independently of gcid
// (an empty gcid no longer short-circuits before the roles GUC).
func TestApplySession_RolesWithoutGCID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctx = tracing.WithTenantID(ctx, "22222222-2222-7222-8222-222222222222")
	ctx = tracing.WithUserRoles(ctx, "admin")
	fe := &fakeExecer{}

	if err := rls.ApplySession(ctx, fe); err != nil {
		t.Fatalf("ApplySession: %v", err)
	}
	if got, want := len(fe.calls), 2; got != want {
		t.Fatalf("call count = %d, want %d (calls=%v)", got, want, fe.calls)
	}
	if fe.calls[1] != "SET LOCAL chora.user_roles = 'admin'" {
		t.Errorf("roles call = %q", fe.calls[1])
	}
}

// TestValidateUserRoles covers the comma-list allowance + injection rejection.
func TestValidateUserRoles(t *testing.T) {
	t.Parallel()
	ok := []string{"admin", "instructor,training-admin", "tenant_admin", "a-b,c_d"}
	for _, r := range ok {
		if err := rls.ValidateUserRoles(r); err != nil {
			t.Errorf("ValidateUserRoles(%q) = %v, want nil", r, err)
		}
	}
	bad := []string{"", "admin;DROP", "admin'--", "role with space", " admin", "admin ", "rö le"}
	for _, r := range bad {
		if err := rls.ValidateUserRoles(r); !errors.Is(err, rls.ErrInvalidIdentifier) {
			t.Errorf("ValidateUserRoles(%q) = %v, want ErrInvalidIdentifier", r, err)
		}
	}
}

// TestApplySession_NoTenantContext returns ErrNoTenantContext when no
// tenant is set on ctx — RLS would silently return zero rows otherwise.
func TestApplySession_NoTenantContext(t *testing.T) {
	t.Parallel()

	fe := &fakeExecer{}
	err := rls.ApplySession(context.Background(), fe)
	if !errors.Is(err, rls.ErrNoTenantContext) {
		t.Fatalf("got %v, want ErrNoTenantContext", err)
	}
	if len(fe.calls) != 0 {
		t.Fatalf("no calls expected, got %v", fe.calls)
	}
}

// TestApplySession_PlatformSentinel allows the literal "platform" tenant
// (per envelope.proto comment) for cross-tenant platform-level operations.
func TestApplySession_PlatformSentinel(t *testing.T) {
	t.Parallel()

	ctx := tracing.WithTenantID(context.Background(), "platform")
	fe := &fakeExecer{}

	if err := rls.ApplySession(ctx, fe); err != nil {
		t.Fatalf("ApplySession: %v", err)
	}

	if got, want := len(fe.calls), 1; got != want {
		t.Fatalf("call count = %d, want %d", got, want)
	}
	want := "SET LOCAL chora.tenant_id = 'platform'"
	if fe.calls[0] != want {
		t.Errorf("call[0] = %q, want %q", fe.calls[0], want)
	}
}

// TestApplySession_RejectsInjection makes sure tenant_id values containing
// SQL meta-characters never reach the wire as-is. RLS itself would catch
// invalid UUID casts but we want defence-in-depth at the helper.
func TestApplySession_RejectsInjection(t *testing.T) {
	t.Parallel()

	cases := []string{
		"'; DROP TABLE atom; --",
		"a' OR '1'='1",
		"abc\x00def",
		"line\nbreak",
		"  spaced  ",
	}
	for _, payload := range cases {
		payload := payload
		t.Run(payload, func(t *testing.T) {
			ctx := tracing.WithTenantID(context.Background(), payload)
			fe := &fakeExecer{}
			err := rls.ApplySession(ctx, fe)
			if !errors.Is(err, rls.ErrInvalidIdentifier) {
				t.Fatalf("payload %q: got %v, want ErrInvalidIdentifier", payload, err)
			}
		})
	}
}

// TestApplySession_PropagatesExecError surfaces the underlying tx error
// instead of swallowing it.
func TestApplySession_PropagatesExecError(t *testing.T) {
	t.Parallel()

	ctx := tracing.WithTenantID(context.Background(), "44444444-4444-7444-8444-444444444444")
	wantErr := errors.New("simulated tx failure")
	fe := &fakeExecer{failOn: "SET LOCAL", failErr: wantErr}

	err := rls.ApplySession(ctx, fe)
	if !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want wrapped %v", err, wantErr)
	}
}

// TestRunInTx_Success exercises the convenience runner — apply session
// then call user fn — proving tenant context is set before fn runs.
func TestRunInTx_Success(t *testing.T) {
	t.Parallel()

	ctx := tracing.WithTenantID(context.Background(), "55555555-5555-7555-8555-555555555555")
	fe := &fakeExecer{}

	called := false
	err := rls.RunInTx(ctx, fe, func(_ context.Context, e rls.Execer) error {
		called = true
		_, _ = e.Exec(ctx, "SELECT 1")
		return nil
	})
	if err != nil {
		t.Fatalf("RunInTx: %v", err)
	}
	if !called {
		t.Errorf("user fn not invoked")
	}
	// Order: SET LOCAL tenant, then user query.
	if len(fe.calls) < 2 {
		t.Fatalf("expected ≥ 2 exec calls, got %d", len(fe.calls))
	}
	if !contains(fe.calls[0], "SET LOCAL chora.tenant_id") {
		t.Errorf("first call should be SET LOCAL tenant, got %q", fe.calls[0])
	}
	if fe.calls[1] != "SELECT 1" {
		t.Errorf("second call should be user query, got %q", fe.calls[1])
	}
}

// TestRunInTx_NoTenant short-circuits before invoking user fn.
func TestRunInTx_NoTenant(t *testing.T) {
	t.Parallel()

	fe := &fakeExecer{}
	called := false
	err := rls.RunInTx(context.Background(), fe, func(_ context.Context, _ rls.Execer) error {
		called = true
		return nil
	})
	if !errors.Is(err, rls.ErrNoTenantContext) {
		t.Fatalf("got %v, want ErrNoTenantContext", err)
	}
	if called {
		t.Errorf("user fn should not run when tenant is missing")
	}
}

// TestValidateTenantID is a public-facing helper — surface for callers
// who want to validate before opening a transaction.
func TestValidateTenantID(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		"":                                     false,
		"   ":                                  false,
		"platform":                             true,
		"00000000-0000-0000-0000-000000000000": true,
		"abcd1234-ab12-7c34-8d56-ef7890123456": true,
		"contains;semi":                        false,
		"contains'apost":                       false,
		"contains\"quote":                      false,
		"contains\nnewline":                    false,
		"contains\x00null":                     false,
	}
	for input, wantOK := range cases {
		input, wantOK := input, wantOK
		t.Run(input, func(t *testing.T) {
			err := rls.ValidateTenantID(input)
			if wantOK && err != nil {
				t.Fatalf("expected ok, got %v", err)
			}
			if !wantOK && err == nil {
				t.Fatalf("expected err, got nil for %q", input)
			}
		})
	}
}

// TestValidateGCID — same surface for gcid/agid identifiers.
func TestValidateGCID(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		"":                                     false,
		"abcd1234-ab12-7c34-8d56-ef7890123456": true,
		"0197a000-0000-0000-0000-000000000000": true, // AGID-shaped is OK at the helper level
		"contains;evil":                        false,
	}
	for input, wantOK := range cases {
		input, wantOK := input, wantOK
		t.Run(input, func(t *testing.T) {
			err := rls.ValidateGCID(input)
			if wantOK && err != nil {
				t.Fatalf("expected ok, got %v", err)
			}
			if !wantOK && err == nil {
				t.Fatalf("expected err, got nil for %q", input)
			}
		})
	}
}
