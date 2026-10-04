// Package rls — supplementary ApplySession edge tests: gcid/roles
// validation failures and per-var Exec error propagation.
package rls_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/rls"
	"github.com/apollo-chora/chora-common/tracing"
)

const (
	testTenantID = "01970000-0000-7000-8000-0000000000aa"
	testGCID     = "01970000-0000-7000-8000-0000000000bb"
)

// recordingExecer fails on the failAt-th Exec call (1-based), letting
// tests target the tenant / gcid / roles statement independently.
type recordingExecer struct {
	calls  int
	failAt int
	err    error
}

func (e *recordingExecer) Exec(_ context.Context, _ string, _ ...any) (rls.CommandTag, error) {
	e.calls++
	if e.calls == e.failAt {
		return rls.CommandTag{}, e.err
	}
	return rls.CommandTag{RowsAffected: 1}, nil
}

func TestApplySession_GcidValidationError(t *testing.T) {
	t.Parallel()
	ctx := tracing.WithTenantID(context.Background(), testTenantID)
	ctx = tracing.WithGCID(ctx, "bad gcid") // whitespace → invalid
	e := &recordingExecer{}
	err := rls.ApplySession(ctx, e)
	if !errors.Is(err, rls.ErrInvalidIdentifier) {
		t.Fatalf("err = %v, want ErrInvalidIdentifier", err)
	}
	if e.calls != 1 {
		t.Errorf("expected only the tenant SET LOCAL before the gcid invalid; calls = %d", e.calls)
	}
}

func TestApplySession_GcidExecError(t *testing.T) {
	t.Parallel()
	ctx := tracing.WithTenantID(context.Background(), testTenantID)
	ctx = tracing.WithGCID(ctx, testGCID)
	e := &recordingExecer{failAt: 2, err: errors.New("db down")}
	err := rls.ApplySession(ctx, e)
	if err == nil {
		t.Fatal("expected exec error")
	}
	if !strings.Contains(err.Error(), "chora.user_gcid failed") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestApplySession_RolesValidationError(t *testing.T) {
	t.Parallel()
	ctx := tracing.WithTenantID(context.Background(), testTenantID)
	ctx = tracing.WithUserRoles(ctx, "bad role") // whitespace → invalid
	e := &recordingExecer{}
	err := rls.ApplySession(ctx, e)
	if !errors.Is(err, rls.ErrInvalidIdentifier) {
		t.Fatalf("err = %v, want ErrInvalidIdentifier", err)
	}
}

func TestApplySession_RolesExecError(t *testing.T) {
	t.Parallel()
	ctx := tracing.WithTenantID(context.Background(), testTenantID)
	ctx = tracing.WithUserRoles(ctx, "instructor")
	e := &recordingExecer{failAt: 2, err: errors.New("db down")} // tenant #1 ok, roles #2 fails
	err := rls.ApplySession(ctx, e)
	if err == nil {
		t.Fatal("expected exec error")
	}
	if !strings.Contains(err.Error(), "chora.user_roles failed") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateGCID_RejectsWhitespace(t *testing.T) {
	t.Parallel()
	if err := rls.ValidateGCID(" " + testGCID); !errors.Is(err, rls.ErrInvalidIdentifier) {
		t.Fatalf("err = %v, want ErrInvalidIdentifier", err)
	}
}

func TestValidateUserRoles_AllowsDigitsUppercaseDashUnderscoreComma(t *testing.T) {
	t.Parallel()
	if err := rls.ValidateUserRoles("ADMIN,role2,a_b,training-admin"); err != nil {
		t.Fatalf("ValidateUserRoles(%q) = %v, want nil", "ADMIN,role2,a_b,training-admin", err)
	}
}

func TestValidateTenantID_AllowsUppercaseHex(t *testing.T) {
	t.Parallel()
	if err := rls.ValidateTenantID(strings.ToUpper(testTenantID)); err != nil {
		t.Fatalf("ValidateTenantID(upper hex) = %v, want nil", err)
	}
}
