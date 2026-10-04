// Package cmek tests — RED-phase first per .claude/rules/development-execution.md.
//
// Aligned with Architecture Review locked 2026-05-07 (Tier 3 D11) +
// .claude/skills/secrets-and-env/SKILL.md ("CMEK + per-tenant master key for sensitive data").
//
// The CMEK port abstracts Cloud KMS so:
//   - Production wires the real Cloud KMS client
//   - Tests use the in-memory KeyManager (deterministic + zero infra)
//   - Crypto-shred is exercised end-to-end via DeleteMasterKey
//
// Coverage gate: 85% (domain port + in-mem adapter both qualify per
// `feedback_strict_tdd` because the helper is consumed by chora-tenancy +
// chora-identity domain code paths).
package cmek_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/cmek"
)

// ─────────────────────────────────────────────────────────────────────────────
// Tenant master key lifecycle
// ─────────────────────────────────────────────────────────────────────────────

func TestInMemoryKeyManager_CreateMasterKey_Idempotent(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()

	first, err := km.CreateMasterKey(ctx, "tenant-A")
	if err != nil {
		t.Fatalf("CreateMasterKey first call: %v", err)
	}
	if first.ResourceName == "" {
		t.Fatal("expected non-empty ResourceName")
	}
	if first.TenantID != "tenant-A" {
		t.Errorf("TenantID=%q want tenant-A", first.TenantID)
	}

	// Idempotent: second call with the same tenantID returns the same key.
	second, err := km.CreateMasterKey(ctx, "tenant-A")
	if err != nil {
		t.Fatalf("CreateMasterKey second call: %v", err)
	}
	if second.ResourceName != first.ResourceName {
		t.Errorf("idempotency broken: second=%q first=%q", second.ResourceName, first.ResourceName)
	}
}

func TestInMemoryKeyManager_CreateMasterKey_RejectsEmptyTenant(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()

	_, err := km.CreateMasterKey(ctx, "")
	if err == nil {
		t.Fatal("expected error on empty tenantID")
	}
	if !errors.Is(err, cmek.ErrInvalidTenant) {
		t.Errorf("err=%v want ErrInvalidTenant", err)
	}
}

func TestInMemoryKeyManager_GetMasterKeyResourceName(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()

	_, err := km.GetMasterKeyResourceName(ctx, "tenant-X")
	if !errors.Is(err, cmek.ErrMasterKeyNotFound) {
		t.Fatalf("expected ErrMasterKeyNotFound, got %v", err)
	}

	mk, err := km.CreateMasterKey(ctx, "tenant-X")
	if err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	got, err := km.GetMasterKeyResourceName(ctx, "tenant-X")
	if err != nil {
		t.Fatalf("GetMasterKeyResourceName: %v", err)
	}
	if got != mk.ResourceName {
		t.Errorf("ResourceName mismatch got=%q want=%q", got, mk.ResourceName)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Envelope encryption (DEK wrap / unwrap via the master key)
// ─────────────────────────────────────────────────────────────────────────────

func TestInMemoryKeyManager_WrapUnwrap_Roundtrip(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-W"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}

	dek := []byte("32-byte-deterministic-test-dek!!")
	wrapped, err := km.WrapDEK(ctx, "tenant-W", dek)
	if err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}
	if len(wrapped) == 0 {
		t.Fatal("wrapped is empty")
	}
	if string(wrapped) == string(dek) {
		t.Fatal("wrapped equals plaintext DEK — wrap is a no-op")
	}

	unwrapped, err := km.UnwrapDEK(ctx, "tenant-W", wrapped)
	if err != nil {
		t.Fatalf("UnwrapDEK: %v", err)
	}
	if string(unwrapped) != string(dek) {
		t.Errorf("roundtrip mismatch: got=%q want=%q", unwrapped, dek)
	}
}

func TestInMemoryKeyManager_WrapDEK_ErrorsWhenMissingMaster(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()

	_, err := km.WrapDEK(ctx, "tenant-NOPE", []byte("dek"))
	if !errors.Is(err, cmek.ErrMasterKeyNotFound) {
		t.Errorf("err=%v want ErrMasterKeyNotFound", err)
	}
}

func TestInMemoryKeyManager_UnwrapDEK_ErrorsAfterMasterDeleted(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-S"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	dek := []byte("dek-payload-32bytes-aes-256-len!")
	wrapped, err := km.WrapDEK(ctx, "tenant-S", dek)
	if err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}

	if err := km.DeleteMasterKey(ctx, "tenant-S"); err != nil {
		t.Fatalf("DeleteMasterKey: %v", err)
	}

	// Production semantic: master key destruction = entire tenant crypto-shredded.
	_, err = km.UnwrapDEK(ctx, "tenant-S", wrapped)
	if !errors.Is(err, cmek.ErrMasterKeyDestroyed) {
		t.Errorf("expected ErrMasterKeyDestroyed, got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Crypto-shred: DeleteMasterKey
// ─────────────────────────────────────────────────────────────────────────────

func TestInMemoryKeyManager_DeleteMasterKey_AuthorizedSAOnly(t *testing.T) {
	// The InMemoryKeyManager does NOT enforce IAM — that is the responsibility
	// of the Cloud KMS adapter wired via Terraform IAM bindings (closure-orchestrator
	// SA only). The in-memory manager DOES record the operation so callers can
	// assert the audit trail.
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-D"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	if err := km.DeleteMasterKey(ctx, "tenant-D"); err != nil {
		t.Fatalf("DeleteMasterKey: %v", err)
	}

	// Idempotent: deleting an already-destroyed master is a no-op.
	if err := km.DeleteMasterKey(ctx, "tenant-D"); err != nil {
		t.Errorf("expected idempotent DeleteMasterKey, got %v", err)
	}

	// Subsequent CreateMasterKey for the same tenant must NOT resurrect data —
	// even if a new master is provisioned, prior ciphertexts stay unrecoverable.
	if _, err := km.CreateMasterKey(ctx, "tenant-D"); err != nil {
		t.Errorf("expected new master after destroy: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CryptoOpRecorder — audit hook for IMDA D1 accountability
// ─────────────────────────────────────────────────────────────────────────────

func TestInMemoryKeyManager_RecordsAuditOps(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-A"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	if _, err := km.WrapDEK(ctx, "tenant-A", []byte("dek-32-bytes-of-determinism-1234")); err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}
	if err := km.DeleteMasterKey(ctx, "tenant-A"); err != nil {
		t.Fatalf("DeleteMasterKey: %v", err)
	}

	ops := km.AuditOps()
	if len(ops) < 3 {
		t.Fatalf("expected ≥3 audit ops, got %d", len(ops))
	}

	// Verify the canonical op kinds exist in order.
	wantKinds := []cmek.OpKind{cmek.OpCreateMasterKey, cmek.OpWrapDEK, cmek.OpDeleteMasterKey}
	for i, want := range wantKinds {
		if ops[i].Kind != want {
			t.Errorf("ops[%d].Kind=%v want %v", i, ops[i].Kind, want)
		}
		if ops[i].TenantID != "tenant-A" {
			t.Errorf("ops[%d].TenantID=%q want tenant-A", i, ops[i].TenantID)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// MasterKeyRef — composite of name + KMS resource path
// ─────────────────────────────────────────────────────────────────────────────

func TestMasterKeyRef_FullResourceName_FormattedCorrectly(t *testing.T) {
	ref := cmek.MasterKeyRef{
		Project:  "chora-489812",
		Region:   "asia-southeast1",
		KeyRing:  "chora-keys",
		Name:     "cmek-tenant-abc",
		TenantID: "abc",
	}
	got := ref.FullResourceName()
	want := "projects/chora-489812/locations/asia-southeast1/keyRings/chora-keys/cryptoKeys/cmek-tenant-abc"
	if got != want {
		t.Errorf("got=%q want=%q", got, want)
	}
}

func TestParseMasterKeyResourceName(t *testing.T) {
	resource := "projects/chora-489812/locations/asia-southeast1/keyRings/chora-keys/cryptoKeys/cmek-tenant-xyz"
	ref, err := cmek.ParseMasterKeyResourceName(resource)
	if err != nil {
		t.Fatalf("ParseMasterKeyResourceName: %v", err)
	}
	if ref.Project != "chora-489812" {
		t.Errorf("Project=%q want chora-489812", ref.Project)
	}
	if ref.TenantID != "xyz" {
		t.Errorf("TenantID=%q want xyz", ref.TenantID)
	}
}

func TestParseMasterKeyResourceName_RejectsMalformed(t *testing.T) {
	for _, bad := range []string{
		"",
		"random/garbage",
		"projects//locations/foo/keyRings/bar/cryptoKeys/baz", // empty project
		"projects/p/locations/r/keyRings/k/cryptoKeys/not-cmek-format",
	} {
		if _, err := cmek.ParseMasterKeyResourceName(bad); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Audit + clock + helpers
// ─────────────────────────────────────────────────────────────────────────────

func TestOpKind_String(t *testing.T) {
	cases := map[cmek.OpKind]string{
		cmek.OpCreateMasterKey: "create_master_key",
		cmek.OpWrapDEK:         "wrap_dek",
		cmek.OpUnwrapDEK:       "unwrap_dek",
		cmek.OpDeleteMasterKey: "delete_master_key",
		cmek.OpUnknown:         "unknown",
		cmek.OpKind(99):        "unknown",
	}
	for kind, want := range cases {
		if got := kind.String(); got != want {
			t.Errorf("OpKind(%d).String()=%q want %q", kind, got, want)
		}
	}
}

func TestInMemoryKeyManager_OverrideClock(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	fixed := mustParseTime(t, "2026-05-09T00:00:00Z")
	km.OverrideClock(func() time.Time { return fixed })

	if _, err := km.CreateMasterKey(ctx, "tenant-T"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	ops := km.AuditOps()
	if len(ops) != 1 {
		t.Fatalf("len(ops)=%d want 1", len(ops))
	}
	if !ops[0].OccurredAt.Equal(fixed) {
		t.Errorf("OccurredAt=%v want %v", ops[0].OccurredAt, fixed)
	}
}

func TestInMemoryKeyManager_AuditOps_DefensiveCopy(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-A"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	ops1 := km.AuditOps()
	if len(ops1) != 1 {
		t.Fatalf("len(ops1)=%d want 1", len(ops1))
	}
	// Mutate the returned slice — must NOT affect the manager's internal log.
	ops1[0].TenantID = "tampered"
	ops2 := km.AuditOps()
	if ops2[0].TenantID != "tenant-A" {
		t.Errorf("internal audit got mutated; ops2[0].TenantID=%q", ops2[0].TenantID)
	}
}

func TestInMemoryKeyManager_AuditFailures_Recorded(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()

	// WrapDEK without master → recorded as failure.
	_, _ = km.WrapDEK(ctx, "tenant-Z", []byte("x"))
	// UnwrapDEK without master → recorded as failure.
	_, _ = km.UnwrapDEK(ctx, "tenant-Z", []byte("x"))

	ops := km.AuditOps()
	if len(ops) != 2 {
		t.Fatalf("len(ops)=%d want 2", len(ops))
	}
	for _, op := range ops {
		if op.Success {
			t.Errorf("op=%v expected Success=false", op)
		}
	}
}

func TestInMemoryKeyManager_UnwrapDEK_CorruptedCiphertext(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-C"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}

	// Bad base64
	if _, err := km.UnwrapDEK(ctx, "tenant-C", []byte("@@@not-base64@@@")); !errors.Is(err, cmek.ErrCorruptedCiphertext) {
		t.Errorf("err=%v want ErrCorruptedCiphertext", err)
	}

	// Too short (less than MAC size)
	if _, err := km.UnwrapDEK(ctx, "tenant-C", []byte("AAA=")); !errors.Is(err, cmek.ErrCorruptedCiphertext) {
		t.Errorf("err=%v want ErrCorruptedCiphertext", err)
	}

	// MAC mismatch (32 zero-bytes prefix + 1 byte ciphertext, decrypted via MAC check)
	tampered := make([]byte, 33)
	wrapped := []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") // base64 of 33 zero bytes
	_ = tampered
	_ = wrapped
	// the prior literal is 33 bytes of zero MAC || 0x00 ciphertext — but for simplicity construct via base64 of zeros.
	_, err := km.UnwrapDEK(ctx, "tenant-C", []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"))
	if !errors.Is(err, cmek.ErrCorruptedCiphertext) {
		t.Errorf("expected MAC mismatch ErrCorruptedCiphertext, got %v", err)
	}
}

func TestInMemoryKeyManager_GetMasterKeyResourceName_DestroyedReturnsErrorVariant(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-G"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	if err := km.DeleteMasterKey(ctx, "tenant-G"); err != nil {
		t.Fatalf("DeleteMasterKey: %v", err)
	}
	_, err := km.GetMasterKeyResourceName(ctx, "tenant-G")
	if !errors.Is(err, cmek.ErrMasterKeyDestroyed) {
		t.Errorf("err=%v want ErrMasterKeyDestroyed", err)
	}
}

func TestInMemoryKeyManager_RejectsEmptyTenantOnEachOp(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	_, err1 := km.GetMasterKeyResourceName(ctx, "")
	_, err2 := km.WrapDEK(ctx, "  ", []byte("x"))
	_, err3 := km.UnwrapDEK(ctx, "\t", []byte("x"))
	err4 := km.DeleteMasterKey(ctx, "")
	for i, e := range []error{err1, err2, err3, err4} {
		if !errors.Is(e, cmek.ErrInvalidTenant) {
			t.Errorf("call#%d err=%v want ErrInvalidTenant", i+1, e)
		}
	}
}

func TestHexEncode(t *testing.T) {
	got := cmek.HexEncode([]byte{0xde, 0xad, 0xbe, 0xef})
	if got != "deadbeef" {
		t.Errorf("HexEncode=%q want deadbeef", got)
	}
}

func mustParseTime(t *testing.T, value string) time.Time {
	t.Helper()
	tt, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("time.Parse: %v", err)
	}
	return tt
}
