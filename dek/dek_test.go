// Package dek tests — RED-phase first per .claude/rules/development-execution.md.
//
// DEK lifecycle (per .claude/rules/ddd-enforcement.md §Account Closure):
//   - Generate 32-byte random DEK on GCID issuance
//   - Wrap with tenant master key (CMEK envelope encryption)
//   - Store wrapped + status only — never plaintext
//   - Rotate: scheduled (Cloud Run Job) — old DEK marked rotated, new one generated
//   - Crypto-shred: DELETE row → unrecoverable
//
// Coverage gate: 85% domain.
package dek_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/cmek"
	"github.com/apollo-chora/chora-common/dek"
)

// ─────────────────────────────────────────────────────────────────────────────
// DEK aggregate construction
// ─────────────────────────────────────────────────────────────────────────────

func TestNewDEK_PopulatesFields(t *testing.T) {
	d, err := dek.New(dek.NewArgs{
		GCID:               "01975a73-9a8b-7e2c-bd11-aa0001000001",
		TenantID:           "tenant-A",
		WrappedKey:         []byte("wrapped-cipher-bytes"),
		KMSKeyResourceName: "projects/chora-489812/locations/asia-southeast1/keyRings/chora-keys/cryptoKeys/cmek-tenant-A",
		WrapAlgorithm:      dek.WrapAlgoCMEKEnvelope,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if d.DEKID == "" {
		t.Error("expected DEKID generated (UUIDv7)")
	}
	if d.Status != dek.StatusActive {
		t.Errorf("Status=%v want StatusActive", d.Status)
	}
	if d.CreatedAt.IsZero() {
		t.Error("expected CreatedAt")
	}
	if !d.RotatedAt.IsZero() {
		t.Error("expected RotatedAt zero on new DEK")
	}
	if !d.ShreddedAt.IsZero() {
		t.Error("expected ShreddedAt zero on new DEK")
	}
}

func TestNewDEK_RejectsInvalidArgs(t *testing.T) {
	bad := []dek.NewArgs{
		{}, // empty
		{GCID: "x", TenantID: ""},
		{GCID: "", TenantID: "y"},
		{GCID: "x", TenantID: "y"}, // missing WrappedKey
		{GCID: "x", TenantID: "y", WrappedKey: []byte("w")}, // missing KMSKeyResourceName
	}
	for i, args := range bad {
		_, err := dek.New(args)
		if err == nil {
			t.Errorf("case#%d: expected error", i)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Status transitions
// ─────────────────────────────────────────────────────────────────────────────

func TestDEK_MarkRotated_FromActive(t *testing.T) {
	d := mustNewDEK(t)
	now := time.Now().UTC()
	if err := d.MarkRotated(now); err != nil {
		t.Fatalf("MarkRotated: %v", err)
	}
	if d.Status != dek.StatusRotated {
		t.Errorf("Status=%v want StatusRotated", d.Status)
	}
	if !d.RotatedAt.Equal(now) {
		t.Errorf("RotatedAt=%v want %v", d.RotatedAt, now)
	}
}

func TestDEK_MarkRotated_RejectsTwice(t *testing.T) {
	d := mustNewDEK(t)
	if err := d.MarkRotated(time.Now().UTC()); err != nil {
		t.Fatalf("first MarkRotated: %v", err)
	}
	if err := d.MarkRotated(time.Now().UTC()); !errors.Is(err, dek.ErrInvalidTransition) {
		t.Errorf("err=%v want ErrInvalidTransition", err)
	}
}

func TestDEK_MarkShredded_FromActive(t *testing.T) {
	d := mustNewDEK(t)
	now := time.Now().UTC()
	if err := d.MarkShredded(now); err != nil {
		t.Fatalf("MarkShredded: %v", err)
	}
	if d.Status != dek.StatusCryptoShredded {
		t.Errorf("Status=%v want StatusCryptoShredded", d.Status)
	}
	if !d.ShreddedAt.Equal(now) {
		t.Errorf("ShreddedAt=%v want %v", d.ShreddedAt, now)
	}
}

func TestDEK_MarkShredded_FromRotated(t *testing.T) {
	d := mustNewDEK(t)
	if err := d.MarkRotated(time.Now().UTC()); err != nil {
		t.Fatalf("MarkRotated: %v", err)
	}
	now := time.Now().UTC()
	if err := d.MarkShredded(now); err != nil {
		t.Fatalf("MarkShredded after rotated: %v", err)
	}
	if d.Status != dek.StatusCryptoShredded {
		t.Errorf("Status=%v want StatusCryptoShredded", d.Status)
	}
}

func TestDEK_MarkShredded_RejectsTwice(t *testing.T) {
	d := mustNewDEK(t)
	if err := d.MarkShredded(time.Now().UTC()); err != nil {
		t.Fatalf("first MarkShredded: %v", err)
	}
	if err := d.MarkShredded(time.Now().UTC()); !errors.Is(err, dek.ErrInvalidTransition) {
		t.Errorf("err=%v want ErrInvalidTransition", err)
	}
}

func TestDEK_MarkUsed_TouchesLastUsedAt(t *testing.T) {
	d := mustNewDEK(t)
	now := time.Now().UTC()
	d.MarkUsed(now)
	if !d.LastUsedAt.Equal(now) {
		t.Errorf("LastUsedAt=%v want %v", d.LastUsedAt, now)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Status enum stable encoding (DB persistence)
// ─────────────────────────────────────────────────────────────────────────────

func TestStatus_String_StableTokens(t *testing.T) {
	cases := map[dek.Status]string{
		dek.StatusActive:         "active",
		dek.StatusRotated:        "rotated",
		dek.StatusCryptoShredded: "crypto_shredded",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("Status(%d).String()=%q want %q", s, got, want)
		}
	}
}

func TestParseStatus_RoundTrips(t *testing.T) {
	for _, raw := range []string{"active", "rotated", "crypto_shredded"} {
		s, err := dek.ParseStatus(raw)
		if err != nil {
			t.Errorf("ParseStatus(%q): %v", raw, err)
			continue
		}
		if s.String() != raw {
			t.Errorf("roundtrip mismatch: %q → %v → %q", raw, s, s.String())
		}
	}
}

func TestParseStatus_RejectsUnknown(t *testing.T) {
	if _, err := dek.ParseStatus("garbage"); err == nil {
		t.Error("expected error on unknown status")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Service-level: per-user DEK roundtrip via in-memory CMEK + repo
// ─────────────────────────────────────────────────────────────────────────────

func TestService_IssueAndUse_Roundtrip(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-A"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	repo := dek.NewInMemoryRepository()
	svc := dek.NewService(km, repo, nil) // nil = use crypto/rand

	// IssueDEK on GCID issuance
	d, err := svc.IssueDEK(ctx, "01975a73-aaaa-7e2c-bd11-aa0001000001", "tenant-A")
	if err != nil {
		t.Fatalf("IssueDEK: %v", err)
	}
	if d.Status != dek.StatusActive {
		t.Errorf("Status=%v want StatusActive", d.Status)
	}

	// EncryptForUser → Decrypt roundtrip
	plain := []byte("user@example.com")
	ct, err := svc.EncryptForUser(ctx, "01975a73-aaaa-7e2c-bd11-aa0001000001", plain)
	if err != nil {
		t.Fatalf("EncryptForUser: %v", err)
	}
	got, err := svc.DecryptForUser(ctx, "01975a73-aaaa-7e2c-bd11-aa0001000001", ct)
	if err != nil {
		t.Fatalf("DecryptForUser: %v", err)
	}
	if string(got) != string(plain) {
		t.Errorf("roundtrip mismatch: got=%q want=%q", got, plain)
	}
}

func TestService_CryptoShred_RendersDecryptUnrecoverable(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-S"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	repo := dek.NewInMemoryRepository()
	svc := dek.NewService(km, repo, nil)

	gcid := "01975a73-bbbb-7e2c-bd11-aa0001000002"
	if _, err := svc.IssueDEK(ctx, gcid, "tenant-S"); err != nil {
		t.Fatalf("IssueDEK: %v", err)
	}
	plain := []byte("alice@example.com")
	ct, err := svc.EncryptForUser(ctx, gcid, plain)
	if err != nil {
		t.Fatalf("EncryptForUser: %v", err)
	}

	// Crypto-shred — DELETE the DEK row + tombstone status.
	if err := svc.CryptoShred(ctx, gcid); err != nil {
		t.Fatalf("CryptoShred: %v", err)
	}

	// Subsequent decrypt MUST fail.
	if _, err := svc.DecryptForUser(ctx, gcid, ct); err == nil {
		t.Fatal("expected decrypt to fail after crypto-shred — DATA RECOVERED!")
	}
}

func TestService_RotateDEK_GeneratesNewActive(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-R"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	repo := dek.NewInMemoryRepository()
	svc := dek.NewService(km, repo, nil)

	gcid := "01975a73-cccc-7e2c-bd11-aa0001000003"
	original, err := svc.IssueDEK(ctx, gcid, "tenant-R")
	if err != nil {
		t.Fatalf("IssueDEK: %v", err)
	}

	rotated, err := svc.RotateDEK(ctx, gcid)
	if err != nil {
		t.Fatalf("RotateDEK: %v", err)
	}
	if rotated.DEKID == original.DEKID {
		t.Error("rotated DEKID equals original — should be a new key")
	}
	if rotated.Status != dek.StatusActive {
		t.Errorf("rotated Status=%v want StatusActive", rotated.Status)
	}

	// The original DEK must now be marked rotated.
	prior, err := repo.Get(ctx, original.DEKID)
	if err != nil {
		t.Fatalf("Get original: %v", err)
	}
	if prior.Status != dek.StatusRotated {
		t.Errorf("prior.Status=%v want StatusRotated", prior.Status)
	}
}

func TestService_EncryptDecrypt_ErrorsWhenNoActiveDEK(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-N"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	repo := dek.NewInMemoryRepository()
	svc := dek.NewService(km, repo, nil)

	if _, err := svc.EncryptForUser(ctx, "no-such-gcid", []byte("x")); !errors.Is(err, dek.ErrNoActiveDEK) {
		t.Errorf("err=%v want ErrNoActiveDEK", err)
	}
	if _, err := svc.DecryptForUser(ctx, "no-such-gcid", []byte("x")); !errors.Is(err, dek.ErrNoActiveDEK) {
		t.Errorf("err=%v want ErrNoActiveDEK", err)
	}
}

func TestService_IssueDEK_RejectsBadArgs(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	repo := dek.NewInMemoryRepository()
	svc := dek.NewService(km, repo, nil)

	for _, tc := range []struct{ gcid, tenantID string }{
		{"", "t"},
		{"g", ""},
	} {
		if _, err := svc.IssueDEK(ctx, tc.gcid, tc.tenantID); !errors.Is(err, dek.ErrInvalidArgs) {
			t.Errorf("IssueDEK(%q,%q) err=%v want ErrInvalidArgs", tc.gcid, tc.tenantID, err)
		}
	}
}

func TestService_IssueDEK_PropagatesMasterKeyMissing(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	// Note: NO CreateMasterKey call → IssueDEK should fail.
	repo := dek.NewInMemoryRepository()
	svc := dek.NewService(km, repo, nil)
	if _, err := svc.IssueDEK(ctx, "g", "tenant-NONE"); err == nil {
		t.Fatal("expected error when master key missing")
	}
}

func TestService_DecryptForUser_RejectsCorruptedCiphertext(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-K"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	repo := dek.NewInMemoryRepository()
	svc := dek.NewService(km, repo, nil)
	gcid := "01975a73-eeee-7e2c-bd11-aa0001000004"
	if _, err := svc.IssueDEK(ctx, gcid, "tenant-K"); err != nil {
		t.Fatalf("IssueDEK: %v", err)
	}
	// bad base64
	if _, err := svc.DecryptForUser(ctx, gcid, []byte("@@bad-base64@@")); !errors.Is(err, dek.ErrCorruptedCiphertext) {
		t.Errorf("err=%v want ErrCorruptedCiphertext", err)
	}
	// short
	if _, err := svc.DecryptForUser(ctx, gcid, []byte("AAA=")); !errors.Is(err, dek.ErrCorruptedCiphertext) {
		t.Errorf("err=%v want ErrCorruptedCiphertext (short)", err)
	}
	// MAC mismatch — 32 bytes of MAC + 1 byte plaintext, all zero — won't validate
	if _, err := svc.DecryptForUser(ctx, gcid, []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")); !errors.Is(err, dek.ErrCorruptedCiphertext) {
		t.Errorf("expected MAC mismatch ErrCorruptedCiphertext, got %v", err)
	}
}

func TestService_CryptoShred_IdempotentWhenNoActive(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	repo := dek.NewInMemoryRepository()
	svc := dek.NewService(km, repo, nil)
	if err := svc.CryptoShred(ctx, "no-such-gcid"); err != nil {
		t.Errorf("CryptoShred unexpectedly failed when no active DEK: %v", err)
	}
}

func TestRepository_Save_RejectsDuplicateActive(t *testing.T) {
	ctx := context.Background()
	repo := dek.NewInMemoryRepository()
	d1 := mustNewDEK(t)
	d2 := mustNewDEK(t)
	d2.GCID = d1.GCID // same gcid → duplicate active conflict

	if err := repo.Save(ctx, d1); err != nil {
		t.Fatalf("Save d1: %v", err)
	}
	if err := repo.Save(ctx, d2); err == nil {
		t.Fatal("expected duplicate-active error")
	}
}

func TestRepository_Save_AllowsRotatedPlusNewActive(t *testing.T) {
	ctx := context.Background()
	repo := dek.NewInMemoryRepository()
	d1 := mustNewDEK(t)
	if err := repo.Save(ctx, d1); err != nil {
		t.Fatalf("Save d1: %v", err)
	}
	if err := d1.MarkRotated(time.Now().UTC()); err != nil {
		t.Fatalf("MarkRotated: %v", err)
	}
	if err := repo.Save(ctx, d1); err != nil {
		t.Fatalf("Save rotated d1: %v", err)
	}
	// Now a new active DEK for the same gcid should be acceptable.
	d2 := mustNewDEK(t)
	d2.GCID = d1.GCID
	if err := repo.Save(ctx, d2); err != nil {
		t.Errorf("Save d2 after rotation: %v", err)
	}
}

func TestRepository_Get_NotFound(t *testing.T) {
	ctx := context.Background()
	repo := dek.NewInMemoryRepository()
	if _, err := repo.Get(ctx, "missing-id"); !errors.Is(err, dek.ErrDEKNotFound) {
		t.Errorf("err=%v want ErrDEKNotFound", err)
	}
}

func TestRepository_Save_NilRejected(t *testing.T) {
	ctx := context.Background()
	repo := dek.NewInMemoryRepository()
	if err := repo.Save(ctx, nil); err == nil {
		t.Error("expected error for nil DEK")
	}
}

func TestRepository_Delete_Idempotent(t *testing.T) {
	ctx := context.Background()
	repo := dek.NewInMemoryRepository()
	if err := repo.Delete(ctx, "missing"); err != nil {
		t.Errorf("expected idempotent delete, got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

func mustNewDEK(t *testing.T) *dek.DEK {
	t.Helper()
	d, err := dek.New(dek.NewArgs{
		GCID:               "01975a73-9a8b-7e2c-bd11-aa0001000001",
		TenantID:           "tenant-A",
		WrappedKey:         []byte("wrapped-cipher-bytes"),
		KMSKeyResourceName: "projects/chora-489812/locations/asia-southeast1/keyRings/chora-keys/cryptoKeys/cmek-tenant-A",
		WrapAlgorithm:      dek.WrapAlgoCMEKEnvelope,
	})
	if err != nil {
		t.Fatalf("mustNewDEK: %v", err)
	}
	return d
}
