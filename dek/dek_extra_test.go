// dek_extra_test.go — statement-coverage extension for dek/dek.go: the
// status-enum unknown token, New's default wrap algorithm, and the
// Service error branches (rand failure, unwrap failure, rotate/shred
// repository failures, wrap + New failures via a stub key manager).
// Test-only; does not weaken existing assertions in dek_test.go.
package dek_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/5007-Capstone/chora/libs/chora-go-common/cmek"
	"github.com/5007-Capstone/chora/libs/chora-go-common/dek"
)

// ─────────────────────────────────────────────────────────────────────────────
// Status enum + constructor defaults
// ─────────────────────────────────────────────────────────────────────────────

func TestStatus_String_UnknownToken(t *testing.T) {
	if got := dek.StatusUnknown.String(); got != "unknown" {
		t.Errorf("StatusUnknown.String()=%q want unknown", got)
	}
	if got := dek.Status(99).String(); got != "unknown" {
		t.Errorf("Status(99).String()=%q want unknown", got)
	}
}

func TestNew_DefaultsWrapAlgorithm(t *testing.T) {
	d, err := dek.New(dek.NewArgs{
		GCID:               "gcid-x",
		TenantID:           "tenant-x",
		WrappedKey:         []byte("wrapped"),
		KMSKeyResourceName: "projects/p/locations/r/keyRings/k/cryptoKeys/cmek-tenant-x",
		// WrapAlgorithm intentionally omitted — must default to the CMEK
		// envelope token.
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if d.WrapAlgorithm != dek.WrapAlgoCMEKEnvelope {
		t.Errorf("WrapAlgorithm=%q want %q", d.WrapAlgorithm, dek.WrapAlgoCMEKEnvelope)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Service error branches
// ─────────────────────────────────────────────────────────────────────────────

// failingReader makes io.ReadFull error deterministically inside IssueDEK.
type failingReader struct{ err error }

func (f failingReader) Read([]byte) (int, error) { return 0, f.err }

func TestIssueDEK_RandFailure(t *testing.T) {
	ctx := context.Background()
	km := &stubKeyManager{master: "projects/p/locations/r/keyRings/k/cryptoKeys/cmek-tenant-x"}
	repo := dek.NewInMemoryRepository()
	svc := dek.NewService(km, repo, failingReader{err: errors.New("entropy exhausted")})

	if _, err := svc.IssueDEK(ctx, "gcid", "tenant-x"); err == nil {
		t.Fatal("expected rand failure to abort IssueDEK")
	}
}

// TestIssueDEK_SaveFailure_DuplicateActive covers the "dek: save" branch:
// a second active DEK for the same gcid violates the one-active-per-gcid
// invariant enforced by the repository, so IssueDEK must fail after the
// aggregate was successfully constructed.
func TestIssueDEK_SaveFailure_DuplicateActive(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-x"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	repo := dek.NewInMemoryRepository()
	svc := dek.NewService(km, repo, nil)

	const gcid = "gcid-dupe"
	if _, err := svc.IssueDEK(ctx, gcid, "tenant-x"); err != nil {
		t.Fatalf("first IssueDEK: %v", err)
	}
	if _, err := svc.IssueDEK(ctx, gcid, "tenant-x"); err == nil {
		t.Fatal("second IssueDEK for the same gcid must fail (duplicate active DEK)")
	}
}

func TestEncryptDecrypt_UnwrapFailure(t *testing.T) {
	ctx := context.Background()
	// Real in-memory KeyManager WITHOUT a master for the DEK's tenant: the
	// unwrap step must fail and the service must wrap the error.
	km := cmek.NewInMemoryKeyManager()
	repo := dek.NewInMemoryRepository()
	d := mustNewDEK(t)
	if err := repo.Save(ctx, d); err != nil {
		t.Fatalf("Save: %v", err)
	}
	svc := dek.NewService(km, repo, nil)

	if _, err := svc.EncryptForUser(ctx, d.GCID, []byte("x")); err == nil {
		t.Error("EncryptForUser must fail when the tenant master is missing")
	}
	if _, err := svc.DecryptForUser(ctx, d.GCID, []byte("x")); err == nil {
		t.Error("DecryptForUser must fail when the tenant master is missing")
	}
}

func TestRotateDEK_NoActive(t *testing.T) {
	ctx := context.Background()
	km := &stubKeyManager{master: "projects/p/locations/r/keyRings/k/cryptoKeys/cmek-tenant-x"}
	svc := dek.NewService(km, dek.NewInMemoryRepository(), nil)
	if _, err := svc.RotateDEK(ctx, "no-such-gcid"); !errors.Is(err, dek.ErrNoActiveDEK) {
		t.Errorf("RotateDEK err=%v want ErrNoActiveDEK", err)
	}
}

// flagRepo wraps the in-memory repository with failure switches for the
// rotate/shred save and shred find-error paths.
type flagRepo struct {
	*dek.InMemoryRepository
	failSave bool
	failFind bool
	findErr  error
}

func (r *flagRepo) Save(ctx context.Context, d *dek.DEK) error {
	if r.failSave {
		return errors.New("repo: save failed")
	}
	return r.InMemoryRepository.Save(ctx, d)
}

func (r *flagRepo) FindActiveByGCID(ctx context.Context, gcid string) (*dek.DEK, error) {
	if r.failFind {
		return nil, r.findErr
	}
	return r.InMemoryRepository.FindActiveByGCID(ctx, gcid)
}

func TestRotateDEK_SaveFailure(t *testing.T) {
	ctx := context.Background()
	km := &stubKeyManager{master: "projects/p/locations/r/keyRings/k/cryptoKeys/cmek-tenant-x"}
	repo := &flagRepo{InMemoryRepository: dek.NewInMemoryRepository()}
	svc := dek.NewService(km, repo, nil)
	if _, err := svc.IssueDEK(ctx, "gcid-r", "tenant-x"); err != nil {
		t.Fatalf("IssueDEK: %v", err)
	}
	repo.failSave = true
	if _, err := svc.RotateDEK(ctx, "gcid-r"); err == nil {
		t.Fatal("expected RotateDEK to fail when persisting the rotated row fails")
	}
}

func TestCryptoShred_FindErrorPropagates(t *testing.T) {
	ctx := context.Background()
	km := &stubKeyManager{master: "projects/p/locations/r/keyRings/k/cryptoKeys/cmek-tenant-x"}
	repo := &flagRepo{
		InMemoryRepository: dek.NewInMemoryRepository(),
		failFind:           true,
		findErr:            errors.New("repo: find exploded"),
	}
	svc := dek.NewService(km, repo, nil)
	if err := svc.CryptoShred(ctx, "gcid-s"); err == nil {
		t.Fatal("expected non-ErrNoActiveDEK find error to propagate")
	}
}

func TestCryptoShred_SaveFailure(t *testing.T) {
	ctx := context.Background()
	km := &stubKeyManager{master: "projects/p/locations/r/keyRings/k/cryptoKeys/cmek-tenant-x"}
	repo := &flagRepo{InMemoryRepository: dek.NewInMemoryRepository()}
	svc := dek.NewService(km, repo, nil)
	if _, err := svc.IssueDEK(ctx, "gcid-sh", "tenant-x"); err != nil {
		t.Fatalf("IssueDEK: %v", err)
	}
	repo.failSave = true
	if err := svc.CryptoShred(ctx, "gcid-sh"); err == nil {
		t.Fatal("expected CryptoShred to fail when persisting the shredded row fails")
	}
}

// TestIssueDEK_WrapAndNewFailures drives the wrap-error and New-error
// branches via a stub KeyManager: WrapDEK failing surfaces "dek: wrap",
// and WrapDEK returning empty material makes New reject the aggregate.
func TestIssueDEK_WrapAndNewFailures(t *testing.T) {
	ctx := context.Background()

	t.Run("wrap error propagates", func(t *testing.T) {
		km := &stubKeyManager{
			master:  "projects/p/locations/r/keyRings/k/cryptoKeys/cmek-tenant-x",
			wrapErr: errors.New("kms wrap refused"),
		}
		svc := dek.NewService(km, dek.NewInMemoryRepository(), nil)
		if _, err := svc.IssueDEK(ctx, "gcid", "tenant-x"); err == nil {
			t.Fatal("expected wrap failure to abort IssueDEK")
		}
	})

	t.Run("empty wrapped material fails construction", func(t *testing.T) {
		km := &stubKeyManager{
			master:  "projects/p/locations/r/keyRings/k/cryptoKeys/cmek-tenant-x",
			wrapped: []byte{},
		}
		svc := dek.NewService(km, dek.NewInMemoryRepository(), nil)
		if _, err := svc.IssueDEK(ctx, "gcid", "tenant-x"); !errors.Is(err, dek.ErrInvalidArgs) {
			t.Errorf("IssueDEK err=%v want ErrInvalidArgs (empty wrapped key)", err)
		}
	})
}

// stubKeyManager is a minimal cmek.KeyManager for error-path injection.
type stubKeyManager struct {
	master  string
	wrapErr error
	wrapped []byte
}

func (s *stubKeyManager) CreateMasterKey(context.Context, string) (cmek.MasterKeyRef, error) {
	return cmek.MasterKeyRef{}, nil
}

func (s *stubKeyManager) GetMasterKeyResourceName(context.Context, string) (string, error) {
	return s.master, nil
}

func (s *stubKeyManager) WrapDEK(context.Context, string, []byte) ([]byte, error) {
	if s.wrapErr != nil {
		return nil, s.wrapErr
	}
	if s.wrapped != nil {
		return append([]byte(nil), s.wrapped...), nil
	}
	return []byte("stub-wrapped"), nil
}

func (s *stubKeyManager) UnwrapDEK(context.Context, string, []byte) ([]byte, error) {
	return nil, errors.New("stub: unwrap not wired")
}

func (s *stubKeyManager) DeleteMasterKey(context.Context, string) error { return nil }

var _ io.Reader = failingReader{}
