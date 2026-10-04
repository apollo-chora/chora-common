// keymgr_extra_test.go — statement-coverage extension for cmek/keymgr.go:
// the DeleteMasterKey tombstone path for a never-provisioned tenant and
// WrapDEK's destroyed-master rejection via lookupActiveLocked. Test-only;
// does not weaken existing assertions in keymgr_test.go.
package cmek_test

import (
	"context"
	"errors"
	"testing"

	"github.com/5007-Capstone/chora/libs/chora-go-common/cmek"
)

// TestDeleteMasterKey_UnknownTenant_CreatesTombstone exercises the
// no-master branch of DeleteMasterKey: the manager must record a
// destroyed tombstone so every later key operation reports
// ErrMasterKeyDestroyed (the closure-saga "tenant is crypto-shredded"
// invariant) even for a tenant whose master was never provisioned.
func TestDeleteMasterKey_UnknownTenant_CreatesTombstone(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()

	tenant := "tenant-never-created"
	if err := km.DeleteMasterKey(ctx, tenant); err != nil {
		t.Fatalf("DeleteMasterKey on unknown tenant must be a no-op error: %v", err)
	}

	// GetMasterKeyResourceName must now surface destroyed, not not-found.
	if _, err := km.GetMasterKeyResourceName(ctx, tenant); !errors.Is(err, cmek.ErrMasterKeyDestroyed) {
		t.Errorf("GetMasterKeyResourceName err=%v want ErrMasterKeyDestroyed (tombstone)", err)
	}

	// UnwrapDEK: nothing was ever wrapped, but the tenant is shredded.
	if _, err := km.UnwrapDEK(ctx, tenant, []byte("x")); !errors.Is(err, cmek.ErrMasterKeyDestroyed) {
		t.Errorf("UnwrapDEK err=%v want ErrMasterKeyDestroyed (tombstone)", err)
	}

	// The delete was audited as a successful OpDeleteMasterKey (the
	// subsequent UnwrapDEK failure is audited separately).
	ops := km.AuditOps()
	if len(ops) != 2 {
		t.Fatalf("audit len=%d, want 2 (delete + unwrap-failure)", len(ops))
	}
	if ops[0].Kind != cmek.OpDeleteMasterKey || !ops[0].Success {
		t.Errorf("audit[0] = %+v, want successful OpDeleteMasterKey", ops[0])
	}
	if ops[1].Success {
		t.Errorf("audit[1] = %+v, want unwrap failure recorded", ops[1])
	}
}

// TestWrapDEK_AfterDestroy_ReturnsDestroyed drives WrapDEK through
// lookupActiveLocked's destroyed guard (a master that was already
// crypto-shredded must not wrap new material, even after the bytes were
// zeroed).
func TestWrapDEK_AfterDestroy_ReturnsDestroyed(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-wrapped"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	if err := km.DeleteMasterKey(ctx, "tenant-wrapped"); err != nil {
		t.Fatalf("DeleteMasterKey: %v", err)
	}

	if _, err := km.WrapDEK(ctx, "tenant-wrapped", []byte("dek")); !errors.Is(err, cmek.ErrMasterKeyDestroyed) {
		t.Errorf("WrapDEK err=%v want ErrMasterKeyDestroyed", err)
	}
}

// TestRecreateAfterDestroy_YieldsNewKey pins the "new master does NOT
// resurrect prior wraps" invariant: a wrap made under the FIRST master
// cannot be unwrapped after delete+recreate.
func TestRecreateAfterDestroy_YieldsNewKey(t *testing.T) {
	ctx := context.Background()
	km := cmek.NewInMemoryKeyManager()
	if _, err := km.CreateMasterKey(ctx, "tenant-rc"); err != nil {
		t.Fatalf("CreateMasterKey: %v", err)
	}
	wrapped, err := km.WrapDEK(ctx, "tenant-rc", []byte("secret-dek"))
	if err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}
	if err := km.DeleteMasterKey(ctx, "tenant-rc"); err != nil {
		t.Fatalf("DeleteMasterKey: %v", err)
	}
	if _, err := km.CreateMasterKey(ctx, "tenant-rc"); err != nil {
		t.Fatalf("re-create: %v", err)
	}
	if _, err := km.UnwrapDEK(ctx, "tenant-rc", wrapped); !errors.Is(err, cmek.ErrCorruptedCiphertext) {
		t.Errorf("old wrap after re-create err=%v want ErrCorruptedCiphertext (new key cannot validate old MAC)", err)
	}
}
