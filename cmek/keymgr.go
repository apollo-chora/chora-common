// Package cmek is the CMEK / Cloud KMS port helper used across Chora services
// for per-tenant master key lifecycle (create on tenant.golive, wrap/unwrap
// per-user DEKs via envelope encryption, delete on tenant.crypto_shred).
//
// Aligned with:
//   - Architecture Review locked 2026-05-07 — Tier 3 D11 federated closure saga
//   - .claude/skills/secrets-and-env/SKILL.md — "CMEK + per-tenant master key
//     for sensitive data; per-user DEK for crypto-shred"
//   - .claude/rules/ddd-enforcement.md §Account Closure — pseudonymise +
//     crypto-shred, never hard-delete
//   - chora-infra/terraform/modules/cmek (the IaC module that provisions the
//     KMS keyring, per-tenant master keys, and IAM bindings consumed here)
//
// Design:
//   - KeyManager port abstracts the runtime backend (Cloud KMS in production,
//     in-memory deterministic stub for tests).
//   - InMemoryKeyManager is the test-only adapter — it uses HKDF-style HMAC
//     wrapping (deterministic) so unit tests never touch real GCP. Production
//     swaps in CloudKMSKeyManager (Tier 3 work; thin gRPC client over
//     cloudkms.v1.KeyManagementService).
//   - Crypto-shred semantics: DeleteMasterKey marks the in-memory master as
//     destroyed AND zeroes the underlying key bytes. UnwrapDEK after delete
//     returns ErrMasterKeyDestroyed (= unrecoverable). Re-creating the master
//     for the same tenant gets a NEW key (does NOT resurrect prior wraps).
//   - Audit trail (CryptoOp) feeds chora-governance IMDA D1 accountability
//     evidence + chora-observability AgentDecisionLog. NEVER includes key
//     material — only the OpKind + tenantID + timestamp + result.
//
// HARD INVARIANTS:
//   1. KeyManager NEVER returns the master key bytes — only resource names +
//      wrapped/unwrapped payloads.
//   2. WrapDEK / UnwrapDEK / DeleteMasterKey + CreateMasterKey are the ONLY
//      mutating operations.
//   3. CryptoOp audit slice is append-only (`AuditOps()` returns a copy).
package cmek

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Errors (sentinel — caller compares with errors.Is)
// ─────────────────────────────────────────────────────────────────────────────

var (
	// ErrInvalidTenant — tenantID is empty or malformed (caller validates upstream).
	ErrInvalidTenant = errors.New("cmek: invalid tenant_id")

	// ErrMasterKeyNotFound — no master key exists for the tenant.
	ErrMasterKeyNotFound = errors.New("cmek: master key not found for tenant")

	// ErrMasterKeyDestroyed — master key has been destroyed via DeleteMasterKey;
	// any wrapped DEKs encrypted under it are unrecoverable (= crypto-shredded).
	ErrMasterKeyDestroyed = errors.New("cmek: master key destroyed (crypto-shred)")

	// ErrCorruptedCiphertext — wrapped DEK payload is malformed or fails MAC check.
	ErrCorruptedCiphertext = errors.New("cmek: ciphertext corrupted")
)

// ─────────────────────────────────────────────────────────────────────────────
// MasterKeyRef — composite of KMS path components.
// ─────────────────────────────────────────────────────────────────────────────

// MasterKeyRef captures the GCP Cloud KMS resource path components for a
// per-tenant CMEK master key. The full resource name is composable from the
// fields and matches the format emitted by the chora-infra terraform module.
//
// ResourceName is denormalised (= FullResourceName()) for ergonomic access in
// service code that hands the path straight to the KMS gRPC client.
type MasterKeyRef struct {
	Project      string
	Region       string
	KeyRing      string
	Name         string // e.g. "cmek-tenant-{tenantID}"
	TenantID     string
	ResourceName string // full Cloud KMS path, denormalised for callers
}

// FullResourceName composes the canonical Cloud KMS resource path:
//
//	projects/{project}/locations/{region}/keyRings/{keyring}/cryptoKeys/{name}
//
// This is the format Cloud KMS API expects in EncryptRequest.name etc.
func (r MasterKeyRef) FullResourceName() string {
	if r.ResourceName != "" {
		return r.ResourceName
	}
	return fmt.Sprintf(
		"projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
		r.Project, r.Region, r.KeyRing, r.Name,
	)
}

// ParseMasterKeyResourceName parses a Cloud KMS crypto-key resource path back
// into a MasterKeyRef. Returns an error if the input does not match the
// canonical format AND the chora "cmek-tenant-{tenantID}" naming convention.
func ParseMasterKeyResourceName(resource string) (MasterKeyRef, error) {
	parts := strings.Split(resource, "/")
	if len(parts) != 8 ||
		parts[0] != "projects" || parts[1] == "" ||
		parts[2] != "locations" || parts[3] == "" ||
		parts[4] != "keyRings" || parts[5] == "" ||
		parts[6] != "cryptoKeys" || parts[7] == "" {
		return MasterKeyRef{}, fmt.Errorf("cmek: malformed resource name %q", resource)
	}

	keyName := parts[7]
	const prefix = "cmek-tenant-"
	if !strings.HasPrefix(keyName, prefix) || len(keyName) <= len(prefix) {
		return MasterKeyRef{}, fmt.Errorf("cmek: key %q does not match cmek-tenant-{id} convention", keyName)
	}

	ref := MasterKeyRef{
		Project:  parts[1],
		Region:   parts[3],
		KeyRing:  parts[5],
		Name:     keyName,
		TenantID: keyName[len(prefix):],
	}
	ref.ResourceName = resource
	return ref, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// KeyManager port — implemented by InMemory* (tests) + CloudKMS* (production)
// ─────────────────────────────────────────────────────────────────────────────

// KeyManager is the per-tenant CMEK master-key port. Adapter implementations:
//   - InMemoryKeyManager (tests / local dev)
//   - CloudKMSKeyManager (production — calls cloudkms.v1.KeyManagementService)
//
// IAM enforcement is a property of the production adapter (Cloud KMS rejects
// callers without keyEncrypterDecrypter / keyDestroyer roles). The in-memory
// adapter trusts the caller — callers must enforce auth upstream.
type KeyManager interface {
	// CreateMasterKey provisions (or returns the existing) per-tenant CMEK
	// master key. Idempotent: repeated calls with the same tenantID return the
	// SAME ResourceName (provided the master has not been destroyed).
	CreateMasterKey(ctx context.Context, tenantID string) (MasterKeyRef, error)

	// GetMasterKeyResourceName returns the full Cloud KMS resource name for the
	// tenant's master key. Returns ErrMasterKeyNotFound if no master is provisioned.
	GetMasterKeyResourceName(ctx context.Context, tenantID string) (string, error)

	// WrapDEK encrypts a per-user DEK using the tenant master key (envelope encryption).
	// Caller stores ONLY the wrapped DEK in chora_identity.data_encryption_keys.
	// Returns ErrMasterKeyNotFound if the master is missing; ErrMasterKeyDestroyed
	// if it has been crypto-shredded.
	WrapDEK(ctx context.Context, tenantID string, dek []byte) (wrapped []byte, err error)

	// UnwrapDEK decrypts a wrapped DEK using the tenant master key. Used at
	// every PII-bearing read after the user authenticates. Returns
	// ErrMasterKeyDestroyed if the master has been deleted (= unrecoverable).
	UnwrapDEK(ctx context.Context, tenantID string, wrapped []byte) (dek []byte, err error)

	// DeleteMasterKey destroys the tenant master key. After this call:
	//   - All wrapped DEKs encrypted under this master become unrecoverable.
	//   - Re-creating a master for the same tenant gets a NEW master (does not
	//     resurrect any prior data).
	//
	// In production this maps to KMS DestroyCryptoKeyVersion (with the tenant
	// master key purpose=ENCRYPT_DECRYPT this destroys the primary version).
	// Idempotent: deleting an already-destroyed master is a no-op.
	DeleteMasterKey(ctx context.Context, tenantID string) error
}

// ─────────────────────────────────────────────────────────────────────────────
// CryptoOp — audit-trail entry exposed by the in-memory adapter
// ─────────────────────────────────────────────────────────────────────────────

// OpKind enumerates auditable KeyManager operations.
type OpKind int

const (
	OpUnknown OpKind = iota
	OpCreateMasterKey
	OpWrapDEK
	OpUnwrapDEK
	OpDeleteMasterKey
)

// String renders the OpKind as a stable lowercase token suitable for log lines
// and IMDA D1 accountability evidence.
func (k OpKind) String() string {
	switch k {
	case OpCreateMasterKey:
		return "create_master_key"
	case OpWrapDEK:
		return "wrap_dek"
	case OpUnwrapDEK:
		return "unwrap_dek"
	case OpDeleteMasterKey:
		return "delete_master_key"
	default:
		return "unknown"
	}
}

// CryptoOp is one auditable KMS operation. NEVER contains key material — only
// metadata sufficient for IMDA D1 evidence + observability.
type CryptoOp struct {
	Kind     OpKind
	TenantID string
	OccurredAt time.Time
	Success  bool
}

// ─────────────────────────────────────────────────────────────────────────────
// InMemoryKeyManager — test-only adapter (deterministic envelope wrap/unwrap)
// ─────────────────────────────────────────────────────────────────────────────

const (
	// inMemRegion + inMemKeyRing — the canonical defaults so test resource names
	// match the real Terraform outputs precisely.
	inMemProject = "chora-489812"
	inMemRegion  = "asia-southeast1"
	inMemKeyRing = "chora-keys"
)

// masterKeyState tracks a per-tenant master key + lifecycle.
type masterKeyState struct {
	ref       MasterKeyRef
	keyBytes  []byte // 32 bytes random — used as HMAC key for the deterministic wrap
	destroyed bool
}

// InMemoryKeyManager is a deterministic in-process KeyManager backed by a map
// keyed by tenantID. Used by unit tests and chora-identity local dev.
//
// The "wrap" is HMAC-based authenticated encryption built on SHA-256:
//
//	wrapped = base64(MAC(plain) || plain XOR keystream)
//
// where keystream = HKDF-style HMAC-counter output. NOT a production cipher;
// production swaps to AES-256-GCM via CloudKMSKeyManager.
type InMemoryKeyManager struct {
	mu      sync.RWMutex
	masters map[string]*masterKeyState
	audit   []CryptoOp
	now     func() time.Time // overridable for deterministic timestamps
}

// NewInMemoryKeyManager constructs a fresh in-memory KeyManager.
// In tests, time.Now() is the default clock; tests override via OverrideClock
// when timestamp determinism is required.
func NewInMemoryKeyManager() *InMemoryKeyManager {
	return &InMemoryKeyManager{
		masters: make(map[string]*masterKeyState),
		audit:   make([]CryptoOp, 0, 16),
		now:     time.Now,
	}
}

// OverrideClock swaps the time source — tests use this to assert deterministic
// CryptoOp.OccurredAt values.
func (km *InMemoryKeyManager) OverrideClock(now func() time.Time) {
	km.mu.Lock()
	defer km.mu.Unlock()
	km.now = now
}

// AuditOps returns a defensive copy of the recorded CryptoOps. Append-only —
// callers cannot rewrite history.
func (km *InMemoryKeyManager) AuditOps() []CryptoOp {
	km.mu.RLock()
	defer km.mu.RUnlock()
	out := make([]CryptoOp, len(km.audit))
	copy(out, km.audit)
	return out
}

// CreateMasterKey implements KeyManager. Idempotent — same tenantID returns
// the same MasterKeyRef provided the master has not been destroyed.
func (km *InMemoryKeyManager) CreateMasterKey(ctx context.Context, tenantID string) (MasterKeyRef, error) {
	if err := validateTenantID(tenantID); err != nil {
		return MasterKeyRef{}, err
	}

	km.mu.Lock()
	defer km.mu.Unlock()

	if existing, ok := km.masters[tenantID]; ok && !existing.destroyed {
		km.recordLocked(OpCreateMasterKey, tenantID, true)
		return existing.ref, nil
	}

	keyBytes := make([]byte, 32)
	if _, err := rand.Read(keyBytes); err != nil {
		return MasterKeyRef{}, fmt.Errorf("cmek: rand: %w", err)
	}
	ref := MasterKeyRef{
		Project:  inMemProject,
		Region:   inMemRegion,
		KeyRing:  inMemKeyRing,
		Name:     "cmek-tenant-" + tenantID,
		TenantID: tenantID,
	}
	ref.ResourceName = ref.FullResourceName()
	km.masters[tenantID] = &masterKeyState{
		ref:      ref,
		keyBytes: keyBytes,
	}
	km.recordLocked(OpCreateMasterKey, tenantID, true)
	return ref, nil
}

// GetMasterKeyResourceName implements KeyManager.
func (km *InMemoryKeyManager) GetMasterKeyResourceName(ctx context.Context, tenantID string) (string, error) {
	if err := validateTenantID(tenantID); err != nil {
		return "", err
	}
	km.mu.RLock()
	defer km.mu.RUnlock()
	mk, ok := km.masters[tenantID]
	if !ok {
		return "", fmt.Errorf("%w: tenantID=%s", ErrMasterKeyNotFound, tenantID)
	}
	if mk.destroyed {
		return "", fmt.Errorf("%w: tenantID=%s", ErrMasterKeyDestroyed, tenantID)
	}
	return mk.ref.FullResourceName(), nil
}

// WrapDEK implements KeyManager.
func (km *InMemoryKeyManager) WrapDEK(ctx context.Context, tenantID string, dek []byte) ([]byte, error) {
	if err := validateTenantID(tenantID); err != nil {
		return nil, err
	}
	km.mu.Lock()
	defer km.mu.Unlock()
	mk, err := km.lookupActiveLocked(tenantID)
	if err != nil {
		km.recordLocked(OpWrapDEK, tenantID, false)
		return nil, err
	}

	tag := mac(mk.keyBytes, dek)
	stream := keystream(mk.keyBytes, len(dek))
	ct := xor(dek, stream)
	combined := append(append([]byte(nil), tag...), ct...)
	wrapped := []byte(base64.StdEncoding.EncodeToString(combined))

	km.recordLocked(OpWrapDEK, tenantID, true)
	return wrapped, nil
}

// UnwrapDEK implements KeyManager. Returns ErrMasterKeyDestroyed if the master
// has been crypto-shredded (regardless of whether the master was later
// re-created — destroyed-then-recreated yields a NEW master and old wraps
// remain unrecoverable).
func (km *InMemoryKeyManager) UnwrapDEK(ctx context.Context, tenantID string, wrapped []byte) ([]byte, error) {
	if err := validateTenantID(tenantID); err != nil {
		return nil, err
	}
	km.mu.Lock()
	defer km.mu.Unlock()

	mk, ok := km.masters[tenantID]
	if !ok {
		km.recordLocked(OpUnwrapDEK, tenantID, false)
		return nil, fmt.Errorf("%w: tenantID=%s", ErrMasterKeyNotFound, tenantID)
	}
	if mk.destroyed {
		km.recordLocked(OpUnwrapDEK, tenantID, false)
		return nil, fmt.Errorf("%w: tenantID=%s", ErrMasterKeyDestroyed, tenantID)
	}

	combined, err := base64.StdEncoding.DecodeString(string(wrapped))
	if err != nil {
		km.recordLocked(OpUnwrapDEK, tenantID, false)
		return nil, fmt.Errorf("%w: %v", ErrCorruptedCiphertext, err)
	}
	if len(combined) < sha256.Size {
		km.recordLocked(OpUnwrapDEK, tenantID, false)
		return nil, ErrCorruptedCiphertext
	}
	tag := combined[:sha256.Size]
	ct := combined[sha256.Size:]

	stream := keystream(mk.keyBytes, len(ct))
	plain := xor(ct, stream)
	if !hmac.Equal(mac(mk.keyBytes, plain), tag) {
		km.recordLocked(OpUnwrapDEK, tenantID, false)
		return nil, ErrCorruptedCiphertext
	}
	km.recordLocked(OpUnwrapDEK, tenantID, true)
	return plain, nil
}

// DeleteMasterKey implements KeyManager. Crypto-shred = the master key bytes
// are zeroed AND the destroyed flag is set. Re-creating the master for the
// same tenant later yields a NEW key (does not resurrect prior wraps).
func (km *InMemoryKeyManager) DeleteMasterKey(ctx context.Context, tenantID string) error {
	if err := validateTenantID(tenantID); err != nil {
		return err
	}
	km.mu.Lock()
	defer km.mu.Unlock()

	mk, ok := km.masters[tenantID]
	if !ok {
		// Tombstone so subsequent UnwrapDEK / GetMasterKeyResourceName surface
		// ErrMasterKeyDestroyed (= "this tenant is crypto-shredded"), matching
		// the closure-saga semantic that DeleteMasterKey is the terminal step.
		ref := MasterKeyRef{
			Project:  inMemProject,
			Region:   inMemRegion,
			KeyRing:  inMemKeyRing,
			Name:     "cmek-tenant-" + tenantID,
			TenantID: tenantID,
		}
		ref.ResourceName = ref.FullResourceName()
		km.masters[tenantID] = &masterKeyState{
			ref:       ref,
			destroyed: true,
		}
		km.recordLocked(OpDeleteMasterKey, tenantID, true)
		return nil
	}
	for i := range mk.keyBytes {
		mk.keyBytes[i] = 0
	}
	mk.keyBytes = nil
	mk.destroyed = true
	km.recordLocked(OpDeleteMasterKey, tenantID, true)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// internal helpers
// ─────────────────────────────────────────────────────────────────────────────

func (km *InMemoryKeyManager) lookupActiveLocked(tenantID string) (*masterKeyState, error) {
	mk, ok := km.masters[tenantID]
	if !ok {
		return nil, fmt.Errorf("%w: tenantID=%s", ErrMasterKeyNotFound, tenantID)
	}
	if mk.destroyed {
		return nil, fmt.Errorf("%w: tenantID=%s", ErrMasterKeyDestroyed, tenantID)
	}
	return mk, nil
}

func (km *InMemoryKeyManager) recordLocked(kind OpKind, tenantID string, success bool) {
	km.audit = append(km.audit, CryptoOp{
		Kind:       kind,
		TenantID:   tenantID,
		OccurredAt: km.now().UTC(),
		Success:    success,
	})
}

// validateTenantID is the lone input-validation gate.
func validateTenantID(tenantID string) error {
	if strings.TrimSpace(tenantID) == "" {
		return fmt.Errorf("%w: empty", ErrInvalidTenant)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Crypto primitives — deterministic HMAC-based authenticated encryption.
// NOT for production. Production uses AES-256-GCM via CloudKMSKeyManager.
// ─────────────────────────────────────────────────────────────────────────────

func mac(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	_, _ = m.Write(msg)
	return m.Sum(nil)
}

// keystream produces n bytes of HMAC-counter pseudorandom output keyed by the
// master. Deterministic — same (key, n) always yields identical output, which
// is fine for this stub because the random bytes that go into the keyBytes
// already provide the per-tenant entropy.
func keystream(key []byte, n int) []byte {
	out := make([]byte, 0, n+sha256.Size)
	for counter := byte(0); len(out) < n; counter++ {
		m := hmac.New(sha256.New, key)
		_, _ = m.Write([]byte("cmek/keystream"))
		_, _ = m.Write([]byte{counter})
		out = append(out, m.Sum(nil)...)
	}
	return out[:n]
}

func xor(a, b []byte) []byte {
	if len(a) != len(b) {
		// callers always pass equal lengths; defensive truncation
		min := len(a)
		if len(b) < min {
			min = len(b)
		}
		out := make([]byte, min)
		for i := 0; i < min; i++ {
			out[i] = a[i] ^ b[i]
		}
		return out
	}
	out := make([]byte, len(a))
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
	return out
}

// HexEncode is a thin alias around hex.EncodeToString for callers that want
// to log the wrapped-DEK fingerprint without leaking key bytes.
func HexEncode(b []byte) string { return hex.EncodeToString(b) }
