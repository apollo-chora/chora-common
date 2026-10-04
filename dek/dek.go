// Package dek implements the per-user Data Encryption Key (DEK) lifecycle for
// Chora. Every PII-bearing column in chora_identity (and every other domain
// that holds user-attributable PII) is encrypted with the user's DEK before
// INSERT and decrypted on READ.
//
// Aligned with:
//   - .claude/rules/ddd-enforcement.md §"Account Closure (federated saga,
//     NOT hard-delete)" — pseudonymise + crypto-shred via DEK delete.
//   - Architecture Review locked 2026-05-07 Tier 3 D11 — federated closure saga
//     with CMEK per-tenant master + per-user DEK; crypto-shred = DEK deletion.
//   - .claude/skills/secrets-and-env/SKILL.md — DEK material NEVER inline; CMEK
//     master + DEK both come from Cloud KMS via env-driven config.
//   - chora-infra/terraform/modules/cmek — provisions the IAM that allows only
//     chora-identity SA to wrap and only chora-closure-orchestrator SA to delete.
//
// Schema (chora_identity DB, declared via the Terraform module + SQL migration
// generated alongside this package — see migration 0003 in chora-identity):
//
//	CREATE TABLE data_encryption_keys (
//	    dek_id                    UUID         PRIMARY KEY,
//	    gcid                      UUID         NOT NULL,
//	    tenant_id                 UUID         NOT NULL,
//	    wrapped_key               BYTEA        NOT NULL,
//	    kms_key_resource_name     TEXT         NOT NULL,
//	    wrap_algorithm            TEXT         NOT NULL,
//	    status                    TEXT         NOT NULL CHECK (status IN ('active','rotated','crypto_shredded')),
//	    created_at                TIMESTAMPTZ  NOT NULL DEFAULT now(),
//	    last_used_at              TIMESTAMPTZ,
//	    rotated_at                TIMESTAMPTZ,
//	    shredded_at               TIMESTAMPTZ
//	);
//	-- One active DEK per gcid at a time:
//	CREATE UNIQUE INDEX dek_one_active_per_gcid ON data_encryption_keys(gcid)
//	    WHERE status = 'active';
//
// HARD INVARIANTS:
//  1. The PLAINTEXT DEK never persists — only the wrapped form does.
//  2. Status transitions are STRICTLY one-way: active → rotated → crypto_shredded
//     (and active → crypto_shredded directly).
//  3. CryptoShred deletes the row outright in production — see Repository.Delete.
//  4. EncryptForUser / DecryptForUser unwrap-on-demand and DO NOT cache the
//     plaintext DEK across calls (defence in depth — every call hits CMEK).
package dek

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/5007-Capstone/chora/libs/chora-go-common/cmek"
)

// ─────────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────────

var (
	// ErrInvalidArgs — DEK constructor input failed validation.
	ErrInvalidArgs = errors.New("dek: invalid args")

	// ErrInvalidTransition — illegal status transition.
	ErrInvalidTransition = errors.New("dek: invalid status transition")

	// ErrUnknownStatus — string does not parse to a known status.
	ErrUnknownStatus = errors.New("dek: unknown status")

	// ErrNoActiveDEK — no active DEK exists for the given gcid.
	ErrNoActiveDEK = errors.New("dek: no active DEK for gcid")

	// ErrCorruptedCiphertext — ciphertext fails MAC verification or is malformed.
	ErrCorruptedCiphertext = errors.New("dek: ciphertext corrupted")

	// ErrDEKNotFound — repository lookup returned no row.
	ErrDEKNotFound = errors.New("dek: not found")
)

// ─────────────────────────────────────────────────────────────────────────────
// Status (DB enum)
// ─────────────────────────────────────────────────────────────────────────────

// Status represents the lifecycle stage of a DEK row. Values are stable and
// match the DB enum tokens — never renumber.
type Status int

const (
	StatusUnknown Status = iota
	StatusActive
	StatusRotated
	StatusCryptoShredded
)

// String renders the stable lowercase token. Used for DB persistence and IMDA D1
// audit evidence.
func (s Status) String() string {
	switch s {
	case StatusActive:
		return "active"
	case StatusRotated:
		return "rotated"
	case StatusCryptoShredded:
		return "crypto_shredded"
	default:
		return "unknown"
	}
}

// ParseStatus converts a stable token back to a Status.
func ParseStatus(raw string) (Status, error) {
	switch raw {
	case "active":
		return StatusActive, nil
	case "rotated":
		return StatusRotated, nil
	case "crypto_shredded":
		return StatusCryptoShredded, nil
	default:
		return StatusUnknown, fmt.Errorf("%w: %q", ErrUnknownStatus, raw)
	}
}

// WrapAlgo enumerates the wrapping algorithm. CMEK envelope = production;
// inmem stub for tests is also CMEK envelope (semantically equivalent).
type WrapAlgo string

const (
	WrapAlgoCMEKEnvelope WrapAlgo = "cmek_envelope"
)

// ─────────────────────────────────────────────────────────────────────────────
// DEK aggregate
// ─────────────────────────────────────────────────────────────────────────────

// NewArgs is the constructor input for New.
type NewArgs struct {
	GCID               string
	TenantID           string
	WrappedKey         []byte
	KMSKeyResourceName string
	WrapAlgorithm      WrapAlgo
}

// DEK is the per-user data encryption key aggregate.
type DEK struct {
	DEKID              string
	GCID               string
	TenantID           string
	WrappedKey         []byte
	KMSKeyResourceName string
	WrapAlgorithm      WrapAlgo
	Status             Status
	CreatedAt          time.Time
	LastUsedAt         time.Time
	RotatedAt          time.Time
	ShreddedAt         time.Time
}

// New constructs a fresh DEK in StatusActive. DEKID is auto-generated as UUIDv7.
func New(args NewArgs) (*DEK, error) {
	if strings.TrimSpace(args.GCID) == "" {
		return nil, fmt.Errorf("%w: gcid empty", ErrInvalidArgs)
	}
	if strings.TrimSpace(args.TenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id empty", ErrInvalidArgs)
	}
	if len(args.WrappedKey) == 0 {
		return nil, fmt.Errorf("%w: wrapped_key empty", ErrInvalidArgs)
	}
	if strings.TrimSpace(args.KMSKeyResourceName) == "" {
		return nil, fmt.Errorf("%w: kms_key_resource_name empty", ErrInvalidArgs)
	}
	algo := args.WrapAlgorithm
	if algo == "" {
		algo = WrapAlgoCMEKEnvelope
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("dek: uuid: %w", err)
	}
	return &DEK{
		DEKID:              id.String(),
		GCID:               strings.TrimSpace(args.GCID),
		TenantID:           strings.TrimSpace(args.TenantID),
		WrappedKey:         append([]byte(nil), args.WrappedKey...),
		KMSKeyResourceName: strings.TrimSpace(args.KMSKeyResourceName),
		WrapAlgorithm:      algo,
		Status:             StatusActive,
		CreatedAt:          time.Now().UTC(),
	}, nil
}

// MarkRotated transitions Active → Rotated. Idempotent-once: any subsequent
// MarkRotated call yields ErrInvalidTransition.
func (d *DEK) MarkRotated(at time.Time) error {
	if d.Status != StatusActive {
		return fmt.Errorf("%w: from %v to rotated", ErrInvalidTransition, d.Status)
	}
	d.Status = StatusRotated
	d.RotatedAt = at.UTC()
	return nil
}

// MarkShredded transitions Active|Rotated → CryptoShredded. Terminal.
func (d *DEK) MarkShredded(at time.Time) error {
	if d.Status == StatusCryptoShredded {
		return fmt.Errorf("%w: already shredded", ErrInvalidTransition)
	}
	d.Status = StatusCryptoShredded
	d.ShreddedAt = at.UTC()
	// Hygiene — zero the wrapped key bytes (defence-in-depth).
	for i := range d.WrappedKey {
		d.WrappedKey[i] = 0
	}
	d.WrappedKey = nil
	return nil
}

// MarkUsed updates LastUsedAt. Non-mutating w.r.t. status.
func (d *DEK) MarkUsed(at time.Time) {
	d.LastUsedAt = at.UTC()
}

// ─────────────────────────────────────────────────────────────────────────────
// Repository port + in-memory adapter (for tests + local dev)
// ─────────────────────────────────────────────────────────────────────────────

// Repository persists DEK rows in chora_identity.data_encryption_keys.
//
// Active-uniqueness invariant (= one active DEK per gcid) is enforced at the
// SQL layer via a partial unique index (see schema header above). The
// in-memory adapter mimics that invariant in code.
type Repository interface {
	// Save inserts or updates the DEK row. Active-uniqueness is enforced.
	Save(ctx context.Context, d *DEK) error
	// Get fetches by DEK ID.
	Get(ctx context.Context, dekID string) (*DEK, error)
	// FindActiveByGCID returns the single Active DEK for a gcid (= ErrNoActiveDEK
	// if none).
	FindActiveByGCID(ctx context.Context, gcid string) (*DEK, error)
	// Delete removes the row entirely — used by CryptoShred AFTER a successful
	// MarkShredded transition has been persisted (or in lieu of it, depending
	// on retention policy).
	Delete(ctx context.Context, dekID string) error
}

// InMemoryRepository is the test-only repository. NOT thread-safe-across-procs.
type InMemoryRepository struct {
	mu   sync.RWMutex
	rows map[string]*DEK // keyed by dek_id
}

// NewInMemoryRepository constructs a fresh in-memory DEK repository.
func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{rows: make(map[string]*DEK)}
}

// Save implements Repository.
func (r *InMemoryRepository) Save(ctx context.Context, d *DEK) error {
	if d == nil {
		return errors.New("dek: nil DEK")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Active uniqueness — if d.Status==Active, no other Active row may exist
	// for the same gcid.
	if d.Status == StatusActive {
		for _, other := range r.rows {
			if other.DEKID != d.DEKID && other.GCID == d.GCID && other.Status == StatusActive {
				return fmt.Errorf("dek: duplicate active DEK for gcid=%s", d.GCID)
			}
		}
	}
	// Defensive copy.
	cp := *d
	cp.WrappedKey = append([]byte(nil), d.WrappedKey...)
	r.rows[d.DEKID] = &cp
	return nil
}

// Get implements Repository.
func (r *InMemoryRepository) Get(ctx context.Context, dekID string) (*DEK, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	row, ok := r.rows[dekID]
	if !ok {
		return nil, ErrDEKNotFound
	}
	cp := *row
	cp.WrappedKey = append([]byte(nil), row.WrappedKey...)
	return &cp, nil
}

// FindActiveByGCID implements Repository.
func (r *InMemoryRepository) FindActiveByGCID(ctx context.Context, gcid string) (*DEK, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, row := range r.rows {
		if row.GCID == gcid && row.Status == StatusActive {
			cp := *row
			cp.WrappedKey = append([]byte(nil), row.WrappedKey...)
			return &cp, nil
		}
	}
	return nil, fmt.Errorf("%w: gcid=%s", ErrNoActiveDEK, gcid)
}

// Delete implements Repository.
func (r *InMemoryRepository) Delete(ctx context.Context, dekID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.rows[dekID]; !ok {
		// Idempotent.
		return nil
	}
	delete(r.rows, dekID)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Service — orchestrates CMEK + Repository for the chora-identity hot path.
// ─────────────────────────────────────────────────────────────────────────────

// Service is the per-user DEK lifecycle service. Wired into chora-identity
// (and any other domain holding user-attributable PII) at composition time.
//
// Adapter dependencies:
//   - cmek.KeyManager (Cloud KMS in prod, in-memory in tests)
//   - Repository (Cloud SQL via pgx in prod, in-memory in tests)
//   - rand io.Reader for DEK material — defaults to crypto/rand
type Service struct {
	km   cmek.KeyManager
	repo Repository
	rng  io.Reader
}

// NewService composes the DEK Service. rng=nil falls back to crypto/rand.
func NewService(km cmek.KeyManager, repo Repository, rng io.Reader) *Service {
	if rng == nil {
		rng = rand.Reader
	}
	return &Service{km: km, repo: repo, rng: rng}
}

// IssueDEK is called on GCID issuance: generate 32 bytes random, wrap with the
// tenant master key (CMEK envelope), persist the wrapped form, return the
// active DEK aggregate. The plaintext DEK is NEVER returned by this function.
func (s *Service) IssueDEK(ctx context.Context, gcid, tenantID string) (*DEK, error) {
	if strings.TrimSpace(gcid) == "" {
		return nil, fmt.Errorf("%w: gcid empty", ErrInvalidArgs)
	}
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id empty", ErrInvalidArgs)
	}

	// 1. Pull the tenant's master key resource name.
	master, err := s.km.GetMasterKeyResourceName(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("dek: master key lookup: %w", err)
	}

	// 2. Generate a 32-byte random DEK.
	dekBytes := make([]byte, 32)
	if _, err := io.ReadFull(s.rng, dekBytes); err != nil {
		return nil, fmt.Errorf("dek: rand: %w", err)
	}

	// 3. Wrap the DEK with the tenant master key.
	wrapped, err := s.km.WrapDEK(ctx, tenantID, dekBytes)
	if err != nil {
		return nil, fmt.Errorf("dek: wrap: %w", err)
	}

	// 4. Build the aggregate + persist.
	d, err := New(NewArgs{
		GCID:               gcid,
		TenantID:           tenantID,
		WrappedKey:         wrapped,
		KMSKeyResourceName: master,
		WrapAlgorithm:      WrapAlgoCMEKEnvelope,
	})
	if err != nil {
		return nil, err
	}
	if err := s.repo.Save(ctx, d); err != nil {
		return nil, fmt.Errorf("dek: save: %w", err)
	}

	// 5. Zero the plaintext DEK (defence-in-depth — never persists in memory
	// after this function returns).
	for i := range dekBytes {
		dekBytes[i] = 0
	}
	return d, nil
}

// EncryptForUser encrypts plaintext using the user's active DEK. Symmetric
// MAC-then-XOR using HMAC-SHA256 — semantically equivalent to the CMEK stub's
// internal cipher. Production swaps to AES-256-GCM when wired through a real
// AEAD primitive; algorithm field on DEK row tracks the version.
func (s *Service) EncryptForUser(ctx context.Context, gcid string, plaintext []byte) ([]byte, error) {
	d, err := s.repo.FindActiveByGCID(ctx, gcid)
	if err != nil {
		return nil, err
	}
	dekBytes, err := s.km.UnwrapDEK(ctx, d.TenantID, d.WrappedKey)
	if err != nil {
		return nil, fmt.Errorf("dek: unwrap: %w", err)
	}
	defer zero(dekBytes)

	tag := mac(dekBytes, plaintext)
	stream := keystream(dekBytes, len(plaintext))
	ct := xor(plaintext, stream)
	combined := append(append([]byte(nil), tag...), ct...)
	return []byte(base64.StdEncoding.EncodeToString(combined)), nil
}

// DecryptForUser decrypts ciphertext using the user's active DEK.
func (s *Service) DecryptForUser(ctx context.Context, gcid string, ciphertext []byte) ([]byte, error) {
	d, err := s.repo.FindActiveByGCID(ctx, gcid)
	if err != nil {
		return nil, err
	}
	dekBytes, err := s.km.UnwrapDEK(ctx, d.TenantID, d.WrappedKey)
	if err != nil {
		return nil, fmt.Errorf("dek: unwrap: %w", err)
	}
	defer zero(dekBytes)

	combined, err := base64.StdEncoding.DecodeString(string(ciphertext))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptedCiphertext, err)
	}
	if len(combined) < sha256.Size {
		return nil, ErrCorruptedCiphertext
	}
	tag := combined[:sha256.Size]
	ct := combined[sha256.Size:]

	stream := keystream(dekBytes, len(ct))
	plain := xor(ct, stream)
	if !hmac.Equal(mac(dekBytes, plain), tag) {
		return nil, ErrCorruptedCiphertext
	}
	return plain, nil
}

// RotateDEK marks the user's active DEK as rotated and issues a new active
// DEK. Wraps both operations in a logical retry-safe sequence; production wires
// this in a transactional outbox so the rotated→active swap + downstream
// re-encryption job event publish are atomic per data-consistency skill.
func (s *Service) RotateDEK(ctx context.Context, gcid string) (*DEK, error) {
	prior, err := s.repo.FindActiveByGCID(ctx, gcid)
	if err != nil {
		return nil, err
	}
	if err := prior.MarkRotated(time.Now().UTC()); err != nil {
		return nil, err
	}
	if err := s.repo.Save(ctx, prior); err != nil {
		return nil, fmt.Errorf("dek: save rotated: %w", err)
	}
	// Issue the replacement active DEK.
	return s.IssueDEK(ctx, gcid, prior.TenantID)
}

// CryptoShred is the saga's terminal step — render the user's data
// unrecoverable by deleting the DEK row outright. The closure orchestrator
// also calls cmek.KeyManager.DeleteMasterKey(tenantID) at tenant-level
// crypto-shred; this function is the per-user equivalent.
func (s *Service) CryptoShred(ctx context.Context, gcid string) error {
	d, err := s.repo.FindActiveByGCID(ctx, gcid)
	if errors.Is(err, ErrNoActiveDEK) {
		// Nothing to shred — idempotent.
		return nil
	}
	if err != nil {
		return err
	}
	if err := d.MarkShredded(time.Now().UTC()); err != nil {
		return err
	}
	if err := s.repo.Save(ctx, d); err != nil {
		return fmt.Errorf("dek: save shredded: %w", err)
	}
	// Delete the row outright — the wrapped key bytes were already zeroed in
	// MarkShredded but the row remains in the DB momentarily for audit.
	// Production retention policy may keep the tombstone row for N days; for
	// MVP we delete immediately.
	return s.repo.Delete(ctx, d.DEKID)
}

// ─────────────────────────────────────────────────────────────────────────────
// Crypto primitives (deterministic — same as cmek package).
// Production swaps to AES-256-GCM via a real AEAD primitive.
// ─────────────────────────────────────────────────────────────────────────────

func mac(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	_, _ = m.Write(msg)
	return m.Sum(nil)
}

func keystream(key []byte, n int) []byte {
	out := make([]byte, 0, n+sha256.Size)
	for counter := byte(0); len(out) < n; counter++ {
		m := hmac.New(sha256.New, key)
		_, _ = m.Write([]byte("dek/keystream"))
		_, _ = m.Write([]byte{counter})
		out = append(out, m.Sum(nil)...)
	}
	return out[:n]
}

func xor(a, b []byte) []byte {
	if len(a) != len(b) {
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

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
