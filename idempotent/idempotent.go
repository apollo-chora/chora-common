// Package idempotent provides a Postgres-friendly idempotency-key store + a
// Process(...) helper for at-least-once Pub/Sub subscribers.
//
// Per CLAUDE.md §6 ("Pub/Sub at-least-once delivery means subscribers WILL
// receive duplicates") + the data-consistency skill ("Always check before
// processing"), every subscriber MUST de-duplicate by event_id (or business
// idempotency_key) before applying the event side effect.
//
// Two operating modes are exposed:
//
//   - Process(ctx, key, ttl, fn) — high-level "claim, run, persist" wrapper.
//     First call within TTL: runs fn; if fn succeeds, claims the key for TTL.
//     Subsequent calls within TTL: skips fn entirely; returns nil.
//     If fn errors: the key is NOT claimed, allowing retry.
//
//   - Seen(ctx, key) + Mark(ctx, key, ttl) — low-level primitives for
//     subscribers that need to interleave the dedup with their own DB
//     transaction (e.g. mark-seen + apply-side-effect in one txn).
//
// In-memory implementation (NewMemoryStore) is the test/dev double. The
// Postgres implementation (NewPostgresStore) is the production default,
// backed by an `idempotency_keys(key text PRIMARY KEY, processed_at,
// ttl_at)` table. See package readme for the migration template.
//
// TTL cleanup is owned by a separate Cloud Run Job that calls
// store.CleanupExpired(ctx) on a schedule (typical: hourly).
package idempotent

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Store is the idempotency contract. Implementations: MemoryStore (dev/test),
// PostgresStore (production).
type Store interface {
	// Process claims the key, runs fn, and on success records the key for
	// the supplied TTL. Subsequent calls with the same key within TTL skip
	// fn and return nil (idempotent re-delivery).
	//
	// On fn error, the key is NOT claimed, so retries will run fn again.
	// On context cancellation, behaviour is implementation-defined; the
	// MemoryStore aborts the in-flight call.
	Process(ctx context.Context, key string, ttl time.Duration, fn func() error) error

	// Seen reports whether the key has been recorded and is still within
	// its TTL.
	Seen(ctx context.Context, key string) (bool, error)

	// Mark records the key for the supplied TTL. Idempotent itself —
	// calling Mark twice with the same key is a no-op except for refreshing
	// the TTL.
	Mark(ctx context.Context, key string, ttl time.Duration) error

	// CleanupExpired removes records whose ttl_at is in the past. Returns
	// the number of records removed. Designed to be called from a periodic
	// Cloud Run Job. Safe to call concurrently with Process / Mark / Seen.
	CleanupExpired(ctx context.Context) (int, error)
}

// Clock is the minimal time-source abstraction used by MemoryStore for
// deterministic TTL tests. Production code uses RealClock{}.
type Clock interface {
	Now() time.Time
}

// RealClock returns time.Now().UTC().
type RealClock struct{}

// Now returns the current time in UTC.
func (RealClock) Now() time.Time { return time.Now().UTC() }

// ----------------------------------------------------------------------------
// In-memory store (dev / test default). Goroutine-safe.
// ----------------------------------------------------------------------------

// MemoryStore is an in-memory Store implementation. Used in unit tests and
// dev-time service binaries that don't yet have a Postgres connection.
type MemoryStore struct {
	mu      sync.Mutex
	records map[string]time.Time // key -> ttl_at
	clock   Clock
	offset  time.Duration // for tests: synthetic clock advance
	// inflight de-duplicates concurrent Process calls for the same key
	// before any of them mark the key. First goroutine acquires the slot;
	// subsequent goroutines wait for it to release.
	inflight map[string]chan struct{}
}

// NewMemoryStore constructs a fresh in-memory store using a real clock.
func NewMemoryStore() *MemoryStore {
	return NewMemoryStoreWithClock(RealClock{})
}

// NewMemoryStoreWithClock constructs an in-memory store with an injected
// clock. Useful for TTL tests.
func NewMemoryStoreWithClock(c Clock) *MemoryStore {
	return &MemoryStore{
		records:  make(map[string]time.Time),
		clock:    c,
		inflight: make(map[string]chan struct{}),
	}
}

// Advance is a TEST helper to artificially move the clock forward without
// changing the underlying Clock implementation. Ignored in production
// flows (production uses RealClock and never calls this).
func (s *MemoryStore) Advance(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offset += d
}

func (s *MemoryStore) now() time.Time {
	return s.clock.Now().Add(s.offset)
}

// Seen returns true iff the key has been recorded and not yet TTL-expired.
func (s *MemoryStore) Seen(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	expiry, ok := s.records[key]
	if !ok {
		return false, nil
	}
	if !s.now().Before(expiry) {
		// expired
		delete(s.records, key)
		return false, nil
	}
	return true, nil
}

// Mark records the key for the supplied TTL.
func (s *MemoryStore) Mark(_ context.Context, key string, ttl time.Duration) error {
	if key == "" {
		return errors.New("idempotent: key must not be empty")
	}
	if ttl <= 0 {
		return errors.New("idempotent: ttl must be > 0")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[key] = s.now().Add(ttl)
	return nil
}

// Process implements the high-level claim-run-persist wrapper. See package
// doc for semantics.
func (s *MemoryStore) Process(ctx context.Context, key string, ttl time.Duration, fn func() error) error {
	if key == "" {
		return errors.New("idempotent: key must not be empty")
	}
	if ttl <= 0 {
		return errors.New("idempotent: ttl must be > 0")
	}
	if fn == nil {
		return errors.New("idempotent: fn must not be nil")
	}

	for {
		// Fast path: already seen?
		seen, err := s.Seen(ctx, key)
		if err != nil {
			return err
		}
		if seen {
			return nil
		}

		// Slow path: try to claim the inflight slot.
		s.mu.Lock()
		ch, busy := s.inflight[key]
		if !busy {
			ch = make(chan struct{})
			s.inflight[key] = ch
			s.mu.Unlock()

			// We own the slot. Run fn outside the lock.
			runErr := fn()

			s.mu.Lock()
			if runErr == nil {
				s.records[key] = s.now().Add(ttl)
			}
			delete(s.inflight, key)
			close(ch) // wake waiters
			s.mu.Unlock()

			return runErr
		}
		s.mu.Unlock()

		// Another goroutine is running fn for this key; wait for it.
		select {
		case <-ch:
			// Loop back: the winner either succeeded (we'll observe Seen=true)
			// or failed (we'll re-attempt to claim the slot ourselves).
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// CleanupExpired removes TTL-expired records.
func (s *MemoryStore) CleanupExpired(_ context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	removed := 0
	for k, expiry := range s.records {
		if !now.Before(expiry) {
			delete(s.records, k)
			removed++
		}
	}
	return removed, nil
}

// Compile-time check.
var _ Store = (*MemoryStore)(nil)
