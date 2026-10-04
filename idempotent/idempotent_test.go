// Tests for the idempotent-handler library per CLAUDE.md §6 + the
// data-consistency skill. Pub/Sub is at-least-once delivery so subscribers
// MUST de-dupe by event_id (or business idempotency_key) before processing.
package idempotent_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/idempotent"
)

func TestStore_Process_FirstCallRunsFn(t *testing.T) {
	s := idempotent.NewMemoryStore()
	var ran int32
	err := s.Process(context.Background(), "key-1", time.Hour, func() error {
		atomic.AddInt32(&ran, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if atomic.LoadInt32(&ran) != 1 {
		t.Errorf("ran=%d want 1", ran)
	}
}

func TestStore_Process_DuplicateCallSkipsFn(t *testing.T) {
	s := idempotent.NewMemoryStore()
	var ran int32
	fn := func() error {
		atomic.AddInt32(&ran, 1)
		return nil
	}
	if err := s.Process(context.Background(), "key-dup", time.Hour, fn); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	if err := s.Process(context.Background(), "key-dup", time.Hour, fn); err != nil {
		t.Fatalf("second Process: %v", err)
	}
	if got := atomic.LoadInt32(&ran); got != 1 {
		t.Errorf("ran=%d want 1 (idempotent)", got)
	}
}

func TestStore_Process_FnFailureLeavesKeyUnclaimed(t *testing.T) {
	s := idempotent.NewMemoryStore()
	wantErr := errors.New("boom")
	err := s.Process(context.Background(), "key-fail", time.Hour, func() error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Process err=%v want %v", err, wantErr)
	}
	// On retry, fn should be invoked again because the previous attempt
	// failed (no permanent claim).
	var ranTwice int32
	err = s.Process(context.Background(), "key-fail", time.Hour, func() error {
		atomic.AddInt32(&ranTwice, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("retry Process: %v", err)
	}
	if got := atomic.LoadInt32(&ranTwice); got != 1 {
		t.Errorf("ranTwice=%d want 1", got)
	}
}

func TestStore_Process_ConcurrentDuplicateOnlyRunsOnce(t *testing.T) {
	s := idempotent.NewMemoryStore()
	var ran int32
	const N = 32
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			_ = s.Process(context.Background(), "key-concur", time.Hour, func() error {
				time.Sleep(2 * time.Millisecond)
				atomic.AddInt32(&ran, 1)
				return nil
			})
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt32(&ran); got != 1 {
		t.Errorf("ran=%d want 1 across %d concurrent calls", got, N)
	}
}

func TestStore_Seen_ReturnsFalseInitially(t *testing.T) {
	s := idempotent.NewMemoryStore()
	seen, err := s.Seen(context.Background(), "fresh-key")
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Error("Seen=true want false for fresh key")
	}
}

func TestStore_MarkAndSeen(t *testing.T) {
	s := idempotent.NewMemoryStore()
	if err := s.Mark(context.Background(), "k", time.Hour); err != nil {
		t.Fatalf("Mark: %v", err)
	}
	seen, err := s.Seen(context.Background(), "k")
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if !seen {
		t.Error("Seen=false after Mark, want true")
	}
}

func TestStore_TTLExpiry(t *testing.T) {
	s := idempotent.NewMemoryStoreWithClock(fakeClock{
		now: time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC),
	})
	// Mark with 1-hour TTL
	if err := s.Mark(context.Background(), "k-ttl", time.Hour); err != nil {
		t.Fatalf("Mark: %v", err)
	}
	if seen, _ := s.Seen(context.Background(), "k-ttl"); !seen {
		t.Error("Seen pre-expiry should be true")
	}
	// Advance clock past TTL
	s.Advance(2 * time.Hour)
	if seen, _ := s.Seen(context.Background(), "k-ttl"); seen {
		t.Error("Seen post-expiry should be false (TTL elapsed)")
	}
}

func TestStore_CleanupExpired(t *testing.T) {
	s := idempotent.NewMemoryStoreWithClock(fakeClock{
		now: time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC),
	})
	// 3 entries: two short, one long
	_ = s.Mark(context.Background(), "short-1", time.Minute)
	_ = s.Mark(context.Background(), "short-2", time.Minute)
	_ = s.Mark(context.Background(), "long-1", time.Hour)
	s.Advance(5 * time.Minute)
	removed, err := s.CleanupExpired(context.Background())
	if err != nil {
		t.Fatalf("CleanupExpired: %v", err)
	}
	if removed != 2 {
		t.Errorf("removed=%d want 2", removed)
	}
	if seen, _ := s.Seen(context.Background(), "long-1"); !seen {
		t.Error("long-1 should still be seen")
	}
}

// fakeClock is a deterministic clock for TTL tests.
type fakeClock struct{ now time.Time }

func (f fakeClock) Now() time.Time { return f.now }
