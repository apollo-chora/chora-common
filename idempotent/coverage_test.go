package idempotent_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
)

func TestMemoryStore_RealClock_Now(t *testing.T) {
	c := idempotent.RealClock{}
	if c.Now().IsZero() {
		t.Error("RealClock.Now zero")
	}
}

func TestMemoryStore_Mark_RejectsEmptyKey(t *testing.T) {
	s := idempotent.NewMemoryStore()
	if err := s.Mark(context.Background(), "", time.Hour); err == nil {
		t.Error("expected error on empty key")
	}
}

func TestMemoryStore_Mark_RejectsZeroTTL(t *testing.T) {
	s := idempotent.NewMemoryStore()
	if err := s.Mark(context.Background(), "k", 0); err == nil {
		t.Error("expected error on zero ttl")
	}
}

func TestMemoryStore_Process_RejectsBlankKey(t *testing.T) {
	s := idempotent.NewMemoryStore()
	err := s.Process(context.Background(), "", time.Hour, func() error { return nil })
	if err == nil {
		t.Error("expected error on empty key")
	}
}

func TestMemoryStore_Process_RejectsZeroTTL(t *testing.T) {
	s := idempotent.NewMemoryStore()
	err := s.Process(context.Background(), "k", 0, func() error { return nil })
	if err == nil {
		t.Error("expected error on zero ttl")
	}
}

func TestMemoryStore_Process_RejectsNilFn(t *testing.T) {
	s := idempotent.NewMemoryStore()
	err := s.Process(context.Background(), "k", time.Hour, nil)
	if err == nil {
		t.Error("expected error on nil fn")
	}
}

func TestMemoryStore_Process_ContextCancelDuringWait(t *testing.T) {
	s := idempotent.NewMemoryStore()
	// First goroutine holds the slot for 100ms.
	holdDone := make(chan struct{})
	go func() {
		_ = s.Process(context.Background(), "k", time.Hour, func() error {
			defer close(holdDone)
			time.Sleep(50 * time.Millisecond)
			return nil
		})
	}()
	// Wait for goroutine 1 to definitely be in fn.
	time.Sleep(5 * time.Millisecond)
	// Second goroutine: cancel context immediately while waiting.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.Process(ctx, "k", time.Hour, func() error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err=%v want context.Canceled", err)
	}
	<-holdDone
}

func TestPostgresStore_NewWithTableOption(t *testing.T) {
	stub := &sqlStub{}
	s := idempotent.NewPostgresStore(stub, idempotent.WithTable("custom_idempotency"))
	if s == nil {
		t.Fatal("constructor returned nil")
	}
	// Issue Mark to verify the WithTable option propagates.
	stub.execHandler = func(query string, _ []driver.Value) (driver.Result, error) {
		if !contains(query, "custom_idempotency") {
			t.Errorf("query=%q want custom_idempotency", query)
		}
		return driver.RowsAffected(1), nil
	}
	if err := s.Mark(context.Background(), "k", time.Hour); err != nil {
		t.Fatalf("Mark: %v", err)
	}
}

func TestPostgresStore_Mark_RejectsEmptyKey(t *testing.T) {
	stub := &sqlStub{}
	s := idempotent.NewPostgresStore(stub)
	if err := s.Mark(context.Background(), "", time.Hour); err == nil {
		t.Error("expected error on empty key")
	}
}

func TestPostgresStore_Mark_RejectsZeroTTL(t *testing.T) {
	stub := &sqlStub{}
	s := idempotent.NewPostgresStore(stub)
	if err := s.Mark(context.Background(), "k", 0); err == nil {
		t.Error("expected error on zero ttl")
	}
}

func TestPostgresStore_Mark_PropagatesExecError(t *testing.T) {
	stub := &sqlStub{
		execHandler: func(_ string, _ []driver.Value) (driver.Result, error) {
			return nil, errors.New("disk full")
		},
	}
	s := idempotent.NewPostgresStore(stub)
	err := s.Mark(context.Background(), "k", time.Hour)
	if err == nil {
		t.Error("expected propagated exec error")
	}
}

func TestPostgresStore_Seen_PropagatesQueryError(t *testing.T) {
	stub := &sqlStub{
		queryHandler: func(_ string, _ []driver.Value) (*sqlRows, error) {
			return nil, errors.New("net failed")
		},
	}
	s := idempotent.NewPostgresStore(stub)
	if _, err := s.Seen(context.Background(), "k"); err == nil {
		t.Error("expected propagated query error")
	}
}

func TestPostgresStore_Process_RejectsBlankInputs(t *testing.T) {
	stub := &sqlStub{}
	s := idempotent.NewPostgresStore(stub)
	if err := s.Process(context.Background(), "", time.Hour, func() error { return nil }); err == nil {
		t.Error("expected error on empty key")
	}
	if err := s.Process(context.Background(), "k", 0, func() error { return nil }); err == nil {
		t.Error("expected error on zero ttl")
	}
	if err := s.Process(context.Background(), "k", time.Hour, nil); err == nil {
		t.Error("expected error on nil fn")
	}
}

func TestPostgresStore_Process_PropagatesFnError(t *testing.T) {
	stub := &sqlStub{
		queryHandler: func(_ string, _ []driver.Value) (*sqlRows, error) {
			return newRows([]string{"key"}, nil), nil
		},
	}
	s := idempotent.NewPostgresStore(stub)
	wantErr := errors.New("biz logic")
	err := s.Process(context.Background(), "k", time.Hour, func() error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("err=%v want wantErr", err)
	}
}

func TestPostgresStore_CleanupExpired_PropagatesScanError(t *testing.T) {
	stub := &sqlStub{
		queryHandler: func(_ string, _ []driver.Value) (*sqlRows, error) {
			// Empty rows -> Scan returns "no current row".
			return newRows([]string{"removed"}, nil), nil
		},
	}
	s := idempotent.NewPostgresStore(stub)
	if _, err := s.CleanupExpired(context.Background()); err == nil {
		t.Error("expected scan error on empty rows")
	}
}

// TestMemoryStore_Process_ConcurrentRetryAfterFailure: when one goroutine
// fails fn, a concurrent waiter should retry fn itself rather than
// observing an empty-key state.
func TestMemoryStore_Process_RetryAfterFailure(t *testing.T) {
	s := idempotent.NewMemoryStore()
	var attempts int32
	fn := func() error {
		n := atomic.AddInt32(&attempts, 1)
		if n == 1 {
			return errors.New("first fails")
		}
		return nil
	}
	// First call fails.
	if err := s.Process(context.Background(), "k", time.Hour, fn); err == nil {
		t.Error("expected first call to fail")
	}
	// Second call succeeds.
	if err := s.Process(context.Background(), "k", time.Hour, fn); err != nil {
		t.Errorf("second call err=%v want nil", err)
	}
	if atomic.LoadInt32(&attempts) != 2 {
		t.Errorf("attempts=%d want 2", attempts)
	}
}
