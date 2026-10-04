// Package idempotent — supplementary Postgres Process Seen-error test.
package idempotent_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
)

// TestPostgresStore_Process_PropagatesSeenError — a query failure inside
// Process's Seen step must bubble up verbatim (no silent re-execution).
func TestPostgresStore_Process_PropagatesSeenError(t *testing.T) {
	wantErr := errors.New("select failed")
	stub := &sqlStub{
		queryHandler: func(_ string, _ []driver.Value) (*sqlRows, error) {
			return nil, wantErr
		},
	}
	store := idempotent.NewPostgresStore(stub)
	err := store.Process(context.Background(), "k", time.Hour, func() error { return nil })
	if !errors.Is(err, wantErr) {
		t.Fatalf("Process err = %v, want propagated Seen error", err)
	}
}
