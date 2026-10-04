package db

import (
	"testing"
)

func TestPgxSQLBridge_Construction(t *testing.T) {
	t.Parallel()
	// Verifies the constructor surface — a pgxpool.Pool is required for
	// real construction; we only test the nil-rejection path here. Live
	// usage exercised by service-level integration tests.
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil pool")
		}
	}()
	_ = NewPgxSQLBridge(nil)
}
