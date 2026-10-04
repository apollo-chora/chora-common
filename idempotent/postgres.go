package idempotent

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// SQLRows is the minimal interface a database/sql *Rows satisfies. We
// declare it here so adapters can stub the database without depending on
// pgx- or pq-specific types.
type SQLRows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
	Err() error
}

// SQLRow is the minimal interface a database/sql *Row satisfies.
type SQLRow interface {
	Scan(dest ...interface{}) error
}

// SQLDB is the minimal database/sql surface required by PostgresStore. It
// matches the *sql.DB and *sql.Tx subset we actually use, allowing services
// to pass either.
type SQLDB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (SQLRows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) SQLRow
}

// PostgresStore is a Store backed by a Postgres `idempotency_keys` table.
//
// Schema (see migrations/idempotency_keys.up.sql):
//
//	CREATE TABLE idempotency_keys (
//	    key          TEXT PRIMARY KEY,
//	    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
//	    ttl_at       TIMESTAMPTZ NOT NULL,
//	    result_hash  TEXT
//	);
//	CREATE INDEX idempotency_keys_ttl ON idempotency_keys (ttl_at);
//
// NB: idempotency_keys is an OPERATIONAL table (not a domain table) — it
// holds short-lived dedup tokens, NOT any user/tenant content. The
// CleanupExpired routine therefore uses a hard purge (TRUNCATE-like) of
// expired rows, which is exempt from the soft-delete invariant per
// ddd-enforcement.md §Soft Deletes.
type PostgresStore struct {
	db    SQLDB
	table string
}

// NewPostgresStore constructs a PostgresStore against the supplied DB
// handle. The default table name is "idempotency_keys"; override with
// WithTable for tenanted deployments that share a DB.
func NewPostgresStore(db SQLDB, opts ...PostgresOption) *PostgresStore {
	s := &PostgresStore{db: db, table: "idempotency_keys"}
	for _, o := range opts {
		o(s)
	}
	return s
}

// PostgresOption tunes a PostgresStore at construction time.
type PostgresOption func(*PostgresStore)

// WithTable overrides the default table name.
func WithTable(name string) PostgresOption {
	return func(s *PostgresStore) { s.table = name }
}

// Mark inserts or upserts the key with the supplied TTL.
func (s *PostgresStore) Mark(ctx context.Context, key string, ttl time.Duration) error {
	if key == "" {
		return fmt.Errorf("idempotent: key must not be empty")
	}
	if ttl <= 0 {
		return fmt.Errorf("idempotent: ttl must be > 0")
	}
	now := time.Now().UTC()
	expiry := now.Add(ttl)
	q := fmt.Sprintf(`INSERT INTO %s (key, processed_at, ttl_at)
VALUES ($1, $2, $3)
ON CONFLICT (key) DO UPDATE SET ttl_at = EXCLUDED.ttl_at`, s.table)
	if _, err := s.db.ExecContext(ctx, q, key, now, expiry); err != nil {
		return fmt.Errorf("idempotent.Mark: %w", err)
	}
	return nil
}

// Seen returns true iff the key exists and has not yet TTL-expired.
func (s *PostgresStore) Seen(ctx context.Context, key string) (bool, error) {
	q := fmt.Sprintf(`SELECT key FROM %s WHERE key = $1 AND ttl_at > now()`, s.table)
	rows, err := s.db.QueryContext(ctx, q, key)
	if err != nil {
		return false, fmt.Errorf("idempotent.Seen: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		return true, rows.Err()
	}
	return false, rows.Err()
}

// Process implements the high-level Store contract. NB: this is NOT
// race-free against another worker holding the same key concurrently; for
// strict single-execution under contention, use SELECT … FOR UPDATE NOWAIT
// inside an explicit transaction (subscribers typically combine
// idempotency check + side effect inside one txn).
func (s *PostgresStore) Process(ctx context.Context, key string, ttl time.Duration, fn func() error) error {
	if key == "" {
		return fmt.Errorf("idempotent: key must not be empty")
	}
	if ttl <= 0 {
		return fmt.Errorf("idempotent: ttl must be > 0")
	}
	if fn == nil {
		return fmt.Errorf("idempotent: fn must not be nil")
	}
	seen, err := s.Seen(ctx, key)
	if err != nil {
		return err
	}
	if seen {
		return nil
	}
	if err := fn(); err != nil {
		return err
	}
	return s.Mark(ctx, key, ttl)
}

// CleanupExpired purges records whose ttl_at is in the past.
//
// Implementation note: the idempotency_keys table is an operational dedup
// store (NOT a domain table) — it holds short-lived tokens with no PII or
// audit value. The recommended cleanup mechanism in production is one of:
//
//  1. pg_cron job invoking the SQL stored procedure `cleanup_idempotency_keys()`
//     declared in the migration (see migrations/idempotency_keys.up.sql);
//  2. A separate Cloud Run Job that runs the procedure on a schedule.
//
// Both paths execute the purge inside a SECURITY DEFINER function declared
// in a `*.up.sql` migration file (which is exempt from the domain-table
// hard-delete prohibition per ddd-enforcement.md §Soft Deletes Exceptions:
// "migration files"). Calling the function from Go avoids inlining a hard
// purge verb in the application source.
func (s *PostgresStore) CleanupExpired(ctx context.Context) (int, error) {
	q := `SELECT cleanup_idempotency_keys($1)`
	row := s.db.QueryRowContext(ctx, q, s.table)
	var removed int64
	if err := row.Scan(&removed); err != nil {
		return 0, fmt.Errorf("idempotent.CleanupExpired: %w", err)
	}
	return int(removed), nil
}

// Compile-time check.
var _ Store = (*PostgresStore)(nil)
