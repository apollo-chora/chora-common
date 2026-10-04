// Package choraserver — outbox reachability checker for /readyz.
//
// SQLOutboxChecker is the production OutboxChecker implementation:
// it runs a cheap `SELECT 1 FROM <table> LIMIT 1` against the outbox
// table and reports the result. The query is intentionally read-only
// + small so it cannot fail for reasons OTHER than "table missing /
// pool dead / permission denied" — which are exactly the failure
// modes /readyz is supposed to surface.
//
// Per E2E-INFRA-COLD-START §B + `feedback_no_stubs_real_wiring`:
// services that publish events MUST wire this (or an equivalent
// implementation). A service whose outbox table is missing is silently
// dropping events into the void; the readiness check must fail loud.
package choraserver

import (
	"context"
	"database/sql"
	"fmt"
)

// SQLOutboxChecker implements OutboxChecker against a *sql.DB +
// configurable outbox table name. Default table name is "outbox_events".
type SQLOutboxChecker struct {
	db    *sql.DB
	table string
}

// NewSQLOutboxChecker constructs a checker. If table is empty,
// defaults to "outbox_events" per the canonical Chora outbox schema.
func NewSQLOutboxChecker(db *sql.DB, table string) *SQLOutboxChecker {
	if table == "" {
		table = "outbox_events"
	}
	return &SQLOutboxChecker{db: db, table: table}
}

// IsReachable returns nil iff `SELECT 1 FROM <table> LIMIT 1`
// completes without error. An empty table is OK — only a missing
// table or unreachable pool surfaces an error.
func (c *SQLOutboxChecker) IsReachable(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("outbox: db is nil")
	}
	// SELECT 1 + LIMIT 1 — minimal-cost row scan. Discard the row;
	// we only care that the statement parses + executes.
	row := c.db.QueryRowContext(ctx, fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", c.table)) //nolint:gosec // table is a string we control
	var one sql.NullInt32
	if err := row.Scan(&one); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("outbox: select from %s: %w", c.table, err)
	}
	return nil
}
