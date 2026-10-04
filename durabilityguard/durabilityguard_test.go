package durabilityguard

import (
	"database/sql"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Fakes mirroring the real composition-root shapes -----------------------

// txRunner is the interface a pg adapter holds (mirrors payments' pg.TxRunner:
// an INTERFACE field whose concrete value carries the *pgxpool.Pool). The guard
// must recurse THROUGH the interface to the concrete pool handle.
type txRunner interface{ run() }

type pgxTxRunnerFake struct{ pool *pgxpool.Pool }

func (pgxTxRunnerFake) run() {}

type sqlTxRunnerFake struct{ db *sql.DB }

func (sqlTxRunnerFake) run() {}

// pgRepoFake mirrors pg.CoursePurchaseRepo{ tx TxRunner }.
type pgRepoFake struct{ tx txRunner }

// inmemRepoFake mirrors inmem.CoursePurchaseRepo{ mu sync.RWMutex; byID map }.
type inmemRepoFake struct {
	mu     sync.RWMutex
	byID   map[string]int
	bySess map[string]string
}

// syncMapRepoFake stores data in a sync.Map (a struct, not a map kind).
type syncMapRepoFake struct{ m sync.Map }

// nilPoolPlusMapFake holds a *sql.DB field that is NIL and a data map — a nil
// pool must NOT count as durable, so this is IN_MEMORY.
type nilPoolPlusMapFake struct {
	db   *sql.DB // nil
	byID map[string]int
}

// nilPoolNoMapFake holds only a nil pool handle and nothing else → Unknown.
type nilPoolNoMapFake struct{ db *sql.DB } // nil

// plainFake holds neither a pool nor a data map → Unknown.
type plainFake struct {
	name string
	n    int
}

// decorator wraps another adapter behind an interface field (e.g. an
// outbox/caching decorator) — the guard must see through it to the inner.
type decorator struct{ inner any }

func nonNilPgxPool() *pgxpool.Pool { return &pgxpool.Pool{} }
func nonNilSQLDB() *sql.DB         { return &sql.DB{} }

func TestClassify(t *testing.T) {
	cases := []struct {
		name    string
		adapter any
		want    Verdict
	}{
		{"pg adapter via pgx tx-runner interface", &pgRepoFake{tx: pgxTxRunnerFake{pool: nonNilPgxPool()}}, Durable},
		{"pg adapter via sql tx-runner interface", &pgRepoFake{tx: sqlTxRunnerFake{db: nonNilSQLDB()}}, Durable},
		{"direct pgxpool.Pool field", &struct{ pool *pgxpool.Pool }{pool: nonNilPgxPool()}, Durable},
		{"direct sql.DB field", &struct{ db *sql.DB }{db: nonNilSQLDB()}, Durable},
		{"inmem maps", &inmemRepoFake{byID: map[string]int{}, bySess: map[string]string{}}, InMemory},
		{"sync.Map store", &syncMapRepoFake{}, InMemory},
		{"nil pool + data map is in-memory", &nilPoolPlusMapFake{db: nil, byID: map[string]int{}}, InMemory},
		{"nil pool, no map is unknown", &nilPoolNoMapFake{db: nil}, Unknown},
		{"no backing fields is unknown", &plainFake{name: "x"}, Unknown},
		{"decorator wrapping inmem is in-memory", &decorator{inner: &inmemRepoFake{byID: map[string]int{}}}, InMemory},
		{"decorator wrapping pg is durable", &decorator{inner: &pgRepoFake{tx: pgxTxRunnerFake{pool: nonNilPgxPool()}}}, Durable},
		{"nil adapter is unknown", nil, Unknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.adapter); got != tc.want {
				t.Fatalf("Classify() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEvaluate(t *testing.T) {
	inmem := func() any { return &inmemRepoFake{byID: map[string]int{}} }
	durable := func() any { return &pgRepoFake{tx: pgxTxRunnerFake{pool: nonNilPgxPool()}} }

	// A mixed root: a live pg binding proves the pool is present, so the
	// silently-in-memory siblings are violations — even though the DSN hint is
	// unset (the real chora-payments/identity case behind a cloudsql-proxy).
	mixed := []Binding{
		{Port: "posts", Adapter: inmem()},      // in-memory, not allowlisted
		{Port: "reactions", Adapter: inmem()},  // in-memory, allowlisted
		{Port: "courses", Adapter: durable()},  // durable → infers pool present
		{Port: "router", Adapter: &plainFake{}}, // unknown
	}
	allow := map[string]bool{"svc:reactions": true}

	t.Run("a durable sibling infers pool present and surfaces the in-memory violation without any DSN hint", func(t *testing.T) {
		rep := Evaluate("svc", mixed, allow, false)
		if !rep.PoolPresent {
			t.Fatalf("PoolPresent = false, want true (inferred from the durable binding)")
		}
		if len(rep.Violations) != 1 || rep.Violations[0].Port != "posts" {
			t.Fatalf("violations = %+v, want exactly [posts]", rep.Violations)
		}
		if rep.Counts[Durable] != 1 || rep.Counts[InMemory] != 2 || rep.Counts[Unknown] != 1 {
			t.Fatalf("counts = %v, want Durable1 InMemory2 Unknown1", rep.Counts)
		}
	})

	t.Run("all in-memory with no DSN hint is pure-dev, zero violations", func(t *testing.T) {
		rep := Evaluate("svc", []Binding{{Port: "posts", Adapter: inmem()}, {Port: "reactions", Adapter: inmem()}}, allow, false)
		if rep.PoolPresent {
			t.Fatalf("PoolPresent = true, want false (no durable binding, no hint)")
		}
		if len(rep.Violations) != 0 {
			t.Fatalf("violations = %+v, want none in pure-dev", rep.Violations)
		}
	})

	t.Run("explicit DSN hint forces violations even with no durable binding", func(t *testing.T) {
		rep := Evaluate("svc", []Binding{{Port: "posts", Adapter: inmem()}}, allow, true)
		if len(rep.Violations) != 1 || rep.Violations[0].Port != "posts" {
			t.Fatalf("violations = %+v, want [posts] when DSN hint is set", rep.Violations)
		}
	})

	t.Run("allowlisted in-memory binding is not a violation", func(t *testing.T) {
		rep := Evaluate("svc", []Binding{{Port: "reactions", Adapter: inmem()}, {Port: "courses", Adapter: durable()}}, allow, false)
		if len(rep.Violations) != 0 {
			t.Fatalf("violations = %+v, want none (allowlisted)", rep.Violations)
		}
	})
}

func TestModeFromEnv(t *testing.T) {
	t.Setenv("CHORA_DURABILITY_GUARD", "enforce")
	if ModeFromEnv() != ModeEnforce {
		t.Fatalf("want ModeEnforce")
	}
	t.Setenv("CHORA_DURABILITY_GUARD", "")
	if ModeFromEnv() != ModeReport {
		t.Fatalf("want ModeReport (default)")
	}
}

func TestGuardReportModeReturnsAndDoesNotExit(t *testing.T) {
	t.Setenv("CHORA_DURABILITY_GUARD", "") // report-only
	t.Setenv("CHORA_DB_DSN", "postgres://x")
	rep := Guard("svc", []Binding{{Port: "posts", Adapter: &inmemRepoFake{byID: map[string]int{}}}}, nil)
	// Report-only: it must return (not os.Exit) even with a violation present.
	if len(rep.Violations) != 1 {
		t.Fatalf("want the violation surfaced in report-only mode, got %+v", rep.Violations)
	}
}
