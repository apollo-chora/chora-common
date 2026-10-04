// Package durabilityguard is the runtime half of the W0-F1 durability gate
// (CHO-2198). At boot, a service hands it the adapters it actually wired behind
// each port; the guard reflects over each adapter's field structure and decides
// whether it is backed by a real database handle (Durable), by in-process maps
// (InMemory), or neither (Unknown).
//
// Why runtime and not just the static naming scan
// (chora-infra/scripts/inmemory-guard): the static scan keys on the *constructor
// name* (inmem.NewFooRepo) and is therefore structurally blind to DOMAIN-TYPED
// in-memory stores whose constructor is named for the concept, not for being
// in-memory — e.g. reaction.NewRegistry(), domain.NewCertificationRegistry().
// One of those, shipped ungated, would serve production reads/writes from a map
// and pass every static check silently. This guard keys on the resolved
// adapter's *shape* (does it hold a live *pgxpool.Pool / *sql.DB, or a data
// map), so it catches the domain-typed class too.
//
// It ships REPORT-ONLY by default: it always logs a structured, greppable report
// and never exits. Set CHORA_DURABILITY_GUARD=enforce to make a non-allowlisted
// in-memory binding (while CHORA_DB_DSN is set) a fatal boot failure. Arm enforce
// only once the owner-signed allow-list covers every intentional in-memory
// binding, or the service will refuse to boot.
package durabilityguard

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
)

// Verdict is the durability classification of a wired adapter.
type Verdict int

const (
	// Unknown — no database handle and no data-bearing map were found. The
	// adapter could be a pure-compute port, a thin proxy, or a shape the guard
	// cannot read; adjudicate it explicitly in the allow-list.
	Unknown Verdict = iota
	// Durable — holds a live (non-nil) database handle (*pgxpool.Pool, *sql.DB,
	// pgx.Conn/Tx), directly or through interface/pointer indirection.
	Durable
	// InMemory — holds a data-bearing map or sync.Map and no live DB handle.
	InMemory
)

func (v Verdict) String() string {
	switch v {
	case Durable:
		return "DURABLE"
	case InMemory:
		return "IN_MEMORY"
	default:
		return "UNKNOWN"
	}
}

// Binding names a wired port and the adapter resolved behind it at boot.
type Binding struct {
	Port    string
	Adapter any
}

// Detail is the per-binding classification result.
type Detail struct {
	Port     string
	TypeName string
	Verdict  Verdict
}

// Report is the outcome of a boot-time evaluation.
type Report struct {
	Service      string
	DBConfigured bool // the explicit env hint (CHORA_DB_DSN et al.)
	PoolPresent  bool // effective signal: env hint OR ≥1 binding resolved durable
	Counts       map[Verdict]int
	Details      []Detail
	// Violations are the in-memory bindings that are NOT allow-listed while a
	// pool is present — the set that fails the gate in enforce mode.
	Violations []Detail
}

// Mode controls whether a violation is fatal.
type Mode int

const (
	// ModeReport logs the report and returns; never exits. Default.
	ModeReport Mode = iota
	// ModeEnforce makes a non-empty Violations set a fatal boot failure.
	ModeEnforce
)

const maxDepth = 8

// durableBackingType reports whether t is a recognised live database handle
// type (matched by package path + name, so the guard needs no build-time
// coupling to a specific driver version and catches wrapped handles too).
func durableBackingType(t reflect.Type) bool {
	if t == nil {
		return false
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	pkg, name := t.PkgPath(), t.Name()
	switch {
	case pkg == "database/sql" && (name == "DB" || name == "Conn" || name == "Tx"):
		return true
	case strings.Contains(pkg, "jackc/pgx") && (name == "Pool" || name == "Conn" || name == "Tx"):
		return true
	}
	return false
}

// Classify reflects over the concrete value behind adapter and returns its
// durability Verdict. It reads only field types/kinds (never .Interface()), so
// it works on unexported fields, and it recurses through pointer and interface
// indirection to a bounded depth with cycle protection.
func Classify(adapter any) Verdict {
	if adapter == nil {
		return Unknown
	}
	pool, dataMap := walk(reflect.ValueOf(adapter), maxDepth, map[reflect.Type]bool{})
	switch {
	case pool:
		return Durable
	case dataMap:
		return InMemory
	default:
		return Unknown
	}
}

// walk returns whether the value tree contains a live DB handle (pool) and/or a
// data-bearing map (dataMap). A live pool short-circuits the search: an adapter
// that can reach Postgres is durable regardless of any incidental map.
func walk(v reflect.Value, depth int, seen map[reflect.Type]bool) (pool, dataMap bool) {
	if depth < 0 || !v.IsValid() {
		return false, false
	}
	t := v.Type()

	// A live (non-nil) DB handle anywhere in the tree ⇒ durable.
	if durableBackingType(t) {
		if v.Kind() == reflect.Ptr {
			return !v.IsNil(), false
		}
		return true, false
	}

	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return false, false
		}
		return walk(v.Elem(), depth-1, seen)

	case reflect.Map:
		// A map field is the defining shape of an in-process store.
		return false, true

	case reflect.Struct:
		if seen[t] {
			return false, false
		}
		seen[t] = true
		// sync.Map is a struct, not a map kind, but it is unambiguously an
		// in-process data store.
		if t.PkgPath() == "sync" && t.Name() == "Map" {
			return false, true
		}
		// Don't descend into other sync/atomic primitives (mutexes, Once): they
		// carry no domain data and only add noise.
		if pkg := t.PkgPath(); pkg == "sync" || pkg == "sync/atomic" {
			return false, false
		}
		for i := 0; i < v.NumField(); i++ {
			p, m := walk(v.Field(i), depth-1, seen)
			pool = pool || p
			dataMap = dataMap || m
			if pool {
				return true, dataMap
			}
		}
		return pool, dataMap
	}
	return false, false
}

func typeName(a any) string {
	if a == nil {
		return "<nil>"
	}
	return reflect.TypeOf(a).String()
}

// Evaluate classifies every binding and computes the violation set. It performs
// no logging and never exits — it is the pure core, so it is fully testable.
//
// A binding is a violation iff it is InMemory, a pool is present, and its
// "service:port" key is absent from allowlist. "Pool present" is inferred
// env-agnostically: the explicit dbConfigured hint OR the fact that ≥1 sibling
// binding resolved Durable. The inference is load-bearing — Chora services wire
// their pg adapters off a cloudsql-proxy socket / component-built DSN, so
// CHORA_DB_DSN is frequently unset even when the pool is healthy; keying only on
// that env var would make enforce mode fail OPEN (a mixed root with a live pool
// and silently-in-memory ports would report zero violations). A root with NO
// durable binding and no hint is treated as pure-dev (in-memory expected).
func Evaluate(service string, bindings []Binding, allowlist map[string]bool, dbConfigured bool) Report {
	rep := Report{Service: service, DBConfigured: dbConfigured, Counts: map[Verdict]int{}}
	for _, b := range bindings {
		verdict := Classify(b.Adapter)
		rep.Counts[verdict]++
		rep.Details = append(rep.Details, Detail{Port: b.Port, TypeName: typeName(b.Adapter), Verdict: verdict})
	}
	rep.PoolPresent = dbConfigured || rep.Counts[Durable] > 0
	for _, d := range rep.Details {
		if d.Verdict == InMemory && rep.PoolPresent && !allowlist[service+":"+d.Port] {
			rep.Violations = append(rep.Violations, d)
		}
	}
	sort.Slice(rep.Details, func(i, j int) bool { return rep.Details[i].Port < rep.Details[j].Port })
	sort.Slice(rep.Violations, func(i, j int) bool { return rep.Violations[i].Port < rep.Violations[j].Port })
	return rep
}

// ModeFromEnv returns ModeEnforce iff CHORA_DURABILITY_GUARD=enforce.
func ModeFromEnv() Mode {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("CHORA_DURABILITY_GUARD")), "enforce") {
		return ModeEnforce
	}
	return ModeReport
}

// DBConfigured reports whether a database DSN is configured for the service.
func DBConfigured() bool {
	return strings.TrimSpace(os.Getenv("CHORA_DB_DSN")) != ""
}

// Guard is the ergonomic boot entrypoint a service calls once after wiring its
// repos. It evaluates the bindings, logs a structured greppable report, and —
// only in enforce mode with violations present — fails the boot loudly.
func Guard(service string, bindings []Binding, allowlist map[string]bool) Report {
	rep := Evaluate(service, bindings, allowlist, DBConfigured())
	logReport(rep)
	if ModeFromEnv() == ModeEnforce && len(rep.Violations) > 0 {
		log.Fatalf("DURABILITY-GUARD service=%s FATAL: %d non-allowlisted in-memory binding(s) serving production with CHORA_DB_DSN set: %s",
			service, len(rep.Violations), portList(rep.Violations))
	}
	return rep
}

func logReport(rep Report) {
	log.Printf("DURABILITY-GUARD service=%s pool_present=%t db_dsn_set=%t total=%d durable=%d in_memory=%d unknown=%d violations=%d mode=%s",
		rep.Service, rep.PoolPresent, rep.DBConfigured, len(rep.Details),
		rep.Counts[Durable], rep.Counts[InMemory], rep.Counts[Unknown], len(rep.Violations), modeName())
	for _, d := range rep.Details {
		status := "ok"
		if isViolation(rep, d) {
			status = "VIOLATION"
		} else if d.Verdict == Unknown {
			status = "review"
		}
		log.Printf("DURABILITY-GUARD service=%s port=%s verdict=%s type=%s status=%s",
			rep.Service, d.Port, d.Verdict, d.TypeName, status)
	}
	if len(rep.Violations) > 0 {
		log.Printf("DURABILITY-GUARD service=%s ERROR: %d in-memory binding(s) serving production reads/writes with a DB configured: %s — fix to a durable adapter or add an owner-signed allow-list entry",
			rep.Service, len(rep.Violations), portList(rep.Violations))
	}
}

func isViolation(rep Report, d Detail) bool {
	for _, v := range rep.Violations {
		if v.Port == d.Port {
			return true
		}
	}
	return false
}

func modeName() string {
	if ModeFromEnv() == ModeEnforce {
		return "enforce"
	}
	return "report"
}

func portList(ds []Detail) string {
	ps := make([]string, len(ds))
	for i, d := range ds {
		ps[i] = fmt.Sprintf("%s(%s)", d.Port, d.TypeName)
	}
	return strings.Join(ps, ", ")
}
