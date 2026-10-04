package durabilityguard

// durabilityguard_extra_test.go — statement-coverage extension for the
// remaining branches of durabilityguard.go (Verdict.String, walk's
// defensive branches, Evaluate's violation sorting, logReport status
// classification and modeName). Test-only; does not weaken existing
// assertions in durabilityguard_test.go.

import (
	"os"
	"os/exec"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestVerdictString_AllBranches(t *testing.T) {
	cases := map[Verdict]string{
		Durable:     "DURABLE",
		InMemory:    "IN_MEMORY",
		Unknown:     "UNKNOWN",
		Verdict(99): "UNKNOWN",
	}
	for v, want := range cases {
		if got := v.String(); got != want {
			t.Errorf("Verdict(%d).String() = %q, want %q", v, got, want)
		}
	}
}

// TestClassify_NonPointerDurableBody covers the durableBackingType hit
// when the handle is stored BY VALUE (kind Struct, not Ptr) — the branch
// that returns true immediately instead of the !IsNil() probe.
func TestClassify_NonPointerDurable(t *testing.T) {
	// pgxpool.Pool contains no lock fields, so a by-value composite is
	// vet-clean; sql.DB is not used by value for the same reason.
	if got := Classify(pgxpool.Pool{}); got != Durable {
		t.Errorf("Classify(pgxpool.Pool{}) = %v, want Durable", got)
	}
}

// nilAnyFieldFake holds a nil interface field — walk must take the
// Ptr/Interface IsNil short-circuit rather than panicking on Elem().
type nilAnyFieldFake struct{ inner any }

// cyclicFake points at itself so walk hits the seen-type cycle guard.
type cyclicFake struct{ next *cyclicFake }

// deeplyNestedFake exhausts walk's depth budget (maxDepth=8) via a chain
// of pointer indirections, exercising the depth<0 bail-out.
type deepL1 struct{ next *deepL2 }
type deepL2 struct{ next *deepL3 }
type deepL3 struct{ next *deepL4 }
type deepL4 struct{ next *deepL5 }
type deepL5 struct{ next *deepL6 }
type deepL6 struct{ next *deepL7 }
type deepL7 struct{ next *deepL8 }
type deepL8 struct{ next *deepL9 }
type deepL9 struct{ next *deepL10 }
type deepL10 struct{}

func TestClassify_RemainingShapeBranches(t *testing.T) {
	t.Run("nil interface field short-circuits", func(t *testing.T) {
		if got := Classify(&nilAnyFieldFake{}); got != Unknown {
			t.Errorf("Classify = %v, want Unknown", got)
		}
	})
	t.Run("cyclic struct hits the seen-type guard", func(t *testing.T) {
		c := &cyclicFake{}
		c.next = c
		if got := Classify(c); got != Unknown {
			t.Errorf("Classify = %v, want Unknown", got)
		}
	})
	t.Run("deep indirection exhausts depth budget", func(t *testing.T) {
		root := &deepL1{
			next: &deepL2{
				next: &deepL3{
					next: &deepL4{
						next: &deepL5{
							next: &deepL6{
								next: &deepL7{
									next: &deepL8{
										next: &deepL9{
											next: &deepL10{},
										},
									},
								},
							},
						},
					},
				},
			},
		}
		if got := Classify(root); got != Unknown {
			t.Errorf("Classify = %v, want Unknown", got)
		}
	})
	// typeName(nil) must render "<nil>" instead of panicking on
	// reflect.TypeOf(nil).
	t.Run("nil adapter records typed name as <nil>", func(t *testing.T) {
		rep := Evaluate("svc", []Binding{{Port: "ghost", Adapter: nil}}, nil, false)
		if len(rep.Details) != 1 || rep.Details[0].TypeName != "<nil>" {
			t.Errorf("details = %+v, want one <nil>-typed detail", rep.Details)
		}
		if rep.Details[0].Verdict != Unknown {
			t.Errorf("verdict = %v, want Unknown", rep.Details[0].Verdict)
		}
	})
}

// TestEvaluate_TwoViolations_DeterministicOrder covers the sort.Slice
// callback on rep.Violations, which only fires when there are ≥2
// violations.
func TestEvaluate_TwoViolations_DeterministicOrder(t *testing.T) {
	rep := Evaluate("svc", []Binding{
		{Port: "zebra", Adapter: &inmemRepoFake{byID: map[string]int{}}},
		{Port: "alpha", Adapter: &inmemRepoFake{byID: map[string]int{}}},
		{Port: "courses", Adapter: &pgRepoFake{tx: pgxTxRunnerFake{pool: nonNilPgxPool()}}},
	}, nil, true)
	if len(rep.Violations) != 2 {
		t.Fatalf("violations = %+v, want exactly 2", rep.Violations)
	}
	if rep.Violations[0].Port != "alpha" || rep.Violations[1].Port != "zebra" {
		t.Errorf("violations out of order: %v, %v", rep.Violations[0].Port, rep.Violations[1].Port)
	}
}

// TestGuard_ReportMode_ClassifiesAllStatuses drives logReport over a
// mixed root so every per-detail status branch runs: VIOLATION
// (in-memory, not allowlisted), review (Unknown), ok (durable →
// isViolation false-path).
func TestGuard_ReportMode_ClassifiesAllStatuses(t *testing.T) {
	t.Setenv("CHORA_DURABILITY_GUARD", "")
	t.Setenv("CHORA_DB_DSN", "postgres://x")
	rep := Guard("svc", []Binding{
		{Port: "posts", Adapter: &inmemRepoFake{byID: map[string]int{}}},
		{Port: "router", Adapter: &plainFake{}},
		{Port: "courses", Adapter: &pgRepoFake{tx: pgxTxRunnerFake{pool: nonNilPgxPool()}}},
	}, map[string]bool{"svc:posts": true})
	if len(rep.Violations) != 0 {
		t.Fatalf("allowlisted posts must not violate: %+v", rep.Violations)
	}
	if rep.Counts[Unknown] != 1 || rep.Counts[InMemory] != 1 || rep.Counts[Durable] != 1 {
		t.Errorf("counts = %v", rep.Counts)
	}
}

// TestGuard_EnforceMode_NoViolationsDoesNotExit covers modeName()'s
// enforce branch without tripping log.Fatalf: enforce + violations list
// empty is not fatal.
func TestGuard_EnforceMode_NoViolationsDoesNotExit(t *testing.T) {
	t.Setenv("CHORA_DURABILITY_GUARD", "enforce")
	t.Setenv("CHORA_DB_DSN", "") // no DSN hint
	rep := Guard("svc", []Binding{
		{Port: "courses", Adapter: &pgRepoFake{tx: pgxTxRunnerFake{pool: nonNilPgxPool()}}},
	}, nil)
	if len(rep.Violations) != 0 {
		t.Fatalf("no in-memory binding, no DSN: violations must be empty, got %+v", rep.Violations)
	}
	if !rep.PoolPresent {
		// durable sibling infers the pool — so the report reflects it
		t.Error("durable binding should infer PoolPresent")
	}
}

// TestGuard_EnforceWithViolation_Exits re-runs the test binary as a
// child process so log.Fatalf (os.Exit(1)) is genuinely exercised. The
// parent asserts the child died with the fatal exit code. (Coverage
// counters are per-process, so this cannot contribute to the in-process
// coverprofile — behaviour is what we assert here.)
func TestGuard_EnforceWithViolation_Exits(t *testing.T) {
	if os.Getenv("DG_ENFORCE_CHILD") == "1" {
		t.Setenv("CHORA_DURABILITY_GUARD", "enforce")
		t.Setenv("CHORA_DB_DSN", "postgres://x")
		Guard("svc", []Binding{
			{Port: "posts", Adapter: &inmemRepoFake{byID: map[string]int{}}},
		}, nil)
		t.Fatal("unreachable: Guard must log.Fatalf before returning")
	}
	// Re-exec this test binary in child mode.
	cmd := exec.Command(os.Args[0], "-test.run=^TestGuard_EnforceWithViolation_Exits$")
	cmd.Env = append(os.Environ(), "DG_ENFORCE_CHILD=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("child exited 0 — enforce mode with a violation must be fatal")
	}
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() == 0 {
		t.Fatalf("child exit = %v, want fatal (non-zero)", err)
	}
}
