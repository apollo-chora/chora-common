// Tests for the IMDA dimension + lifecycle stage envelope fields per
// chora-contracts/proto/common/envelope.proto fields 14 + 15 (added v2.0)
// + ADR-141 IMDA Dimension Labels Reconciliation.
package envelope_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/envelope"
)

func TestBuild_IMDAFieldsDefaultEmpty(t *testing.T) {
	ctx := context.Background()
	env := envelope.Build(ctx, envelope.BuildOpts{
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-x",
	})
	if env.ChoraImdaDimension != "" {
		t.Errorf("ChoraImdaDimension=%q want empty default", env.ChoraImdaDimension)
	}
	if env.ImdaLifecycleStage != "" {
		t.Errorf("ImdaLifecycleStage=%q want empty default", env.ImdaLifecycleStage)
	}
}

func TestBuild_IMDAFieldsHonourBuildOpts(t *testing.T) {
	ctx := context.Background()
	env := envelope.Build(ctx, envelope.BuildOpts{
		SchemaVersion:      1,
		SourceProject:      "chora-489812",
		SourceService:      "chora-governance",
		ChoraImdaDimension: "transparency",
		ImdaLifecycleStage: "runtime",
	})
	if env.ChoraImdaDimension != "transparency" {
		t.Errorf("ChoraImdaDimension=%q want transparency", env.ChoraImdaDimension)
	}
	if env.ImdaLifecycleStage != "runtime" {
		t.Errorf("ImdaLifecycleStage=%q want runtime", env.ImdaLifecycleStage)
	}
}

func TestBuild_CanonicalisesV1Aliases(t *testing.T) {
	// Per ADR-141 deprecated v1 aliases — the builder should normalise them
	// to canonical v2 labels before stamping the envelope, so consumers only
	// ever see canonical values on the wire.
	cases := map[string]string{
		"risk_levels":             "accountability",
		"stakeholder_interaction": "transparency",
		"internal_governance":     "safety_and_robustness",
		"operations_management":   "fairness_and_human_oversight",
	}
	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			ctx := context.Background()
			env := envelope.Build(ctx, envelope.BuildOpts{
				SchemaVersion:      1,
				SourceProject:      "p",
				SourceService:      "s",
				ChoraImdaDimension: input,
			})
			if env.ChoraImdaDimension != want {
				t.Errorf("Build canonicalised %q -> %q want %q", input, env.ChoraImdaDimension, want)
			}
		})
	}
}

func TestCanonicaliseImdaDimension(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		// canonical v2 — passthrough
		{"accountability", "accountability"},
		{"transparency", "transparency"},
		{"safety_and_robustness", "safety_and_robustness"},
		{"fairness_and_human_oversight", "fairness_and_human_oversight"},
		// v1 aliases
		{"risk_levels", "accountability"},
		{"stakeholder_interaction", "transparency"},
		{"internal_governance", "safety_and_robustness"},
		{"operations_management", "fairness_and_human_oversight"},
		// case insensitive — ADR-141 says snake_case lower; we normalise
		{"Accountability", "accountability"},
		{"TRANSPARENCY", "transparency"},
		// surrounding whitespace
		{"  accountability  ", "accountability"},
		// empty stays empty (envelope field is optional)
		{"", ""},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			if got := envelope.CanonicaliseImdaDimension(c.in); got != c.want {
				t.Errorf("CanonicaliseImdaDimension(%q)=%q want %q", c.in, got, c.want)
			}
		})
	}
}

func TestCanonicaliseImdaDimension_UnknownValuePreserved(t *testing.T) {
	// Unknown values are returned as-is (lowercased+trimmed) so producers can
	// still emit them; downstream Validate() will reject if the strict mode
	// is opted-in. This is forward-compatibility.
	got := envelope.CanonicaliseImdaDimension("future_label_v3")
	if got != "future_label_v3" {
		t.Errorf("got %q want passthrough lowercased", got)
	}
}

func TestValidate_AcceptsKnownIMDADimension(t *testing.T) {
	ctx := context.Background()
	for _, dim := range []string{
		"accountability", "transparency", "safety_and_robustness", "fairness_and_human_oversight",
	} {
		env := envelope.Build(ctx, envelope.BuildOpts{
			SchemaVersion:      1,
			SourceProject:      "p",
			SourceService:      "s",
			ChoraImdaDimension: dim,
		})
		if err := envelope.Validate(env); err != nil {
			t.Errorf("Validate(dim=%q) = %v want nil", dim, err)
		}
	}
}

func TestValidate_AcceptsKnownLifecycleStage(t *testing.T) {
	ctx := context.Background()
	for _, stage := range []string{"ci_pre_merge", "pre_deploy", "runtime", "post_deploy"} {
		env := envelope.Build(ctx, envelope.BuildOpts{
			SchemaVersion:      1,
			SourceProject:      "p",
			SourceService:      "s",
			ImdaLifecycleStage: stage,
		})
		if err := envelope.Validate(env); err != nil {
			t.Errorf("Validate(stage=%q) = %v want nil", stage, err)
		}
	}
}

func TestValidate_RejectsUnknownLifecycleStage(t *testing.T) {
	ctx := context.Background()
	env := envelope.Build(ctx, envelope.BuildOpts{
		SchemaVersion:      1,
		SourceProject:      "p",
		SourceService:      "s",
		ImdaLifecycleStage: "bogus_stage",
	})
	err := envelope.Validate(env)
	if err == nil {
		t.Fatal("expected validation error for bogus_stage")
	}
	if !strings.Contains(err.Error(), "imda_lifecycle_stage") {
		t.Errorf("err=%q want imda_lifecycle_stage substring", err.Error())
	}
}

func TestValidate_RejectsUnknownIMDADimensionInStrictMode(t *testing.T) {
	// Strict mode: when explicitly enabled, validate against the known
	// canonical labels. Unknown -> error (so producers can opt-in to schema
	// enforcement in CI before publishing).
	ctx := context.Background()
	env := envelope.Build(ctx, envelope.BuildOpts{
		SchemaVersion:      1,
		SourceProject:      "p",
		SourceService:      "s",
		ChoraImdaDimension: "future_label_v3",
	})
	err := envelope.ValidateStrict(env)
	if err == nil {
		t.Fatal("expected ValidateStrict error for unknown imda dimension")
	}
	if !strings.Contains(err.Error(), "chora_imda_dimension") {
		t.Errorf("err=%q want chora_imda_dimension substring", err.Error())
	}
}

func TestValidate_LooseModeAllowsUnknownIMDADimensionForwardCompat(t *testing.T) {
	// Default Validate is forward-compat (loose); only ValidateStrict rejects
	// unknowns. This lets the platform roll out new label vocabularies without
	// breaking older subscribers.
	ctx := context.Background()
	env := envelope.Build(ctx, envelope.BuildOpts{
		SchemaVersion:      1,
		SourceProject:      "p",
		SourceService:      "s",
		ChoraImdaDimension: "future_label_v3",
	})
	if err := envelope.Validate(env); err != nil {
		t.Errorf("Validate (loose) = %v want nil for unknown dimension", err)
	}
}
