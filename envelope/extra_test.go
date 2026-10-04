// Package envelope — supplementary Validate/ValidateStrict/Build edges.
package envelope_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/envelope"
)

func validEnvelope() envelope.Envelope {
	return envelope.Envelope{
		EventID:        "01970000-0000-7000-8000-000000000001",
		IdempotencyKey: "k-1",
		TenantID:       "01970000-0000-7000-8000-0000000000aa",
		OccurredAt:     time.Now().UTC(),
		PublishedAt:    time.Now().UTC(),
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		SourceProject:  "chora-lib-test",
		SourceService:  "chora-lib",
		SchemaVersion:  1,
	}
}

func TestValidate_ReportsMissingOccurredAt(t *testing.T) {
	t.Parallel()
	e := validEnvelope()
	e.OccurredAt = time.Time{}
	err := envelope.Validate(e)
	if err == nil || !strings.Contains(err.Error(), "occurred_at is required") {
		t.Fatalf("err = %v, want occurred_at error", err)
	}
}

func TestValidate_ReportsMissingPublishedAt(t *testing.T) {
	t.Parallel()
	e := validEnvelope()
	e.PublishedAt = time.Time{}
	err := envelope.Validate(e)
	if err == nil || !strings.Contains(err.Error(), "published_at is required") {
		t.Fatalf("err = %v, want published_at error", err)
	}
}

// TestValidateStrict_PropagatesLooseValidationError — a missing mandatory
// field must be surfaced through ValidateStrict, not swallowed.
func TestValidateStrict_PropagatesLooseValidationError(t *testing.T) {
	t.Parallel()
	e := validEnvelope()
	e.EventID = ""
	if err := envelope.ValidateStrict(e); err == nil {
		t.Fatal("expected error for missing event_id")
	}
}

func TestValidateStrict_RejectsNonCanonicalDimension(t *testing.T) {
	t.Parallel()
	e := validEnvelope()
	e.ChoraImdaDimension = "bogus_label"
	if err := envelope.ValidateStrict(e); err == nil {
		t.Fatal("expected error for non-canonical IMDA dimension")
	}
}

func TestValidateStrict_AcceptsCanonicalDimension(t *testing.T) {
	t.Parallel()
	e := validEnvelope()
	e.ChoraImdaDimension = "accountability"
	if err := envelope.ValidateStrict(e); err != nil {
		t.Fatalf("ValidateStrict = %v, want nil", err)
	}
}

// TestBuild_HonoursInjectedClock — Build must use the injectable Now
// function when provided, stamping the same instant into both timestamps.
func TestBuild_HonoursInjectedClock(t *testing.T) {
	t.Parallel()
	fixed := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	buildCalls := 0
	e := envelope.Build(context.Background(), envelope.BuildOpts{
		SourceProject: "chora-lib-test",
		SourceService: "chora-lib",
		SchemaVersion: 1,
		Now: func() time.Time {
			buildCalls++
			return fixed
		},
	})
	if buildCalls == 0 {
		t.Fatal("injected Now was not invoked")
	}
	if !e.OccurredAt.Equal(fixed) || !e.PublishedAt.Equal(fixed) {
		t.Errorf("timestamps = (%v, %v), want both %v", e.OccurredAt, e.PublishedAt, fixed)
	}
}

