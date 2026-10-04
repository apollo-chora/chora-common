package envelope_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/tracing"
)

func TestBuild_FillsMandatoryFields(t *testing.T) {
	ctx := context.Background()
	ctx = tracing.WithTenantID(ctx, "tenant-abc")
	ctx = tracing.WithGCID(ctx, "gcid-123")
	ctx = tracing.WithTraceparent(ctx, "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")

	env := envelope.Build(ctx, envelope.BuildOpts{
		EventType:     "atom_published",
		SchemaVersion: 1,
		SourceProject: "chora-content",
		SourceService: "chora-creation",
	})

	if env.EventID == "" {
		t.Error("EventID empty")
	}
	if env.IdempotencyKey == "" {
		t.Error("IdempotencyKey empty")
	}
	if env.TenantID != "tenant-abc" {
		t.Errorf("TenantID=%q want tenant-abc", env.TenantID)
	}
	if env.GCID != "gcid-123" {
		t.Errorf("GCID=%q want gcid-123", env.GCID)
	}
	if env.OccurredAt.IsZero() {
		t.Error("OccurredAt zero")
	}
	if env.PublishedAt.IsZero() {
		t.Error("PublishedAt zero")
	}
	if env.Traceparent == "" {
		t.Error("Traceparent empty")
	}
	if env.SourceProject != "chora-content" {
		t.Errorf("SourceProject=%q", env.SourceProject)
	}
	if env.SourceService != "chora-creation" {
		t.Errorf("SourceService=%q", env.SourceService)
	}
	if env.SchemaVersion != 1 {
		t.Errorf("SchemaVersion=%d", env.SchemaVersion)
	}
}

func TestBuild_DefaultsTenantToPlatform(t *testing.T) {
	ctx := context.Background()
	env := envelope.Build(ctx, envelope.BuildOpts{
		EventType:     "platform_event",
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-platform",
	})
	if env.TenantID != "platform" {
		t.Errorf("TenantID=%q want platform fallback for system events", env.TenantID)
	}
}

func TestBuild_GeneratesTraceparentWhenAbsent(t *testing.T) {
	ctx := context.Background()
	ctx = tracing.WithTenantID(ctx, "tenant-x")
	env := envelope.Build(ctx, envelope.BuildOpts{
		EventType:     "x_event",
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-x",
	})
	if env.Traceparent == "" {
		t.Error("Traceparent should be generated when missing in ctx")
	}
	parts := strings.Split(env.Traceparent, "-")
	if len(parts) != 4 {
		t.Errorf("Traceparent shape wrong: %q", env.Traceparent)
	}
}

func TestValidate_ReportsMissingMandatoryFields(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*envelope.Envelope)
		wantErr string
	}{
		{"missing event_id", func(e *envelope.Envelope) { e.EventID = "" }, "event_id"},
		{"missing idempotency_key", func(e *envelope.Envelope) { e.IdempotencyKey = "" }, "idempotency_key"},
		{"missing tenant_id", func(e *envelope.Envelope) { e.TenantID = "" }, "tenant_id"},
		{"missing source_project", func(e *envelope.Envelope) { e.SourceProject = "" }, "source_project"},
		{"missing source_service", func(e *envelope.Envelope) { e.SourceService = "" }, "source_service"},
		{"missing traceparent", func(e *envelope.Envelope) { e.Traceparent = "" }, "traceparent"},
		{"zero schema_version", func(e *envelope.Envelope) { e.SchemaVersion = 0 }, "schema_version"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = tracing.WithTenantID(ctx, "tenant-1")
			env := envelope.Build(ctx, envelope.BuildOpts{
				EventType:     "x",
				SchemaVersion: 1,
				SourceProject: "chora-489812",
				SourceService: "chora-x",
			})
			c.mutate(&env)
			err := envelope.Validate(env)
			if err == nil {
				t.Fatalf("expected validation error containing %q", c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("err=%q want substring %q", err.Error(), c.wantErr)
			}
		})
	}
}

func TestValidate_AcceptsValid(t *testing.T) {
	ctx := context.Background()
	ctx = tracing.WithTenantID(ctx, "tenant-ok")
	env := envelope.Build(ctx, envelope.BuildOpts{
		EventType:     "x",
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-x",
	})
	if err := envelope.Validate(env); err != nil {
		t.Errorf("Validate=%v want nil for valid envelope", err)
	}
}

func TestBuild_EventIDIsUUIDv7(t *testing.T) {
	ctx := context.Background()
	env1 := envelope.Build(ctx, envelope.BuildOpts{
		EventType: "x", SchemaVersion: 1, SourceProject: "p", SourceService: "s",
	})
	env2 := envelope.Build(ctx, envelope.BuildOpts{
		EventType: "x", SchemaVersion: 1, SourceProject: "p", SourceService: "s",
	})
	if env1.EventID == env2.EventID {
		t.Error("EventIDs should be unique across builds")
	}
	// UUIDv7 has length 36 with hyphens.
	if len(env1.EventID) != 36 {
		t.Errorf("EventID length=%d want 36 (uuid)", len(env1.EventID))
	}
}

func TestBuild_IdempotencyKeyDefaultsToEventID(t *testing.T) {
	ctx := context.Background()
	env := envelope.Build(ctx, envelope.BuildOpts{
		EventType: "x", SchemaVersion: 1, SourceProject: "p", SourceService: "s",
	})
	if env.IdempotencyKey != env.EventID {
		t.Errorf("IdempotencyKey=%q want default to EventID=%q", env.IdempotencyKey, env.EventID)
	}
}

func TestBuild_HonoursExplicitIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	env := envelope.Build(ctx, envelope.BuildOpts{
		EventType:      "x",
		SchemaVersion:  1,
		SourceProject:  "p",
		SourceService:  "s",
		IdempotencyKey: "atom-123-rev-7",
	})
	if env.IdempotencyKey != "atom-123-rev-7" {
		t.Errorf("IdempotencyKey=%q want atom-123-rev-7", env.IdempotencyKey)
	}
}
