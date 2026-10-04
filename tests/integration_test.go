// Package tests contains cross-package integration tests for chora-common.
// These exercise the public surface of the library composed end-to-end (env →
// log → tracing → envelope) without crossing process boundaries.
package tests

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/log"
	"github.com/apollo-chora/chora-common/tracing"
)

// TestEnd2End_BuildEnvelopeFromContext exercises the typical service flow:
// inbound HTTP -> tracing context -> publish event with envelope.
func TestEnd2End_BuildEnvelopeFromContext(t *testing.T) {
	logger := log.New("chora-test-svc")
	if logger == nil {
		t.Fatal("logger nil")
	}

	ctx := context.Background()
	ctx = tracing.WithTenantID(ctx, "tenant-it-1")
	ctx = tracing.WithGCID(ctx, "gcid-it-1")
	ctx = tracing.WithTraceparent(ctx, "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")

	env := envelope.Build(ctx, envelope.BuildOpts{
		EventType:     "atom_published",
		SchemaVersion: 1,
		SourceProject: "chora-content",
		SourceService: "chora-creation",
	})

	if err := envelope.Validate(env); err != nil {
		t.Fatalf("integration envelope.Validate: %v", err)
	}
	if env.TenantID != "tenant-it-1" {
		t.Errorf("envelope.TenantID=%q", env.TenantID)
	}
	if env.GCID != "gcid-it-1" {
		t.Errorf("envelope.GCID=%q", env.GCID)
	}
	if env.Traceparent == "" {
		t.Error("envelope.Traceparent empty")
	}
}
