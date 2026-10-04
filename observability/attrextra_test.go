// attrextra_test.go — statement-coverage extension for the remaining
// SetAISpanAttributes branches (adapter version + guardrail outcome) and
// the traceHandler's tenant_id / gcid log attributes.
package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/apollo-chora/chora-common/observability"
	"github.com/apollo-chora/chora-common/tracing"
)

func TestSetAISpanAttributes_AdapterVersionAndGuardrail(t *testing.T) {
	_, span := observability.StartSpan(context.Background(), "gen_ai.guardrail")
	observability.SetAISpanAttributes(span, observability.AIAttrs{
		AdapterVersion:   "gemma-lora-tenant-v3",
		GuardrailOutcome: "block",
	})
	span.End()
}

// TestSlogHandler_TenantAndGCIDAttributes covers the traceHandler's
// tenant_id + gcid enrichment from the context helpers.
func TestSlogHandler_TenantAndGCIDAttributes(t *testing.T) {
	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	var buf bytes.Buffer
	logger := observability.NewSlogLogger(&buf)
	ctx := observability.WithTraceparent(context.Background(), tp)
	ctx = tracing.WithTenantID(ctx, "tenant-77")
	ctx = tracing.WithGCID(ctx, "gcid-77")
	logger.InfoContext(ctx, "rich log")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("decode log line: %v\nraw=%s", err, buf.String())
	}
	if entry["tenant_id"] != "tenant-77" {
		t.Errorf("tenant_id = %v, want tenant-77", entry["tenant_id"])
	}
	if entry["gcid"] != "gcid-77" {
		t.Errorf("gcid = %v, want gcid-77", entry["gcid"])
	}
}
