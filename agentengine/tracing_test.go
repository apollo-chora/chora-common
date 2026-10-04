package agentengine

import (
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func TestSpanAttrs_AllFiveD6P4MandatoryAttributes(t *testing.T) {
	attrs := SpanAttrs(SpanAttrsInput{
		TenantID:       "tenant-abc",
		CrewKind:       "familiar_companion",
		EngineResource: "projects/381315455325/locations/us-central1/reasoningEngines/6115726116004036608",
		Model:          "gemini-2.5-flash-lite",
		OutputTokens:   1303,
	})

	want := map[attribute.Key]attribute.Value{
		"chora.tenant_id":           attribute.StringValue("tenant-abc"),
		"chora.crew_kind":           attribute.StringValue("familiar_companion"),
		"chora.engine_resource":     attribute.StringValue("projects/381315455325/locations/us-central1/reasoningEngines/6115726116004036608"),
		"gen_ai.request.model":      attribute.StringValue("gemini-2.5-flash-lite"),
		"gen_ai.usage.output_tokens": attribute.IntValue(1303),
	}

	got := map[attribute.Key]attribute.Value{}
	for _, kv := range attrs {
		got[kv.Key] = kv.Value
	}

	for k, v := range want {
		gv, ok := got[k]
		if !ok {
			t.Errorf("missing attribute %q", k)
			continue
		}
		if gv != v {
			t.Errorf("attribute %q: got %v, want %v", k, gv.Emit(), v.Emit())
		}
	}
	if len(got) < len(want) {
		t.Errorf("attribute count: got %d, want at least %d", len(got), len(want))
	}
}

func TestSpanAttrs_EmptyOutputTokensStillIncluded(t *testing.T) {
	// A pre-terminal partial chunk has OutputTokens=0; the span attr must
	// still be emitted (it's a contract field).
	attrs := SpanAttrs(SpanAttrsInput{
		TenantID:       "t",
		CrewKind:       "x",
		EngineResource: "y",
		Model:          "z",
		OutputTokens:   0,
	})
	gotOutput := false
	for _, kv := range attrs {
		if kv.Key == "gen_ai.usage.output_tokens" {
			gotOutput = true
			if kv.Value.AsInt64() != 0 {
				t.Errorf("OutputTokens attr value: got %d, want 0", kv.Value.AsInt64())
			}
		}
	}
	if !gotOutput {
		t.Error("gen_ai.usage.output_tokens attr missing when OutputTokens=0")
	}
}
