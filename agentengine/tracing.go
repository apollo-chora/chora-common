package agentengine

import "go.opentelemetry.io/otel/attribute"

// SpanAttrsInput is the input to SpanAttrs. All five fields are mandatory
// per the D6 P4 trace-emission contract. Pass empty strings if a value is
// unknown at the call site — the attr will still be stamped so dashboards
// can flag the gap.
type SpanAttrsInput struct {
	TenantID       string
	CrewKind       string // crew name from registry.json (familiar_companion, qgen_pipeline, content_recommender, content_moderation)
	EngineResource string // projects/.../reasoningEngines/NNN
	Model          string // resolved per-event model (tieredmodelplugin output)
	OutputTokens   int    // candidates_token_count from UsageMetadata
}

// SpanAttrs returns the five mandatory D6 P4 span attributes for every
// Reasoning Engine invocation. Callers stamp these on the span surrounding
// CreateSession + StreamQuery.
//
// Mandatory keys (D6 P4 contract; see feedback_d6_resilience_first_class):
//
//   - chora.tenant_id           — multi-tenant traceability
//   - chora.crew_kind           — which crew was invoked
//   - chora.engine_resource     — Vertex AI Reasoning Engine resource_name
//   - gen_ai.request.model      — resolved model per OpenInference convention
//   - gen_ai.usage.output_tokens — for cost-ledger reconciliation
//
// Callers should also add OTel GenAI semantic-convention attrs as available
// (e.g. gen_ai.usage.input_tokens, gen_ai.response.id) — but these five are
// the non-negotiable D6 minimum.
func SpanAttrs(in SpanAttrsInput) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("chora.tenant_id", in.TenantID),
		attribute.String("chora.crew_kind", in.CrewKind),
		attribute.String("chora.engine_resource", in.EngineResource),
		attribute.String("gen_ai.request.model", in.Model),
		attribute.Int("gen_ai.usage.output_tokens", in.OutputTokens),
	}
}
