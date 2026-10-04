// Package agentengine is the shared Chora client for Vertex AI Agent Engine
// (Reasoning Engines) hosted on us-central1 per ADR-148.
//
// All Chora domain services that invoke a deployed Reasoning Engine
// (chora-consumption → Familiar/Recommender, chora-creation → QGen,
// chora-sharing → Moderation) MUST go through this package. It owns:
//
//   - Reasoning Engine REST contract (`:query` create-session, `:streamQuery`
//     SSE conversation turn, `:query` delete-session)
//   - SSE parsing of the per-event JSON envelope ADK emits
//   - Bearer-token acquisition via Google ADC / WIF (no SA keys, no inline)
//   - D6 P4 mandatory span attributes on every call site
//   - Typed errors for fail-loud 503 responses in callers
//
// Engine resource_names + region are env-driven (`no_inline_config`); a caller
// looking up its engine reads `<CREW>_ENGINE_RESOURCE` (e.g.
// `FAMILIAR_ENGINE_RESOURCE`) and passes it on each call.
//
// Aligned with:
//   - ADR-145 Outcome A (polyglot Vertex AI Agent Engine, 2026-05-12)
//   - ADR-148 (agentic resources us-central1; data plane stays Singapore)
//   - feedback_d6_resilience_first_class (P4 trace emission mandatory)
//   - feedback_no_inline_config
package agentengine
