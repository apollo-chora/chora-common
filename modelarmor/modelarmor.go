// Package modelarmor is the canonical Chora guardrail adapter.
//
// Per ADR-152 (`docs/architecture/adrs/adr-152-chora-guardrail-superseded-by-cloud-model-armor.md`):
// this package REPLACED the legacy HTTP client that called the retired
// `chora-guardrail` Go service. Pre-LLM and post-LLM screening calls
// flow from the LLM-issuing service (Model Broker Gateway, or its
// post-ADR-146 successor — chora-ai-kernel orchestrator) through the
// Screener port.
//
// 2026-10-05: HISTORY — the Cloud Model Armor gRPC adapter
// (`cloud.google.com/go/modelarmor/apiv1`) was removed with the
// platform's Google Cloud exit. The package now ships a LOCAL,
// deterministic substitute — LocalScreener, a regex/deny-list screener
// over the request text. It is NOT Cloud Model Armor and MUST NOT be
// represented as such; it exists so tests and the local compose stack
// have a working guardrail that returns real FilterHits and an honest
// Verdict instead of a fake always-clean pass.
//
// The package exposes a narrow port (`Screener`) plus implementations:
//
//   - `LocalScreener` — the local regex/deny-list substitute (default).
//   - `StubScreener` — in-process stub for downstream-package unit tests
//     (no network).
//
// Both speak in canonical Chora types (`ScreenRequest`, `ScreenResult`,
// `Verdict`, `FilterHit`) so downstream services NEVER import guardrail
// vendor SDK types directly. This keeps every call site adapter-thin
// and lets us swap the underlying guardrail vendor without rewriting
// business logic (per ADR-152 §"vendor lock-in mitigation").
//
// # Verdict mapping
//
// The screener returns an overall match state (MATCH_FOUND or
// NO_MATCH_FOUND) plus a per-filter map. The template's
// `enforcement_type` (INSPECT_ONLY vs INSPECT_AND_BLOCK) is configured
// on the template, NOT returned in the response.
//
// Safe-default policy (per the task contract):
//
//   - NO_MATCH_FOUND  → VerdictAllow.
//   - MATCH_FOUND     → VerdictBlock. The caller is expected to honour the
//     block; downstream policy can override to InspectOnly if the agent's
//     AgentCard declares the template as `enforcement_type: INSPECT_ONLY`.
//     `LocalScreener.SetTemplateMode(name, mode)` lets the caller register
//     INSPECT_ONLY templates so the adapter emits VerdictInspectOnly
//     instead of VerdictBlock. Unknown templates default to BLOCK
//     (fail-loud per `feedback_resilience_priority`).
//
// # Tracing
//
// Every call emits one OTel span named `modelarmor.SanitizeUserPrompt` or
// `modelarmor.SanitizeModelResponse` with attributes:
// `chora.tenant_id`, `chora.agent_id`, `chora.gcid`, `modelarmor.template`,
// `modelarmor.verdict`, `modelarmor.latency_ms`, `modelarmor.filter_hits`.
// Tracer name: `chora-common/modelarmor` (mirrors the otel.Tracer
// convention used in `chora-common/otel`).
//
// # No inline config
//
// `NewScreener` requires `project` + `location` arguments — callers MUST
// source these from env vars / Terraform per CLAUDE.md §6 + the
// `secrets-and-env` skill. The arguments are validated and recorded on
// spans for audit but no longer select a cloud endpoint. The template
// name is passed per-call in `ScreenRequest.TemplateName` so a single
// screener can fan out across multiple templates per the risk-tiered
// per-agent principle (Tier 3 D9 preserved by ADR-152).
package modelarmor

import (
	"context"
	"errors"
)

// Verdict is the canonical Chora screening outcome. Downstream code
// switches on this; it never inspects the raw SDK response.
type Verdict string

const (
	// VerdictAllow — Cloud Model Armor returned NO_MATCH_FOUND across all
	// enabled filters. Caller MAY proceed with the LLM call (pre) or
	// return the response to the user (post).
	VerdictAllow Verdict = "ALLOW"

	// VerdictBlock — Cloud Model Armor returned MATCH_FOUND and the
	// template's enforcement is INSPECT_AND_BLOCK (default). Caller MUST
	// short-circuit the LLM call (pre) or return a sanitised error to
	// the user (post) and emit `chora.governance.policy.violation_detected.v1`.
	VerdictBlock Verdict = "BLOCK"

	// VerdictInspectOnly — Cloud Model Armor returned MATCH_FOUND but
	// the template's enforcement is INSPECT_ONLY (audit only). Caller
	// MAY proceed but MUST still emit
	// `chora.governance.policy.violation_detected.v1` for audit.
	VerdictInspectOnly Verdict = "INSPECT_ONLY"
)

// TemplateMode declares whether a template is INSPECT_ONLY or
// INSPECT_AND_BLOCK. Used by `LocalScreener.SetTemplateMode` so the
// screener can downgrade VerdictBlock → VerdictInspectOnly for
// audit-only templates.
type TemplateMode string

const (
	// TemplateModeBlock — match → VerdictBlock (default; safe fail-loud).
	TemplateModeBlock TemplateMode = "INSPECT_AND_BLOCK"

	// TemplateModeInspectOnly — match → VerdictInspectOnly (audit only).
	TemplateModeInspectOnly TemplateMode = "INSPECT_ONLY"
)

// Canonical filter-name constants matching the keys a managed
// guardrail returns in its per-filter result map (lowercase per the
// Cloud Model Armor proto definition, retained as the platform's
// canonical filter vocabulary). Downstream policy code MUST switch on
// these constants — never on raw strings — so a filter-key rename
// surfaces at compile time.
const (
	FilterNameRAI            = "rai"
	FilterNameSDP            = "sdp"
	FilterNamePIAndJailbreak = "pi_and_jailbreak"
	FilterNameMaliciousURI   = "malicious_uri"
	FilterNameCSAM           = "csam"
	FilterNameVirusScan      = "virus_scan"
)

// Canonical filter-match-state strings emitted in FilterHit.MatchState
// (proto enum values; uppercase per the SDK).
const (
	MatchStateMatchFound   = "MATCH_FOUND"
	MatchStateNoMatchFound = "NO_MATCH_FOUND"
	MatchStateUnspecified  = "FILTER_MATCH_STATE_UNSPECIFIED"
)

// Canonical severity strings derived from a managed guardrail's
// detection-confidence levels (LOW_AND_ABOVE/MEDIUM_AND_ABOVE/HIGH).
// Empty string when the filter has no confidence dimension (e.g. CSAM,
// MaliciousURI return only match_state).
const (
	SeverityLow    = "LOW"
	SeverityMedium = "MEDIUM"
	SeverityHigh   = "HIGH"
)

// ScreenRequest is the canonical input to a single screening call. All
// fields are required EXCEPT GCID (may be empty for system-initiated
// calls that don't have a user identity attached).
type ScreenRequest struct {
	// TenantID — Chora tenant under which the call is made. Recorded in
	// the OTel span and propagated to the violation event if the verdict
	// is non-Allow. Required.
	TenantID string

	// AgentID — Chora agent identifier (e.g. `qgen-generator`,
	// `familiar-companion`). Recorded in the OTel span and used by
	// downstream policy to route violation events. Required.
	AgentID string

	// GCID — Global Chora ID of the user whose prompt/response is being
	// screened. Optional (empty when the call is system-initiated).
	GCID string

	// TemplateName — the guardrail template identifier the caller
	// selected for this agent (historically a full Cloud Model Armor
	// template resource name, e.g.
	// `projects/chora-489812/locations/us-central1/templates/chora-guardrail-strict-dev`).
	// The LLM-issuing service reads this from the AgentCard per
	// ADR-152 §"Risk-tiered per-agent (D9 preserved)". Required.
	TemplateName string

	// Text — the prompt (for SanitizeUserPrompt) or response (for
	// SanitizeModelResponse) payload to screen. Required + non-empty.
	Text string
}

// ScreenResult is the canonical output of a single screening call.
type ScreenResult struct {
	// Verdict — Allow / Block / InspectOnly. See package doc for the
	// safe-default mapping policy.
	Verdict Verdict

	// Reason — short human-readable summary of which filter(s) hit.
	// Empty when Verdict == VerdictAllow.
	Reason string

	// Filters — per-filter breakdown for audit. Always populated (even
	// for Allow) so traces capture the full screening matrix.
	Filters []FilterHit

	// LatencyMs — wall-clock time of the screening call.
	LatencyMs int64

	// RawResponse — opaque map for downstream debug + trace span
	// attribute serialisation. Keys are stable filter names + match
	// states; values are primitive scalars (string, int64, bool) to
	// keep them attribute-safe.
	RawResponse map[string]any
}

// FilterHit captures one filter's outcome. Filters that didn't fire are
// still included (with MatchState == NO_MATCH_FOUND) so audit + debug
// traces are complete.
type FilterHit struct {
	// FilterName — one of the FilterName* constants
	// (rai / sdp / pi_and_jailbreak / malicious_uri / csam / virus_scan).
	FilterName string

	// MatchState — one of the MatchState* constants. Always populated.
	MatchState string

	// Severity — one of the Severity* constants. Empty when the filter
	// has no confidence dimension (CSAM / MaliciousURI / VirusScan).
	Severity string

	// Subcategory — RAI sub-filter type when FilterName == "rai":
	// `HATE_SPEECH`, `HARASSMENT`, `SEXUALLY_EXPLICIT`, `DANGEROUS`.
	// Empty for non-RAI filters.
	Subcategory string
}

// Screener is the port. Downstream code depends ONLY on this interface —
// never on a concrete implementation. Tests inject `StubScreener`.
//
// Both methods are sync within the caller's request span (no fan-out per
// ADR-152 §"LLM-issuing service wiring"). Errors are fail-loud per
// `feedback_resilience_priority` — the caller wraps with its own
// retry / fallback policy (typically: log + emit a degraded-screening
// violation event + fail the LLM call).
type Screener interface {
	// SanitizeUserPrompt screens a user prompt BEFORE the LLM call.
	SanitizeUserPrompt(ctx context.Context, req ScreenRequest) (ScreenResult, error)

	// SanitizeModelResponse screens a model response AFTER the LLM call.
	SanitizeModelResponse(ctx context.Context, req ScreenRequest) (ScreenResult, error)

	// Close releases the underlying gRPC connection. Caller MUST defer
	// this during graceful shutdown.
	Close() error
}

// TracerName is the OTel tracer name used by every span emitted from
// this package. Exposed for tests that need to capture spans.
const TracerName = "chora-common/modelarmor"

// ErrInvalidScreenRequest is returned when a ScreenRequest fails the
// minimal-shape validation. Wrapped per call so callers can use
// errors.Is.
var ErrInvalidScreenRequest = errors.New("modelarmor: invalid ScreenRequest")

// validateRequest enforces the minimal-shape contract before issuing
// the SDK call. Fail-loud per `feedback_resilience_priority`.
func validateRequest(req ScreenRequest) error {
	if req.TenantID == "" {
		return errors.Join(ErrInvalidScreenRequest, errors.New("TenantID required"))
	}
	if req.AgentID == "" {
		return errors.Join(ErrInvalidScreenRequest, errors.New("AgentID required"))
	}
	if req.TemplateName == "" {
		return errors.Join(ErrInvalidScreenRequest, errors.New("TemplateName required"))
	}
	if req.Text == "" {
		return errors.Join(ErrInvalidScreenRequest, errors.New("Text required"))
	}
	return nil
}
