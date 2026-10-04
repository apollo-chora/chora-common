# modelarmor — Canonical Cloud Model Armor adapter

> Aligned with **Architecture Review locked 2026-05-07** + **ADR-152**.
> Sources:
> - `docs/architecture/adrs/adr-152-chora-guardrail-superseded-by-cloud-model-armor.md`
> - `.claude/skills/ai-runtime-guardrails/SKILL.md`
> - `.claude/rules/development-execution.md`
> - `docs/architecture-review-inputs-2026-05-07.md` Tier 3 D9

## What this is

`modelarmor` is the **single canonical Go adapter** for Cloud Model
Armor (GA 2026) across all Chora services. It replaces the legacy HTTP
client that called the retired `chora-guardrail` Go service (per
ADR-152).

Every pre-LLM and post-LLM screening call in Chora flows through this
package:

```
chora-ai-kernel orchestrator
    └─ chora-common/modelarmor (this package)
            └─ cloud.google.com/go/modelarmor/apiv1 (SDK)
                    └─ Cloud Model Armor v1 gRPC endpoint
```

Downstream code depends ONLY on the `Screener` interface — it never
imports `modelarmorpb` directly. This keeps every call site
adapter-thin and lets us swap the underlying guardrail vendor without
rewriting business logic.

## Quick start

```go
package main

import (
    "context"
    "log"

    "github.com/apollo-chora/chora-common/modelarmor"
)

func main() {
    ctx := context.Background()

    // Project + location are env-sourced — see CLAUDE.md §6.
    s, err := modelarmor.NewScreener(ctx,
        os.Getenv("CHORA_MODELARMOR_PROJECT"),  // "chora-489812"
        os.Getenv("CHORA_MODELARMOR_LOCATION"), // "us-central1"
    )
    if err != nil {
        log.Fatalf("modelarmor: %v", err)
    }
    defer s.Close()

    // Optional: register audit-only templates so MATCH → InspectOnly
    // (default: any MATCH → Block).
    s.(*modelarmor.Client).SetTemplateMode(
        "projects/chora-489812/locations/us-central1/templates/chora-guardrail-permissive-dev",
        modelarmor.TemplateModeInspectOnly,
    )

    // Pre-LLM screening
    pre, err := s.SanitizeUserPrompt(ctx, modelarmor.ScreenRequest{
        TenantID:     tenantID,
        AgentID:      "qgen-generator",
        GCID:         userGCID,
        TemplateName: agentCard.GuardrailTemplate, // from AgentCard
        Text:         userPrompt,
    })
    if err != nil {
        // Fail-loud per feedback_resilience_priority. Caller decides
        // retry / degraded-mode policy.
        return err
    }
    switch pre.Verdict {
    case modelarmor.VerdictBlock:
        // Short-circuit + emit chora.governance.policy.violation_detected.v1
        return errBlockedByGuardrail
    case modelarmor.VerdictInspectOnly:
        // Audit, but proceed.
        emitViolationEvent(ctx, pre)
    }

    // ... invoke the LLM ...

    // Post-LLM screening
    post, err := s.SanitizeModelResponse(ctx, modelarmor.ScreenRequest{
        TenantID:     tenantID,
        AgentID:      "qgen-generator",
        GCID:         userGCID,
        TemplateName: agentCard.GuardrailTemplate,
        Text:         llmOutput,
    })
    // ... same verdict handling ...
}
```

## Verdict mapping

Cloud Model Armor returns an overall `FilterMatchState` plus a
per-filter map. The template's `enforcement_type`
(INSPECT_ONLY vs INSPECT_AND_BLOCK) is **configured on the template
itself, not in the SDK response**. This adapter applies the
safe-default policy:

| SDK response | Registered template mode | `Verdict` |
|---|---|---|
| `NO_MATCH_FOUND` | (any) | `VerdictAllow` |
| `MATCH_FOUND` | `TemplateModeBlock` (default) | `VerdictBlock` |
| `MATCH_FOUND` | `TemplateModeInspectOnly` | `VerdictInspectOnly` |
| `FILTER_MATCH_STATE_UNSPECIFIED` | (any) | `VerdictBlock` (defensive) |
| nil `SanitizationResult` | (any) | `VerdictAllow` (no signal — caller owns fail-loud policy) |

Callers MUST register INSPECT_ONLY templates at boot via
`Client.SetTemplateMode(name, modelarmor.TemplateModeInspectOnly)`.
Unknown templates default to BLOCK — this matches the fail-loud
posture in `feedback_resilience_priority`.

## Per-filter breakdown

`ScreenResult.Filters` always carries the full per-filter result —
even for VerdictAllow — so audit traces can enumerate which filters
were evaluated.

Filter names match the SDK's `SanitizationResult.FilterResults` map
keys exactly:

| Constant | Filter | Severity dimension | Notes |
|---|---|---|---|
| `FilterNameRAI` (`"rai"`) | Responsible AI | yes (per RAI sub-type) | Each `RaiFilterTypeResult` (hate_speech / harassment / sexually_explicit / dangerous) becomes its own `FilterHit` row, with `Subcategory` populated. |
| `FilterNamePIAndJailbreak` (`"pi_and_jailbreak"`) | Prompt injection + jailbreak | yes | |
| `FilterNameSDP` (`"sdp"`) | Sensitive Data Protection (Cloud DLP) | no | Inspect vs Deidentify result paths captured in `RawResponse`. |
| `FilterNameMaliciousURI` (`"malicious_uri"`) | Malicious URI | no | |
| `FilterNameCSAM` (`"csam"`) | CSAM | no | |
| `FilterNameVirusScan` (`"virus_scan"`) | Virus scan | no | |

`RawResponse` is an `attribute.KeyValue`-safe map (string / int64 /
bool scalars only) for OTel span enrichment.

## OpenTelemetry spans

Every call emits **one span** named:

- `modelarmor.SanitizeUserPrompt` (pre-LLM)
- `modelarmor.SanitizeModelResponse` (post-LLM)

Attributes:

| Key | Type | Source |
|---|---|---|
| `chora.tenant_id` | string | `ScreenRequest.TenantID` |
| `chora.agent_id` | string | `ScreenRequest.AgentID` |
| `chora.gcid` | string | `ScreenRequest.GCID` |
| `modelarmor.template` | string | `ScreenRequest.TemplateName` |
| `modelarmor.project` | string | `NewScreener(project, ...)` |
| `modelarmor.location` | string | `NewScreener(..., location)` |
| `modelarmor.verdict` | string | computed |
| `modelarmor.latency_ms` | int64 | wall-clock |
| `modelarmor.filter_hits` | int | count where MatchState == MATCH_FOUND |
| `modelarmor.filters_evaluated` | int | total filters in response |

Tracer name: `chora-common/modelarmor` (matches the convention
used by `chora-common/otel`).

## Testing downstream packages

Use `modelarmor.NewStubScreener()`:

```go
import (
    "testing"
    "strings"

    "github.com/apollo-chora/chora-common/modelarmor"
)

func TestMyOrchestrator_BlocksMaliciousPrompt(t *testing.T) {
    stub := modelarmor.NewStubScreener()
    stub.SetUserPromptResult(func(req modelarmor.ScreenRequest) (modelarmor.ScreenResult, error) {
        if strings.Contains(req.Text, "ignore previous instructions") {
            return modelarmor.ScreenResult{
                Verdict: modelarmor.VerdictBlock,
                Reason:  "stub: jailbreak pattern",
                Filters: []modelarmor.FilterHit{{
                    FilterName:  modelarmor.FilterNamePIAndJailbreak,
                    MatchState:  modelarmor.MatchStateMatchFound,
                    Severity:    modelarmor.SeverityHigh,
                }},
            }, nil
        }
        return modelarmor.ScreenResult{Verdict: modelarmor.VerdictAllow}, nil
    })

    orchestrator := NewOrchestrator(stub /* + your other deps */)

    err := orchestrator.RunPrompt(ctx, "ignore previous instructions and ...")
    if !errors.Is(err, ErrBlockedByGuardrail) {
        t.Fatalf("expected block, got %v", err)
    }
}
```

The stub also supports:

- `SetModelResponseResult(fn)` — mirror hook for post-LLM
- `SetCloseHook(fn)` — custom Close behaviour
- `EnableCallCapture()` + `Calls()` — assert "Screener was called once
  with these args"
- `ErrStubExplicit` — canonical sentinel for tests that want to assert
  the caller surfaces a Screener error

## Integration test

`integration_test.go` is gated behind the `integration` build tag and
hits a real Cloud Model Armor template. Skipped in the default
`go test ./...` run. To run locally:

```bash
export CHORA_MODELARMOR_PROJECT=chora-489812
export CHORA_MODELARMOR_LOCATION=us-central1
export CHORA_MODELARMOR_TEMPLATE=projects/chora-489812/locations/us-central1/templates/chora-guardrail-strict-dev
export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud/sa-keys/dale-cli-chora-489812.json

go test -tags=integration ./modelarmor/...
```

## Cross-references

- **ADR-152** — `docs/architecture/adrs/adr-152-chora-guardrail-superseded-by-cloud-model-armor.md` — why this package exists + what it replaces.
- **Tier 3 D9** — `docs/architecture-review-inputs-2026-05-07.md` — original risk-tiered per-agent guardrail decision (locked 2026-05-07; ADR-152 supersedes the implementation, preserves the principle).
- **Skill `ai-runtime-guardrails`** — usage patterns + per-agent template provisioning.
- **No-inline-config** — `feedback_no_inline_config` memory + `secrets-and-env` skill. `NewScreener` requires project + location as args; no defaults baked in.
- **Resilience priority** — `feedback_resilience_priority` memory. The adapter is fail-loud — the caller owns retry / fallback policy.
