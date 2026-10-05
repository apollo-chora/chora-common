# modelarmor — Canonical Chora guardrail adapter

> Aligned with **Architecture Review locked 2026-05-07** + **ADR-152**.
> Sources:
> - `docs/architecture/adrs/adr-152-chora-guardrail-superseded-by-cloud-model-armor.md`
> - `.claude/skills/ai-runtime-guardrails/SKILL.md`
> - `.claude/rules/development-execution.md`
> - `docs/architecture-review-inputs-2026-05-07.md` Tier 3 D9

## What this is

`modelarmor` is the **single canonical Go guardrail adapter** across
all Chora services. It replaces the legacy HTTP client that called the
retired `chora-guardrail` Go service (per ADR-152).

**2026-10-05: the Cloud Model Armor gRPC adapter
(`cloud.google.com/go/modelarmor/apiv1`) was removed with the platform's
Google Cloud exit.** The package now ships a LOCAL, deterministic
substitute — `LocalScreener`, a regex/deny-list screener over the
request text. It is **NOT Cloud Model Armor** and MUST NOT be
represented as such. It exists so tests and the local compose stack
have a working guardrail that returns real `FilterHit`s and an honest
`Verdict` instead of a fake always-clean pass.

Every pre-LLM and post-LLM screening call in Chora flows through this
package:

```
chora-ai-kernel orchestrator
    └─ chora-common/modelarmor (this package)
            └─ LocalScreener (regex/deny-list, in-process)
```

Downstream code depends ONLY on the `Screener` interface — it never
imports a guardrail vendor SDK directly. This keeps every call site
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
    // They are validated + recorded on spans for audit but no longer
    // select a cloud endpoint.
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
    s.(*modelarmor.LocalScreener).SetTemplateMode(
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

The screener returns an overall match state plus a per-filter map. The
template's `enforcement_type` (INSPECT_ONLY vs INSPECT_AND_BLOCK) is
**configured by the caller, not returned in-band**. The local
substitute applies the safe-default policy:

| Match state | Registered template mode | `Verdict` |
|---|---|---|
| `NO_MATCH_FOUND` | (any) | `VerdictAllow` |
| `MATCH_FOUND` | `TemplateModeBlock` (default) | `VerdictBlock` |
| `MATCH_FOUND` | `TemplateModeInspectOnly` | `VerdictInspectOnly` |

Callers MUST register INSPECT_ONLY templates at boot via
`LocalScreener.SetTemplateMode(name, modelarmor.TemplateModeInspectOnly)`.
Unknown templates default to BLOCK — this matches the fail-loud
posture in `feedback_resilience_priority`.

## Per-filter breakdown

`ScreenResult.Filters` always carries the full per-filter result —
even for VerdictAllow — so audit traces can enumerate which filters
were evaluated.

Filter names follow the canonical guardrail filter vocabulary (the
Cloud Model Armor proto key names, retained as the platform standard):

| Constant | Filter | Local coverage |
|---|---|---|
| `FilterNameRAI` (`"rai"`) | Responsible AI | HARASSMENT (self-harm incitement) + DANGEROUS (weapons/explosives instructions) sub-categories, each its own `FilterHit` row with `Subcategory` populated. |
| `FilterNamePIAndJailbreak` (`"pi_and_jailbreak"`) | Prompt injection + jailbreak | ignore/disregard/override previous instructions, system-prompt reveal, "you are now …", DAN, "jailbreak". |
| `FilterNameSDP` (`"sdp"`) | Sensitive Data Protection | card numbers, US SSN, AWS access keys, PEM private keys, credential assignments, email addresses. |
| `FilterNameMaliciousURI` (`"malicious_uri"`) | Malicious URI | IP-literal URLs, `.onion` URLs. |
| `FilterNameCSAM` (`"csam"`) | CSAM | sexual content involving minors (text patterns only — no image analysis). |
| `FilterNameVirusScan` (`"virus_scan"`) | Virus scan | **Not covered** — content scanning is not a regex problem; always `NO_MATCH_FOUND`. |

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

## Cross-references

- **ADR-152** — `docs/architecture/adrs/adr-152-chora-guardrail-superseded-by-cloud-model-armor.md` — why this package exists + what it replaces.
- **Tier 3 D9** — `docs/architecture-review-inputs-2026-05-07.md` — original risk-tiered per-agent guardrail decision (locked 2026-05-07; ADR-152 supersedes the implementation, preserves the principle).
- **Skill `ai-runtime-guardrails`** — usage patterns + per-agent template provisioning.
- **No-inline-config** — `feedback_no_inline_config` memory + `secrets-and-env` skill. `NewScreener` requires project + location as args; no defaults baked in.
- **Resilience priority** — `feedback_resilience_priority` memory. The screener is fail-loud — the caller owns retry / fallback policy.
