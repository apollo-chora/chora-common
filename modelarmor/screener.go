// Concrete Cloud Model Armor adapter wrapping
// `cloud.google.com/go/modelarmor/apiv1`. See package doc for the
// contract.

package modelarmor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	modelarmor "cloud.google.com/go/modelarmor/apiv1"
	"cloud.google.com/go/modelarmor/apiv1/modelarmorpb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/option"
)

// regionalEndpoint returns the regional Cloud Model Armor gRPC endpoint for a
// location. Templates are REGIONAL resources: the default global endpoint
// (modelarmor.googleapis.com) returns TEMPLATE_NOT_FOUND when a regional
// template name is supplied, so every Screener MUST dial the regional host.
// Mirrors the regional pin already used by armorplugin + chora-model-gateway
// (feedback_model_armor_regional_endpoint).
func regionalEndpoint(location string) string {
	return fmt.Sprintf("modelarmor.%s.rep.googleapis.com:443", location)
}

// sdkInvoker is the narrow seam between Client and the SDK. The two
// function-pointer fields let tests substitute the SDK call without
// dialling Cloud Model Armor (the SDK's `*modelarmor.Client` has no
// usable interface — its methods take `gax.CallOption ...` directly).
type sdkInvoker struct {
	closer       func() error
	userPromptFn func(context.Context, *modelarmorpb.SanitizeUserPromptRequest) (*modelarmorpb.SanitizeUserPromptResponse, error)
	modelRespFn  func(context.Context, *modelarmorpb.SanitizeModelResponseRequest) (*modelarmorpb.SanitizeModelResponseResponse, error)
}

// newRealInvoker bridges *modelarmor.Client into the seam.
func newRealInvoker(sdk *modelarmor.Client) *sdkInvoker {
	return &sdkInvoker{
		closer: func() error { return sdk.Close() },
		userPromptFn: func(ctx context.Context, req *modelarmorpb.SanitizeUserPromptRequest) (*modelarmorpb.SanitizeUserPromptResponse, error) {
			return sdk.SanitizeUserPrompt(ctx, req)
		},
		modelRespFn: func(ctx context.Context, req *modelarmorpb.SanitizeModelResponseRequest) (*modelarmorpb.SanitizeModelResponseResponse, error) {
			return sdk.SanitizeModelResponse(ctx, req)
		},
	}
}

// Client is the production Screener. Reuse a single instance across the
// service lifetime — the underlying modelarmor.Client wraps a long-lived
// gRPC connection.
//
// Construct via `NewScreener`. The struct is exported so callers can
// declare typed receivers (e.g. metrics middleware) but should always
// hold it via the `Screener` interface.
type Client struct {
	project  string
	location string
	sdk      *sdkInvoker
	tracer   trace.Tracer

	// templateModes holds INSPECT_ONLY overrides keyed by full template
	// resource name. Default = TemplateModeBlock (safe fail-loud).
	mu            sync.RWMutex
	templateModes map[string]TemplateMode
}

// Compile-time check that *Client implements Screener.
var _ Screener = (*Client)(nil)

// NewScreener constructs a production Screener wired to the Cloud Model
// Armor v1 gRPC endpoint. Authentication uses Application Default
// Credentials (Workload Identity Federation in production; gcloud SA key
// in dev — see CLAUDE.md §9).
//
// No inline config: callers MUST pass `project` and `location` from
// env vars / Terraform.
func NewScreener(ctx context.Context, project, location string) (Screener, error) {
	if project == "" {
		return nil, fmt.Errorf("modelarmor: NewScreener: project required")
	}
	if location == "" {
		return nil, fmt.Errorf("modelarmor: NewScreener: location required")
	}
	// Dial the REGIONAL endpoint — templates are regional resources and the
	// default global endpoint 404s (TEMPLATE_NOT_FOUND) against them.
	sdk, err := modelarmor.NewClient(ctx, option.WithEndpoint(regionalEndpoint(location)))
	if err != nil {
		return nil, fmt.Errorf("modelarmor: NewScreener: new SDK client: %w", err)
	}
	return &Client{
		project:       project,
		location:      location,
		sdk:           newRealInvoker(sdk),
		tracer:        otel.Tracer(TracerName),
		templateModes: map[string]TemplateMode{},
	}, nil
}

// SetTemplateMode registers a non-default enforcement mode for a
// template. The SDK does not return the template's enforcement type
// in-band, so the adapter cannot derive INSPECT_ONLY automatically —
// callers (typically wired from an AgentCard at boot) MUST declare
// audit-only templates here.
//
// Unknown templates default to TemplateModeBlock per the safe fail-loud
// policy.
func (c *Client) SetTemplateMode(templateName string, mode TemplateMode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.templateModes[templateName] = mode
}

// templateMode looks up the mode registered for a template; defaults to
// TemplateModeBlock.
func (c *Client) templateMode(templateName string) TemplateMode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if m, ok := c.templateModes[templateName]; ok {
		return m
	}
	return TemplateModeBlock
}

// SanitizeUserPrompt screens a user prompt pre-LLM.
func (c *Client) SanitizeUserPrompt(ctx context.Context, req ScreenRequest) (ScreenResult, error) {
	if err := validateRequest(req); err != nil {
		return ScreenResult{}, err
	}
	ctx, span := c.tracer.Start(ctx, "modelarmor.SanitizeUserPrompt",
		trace.WithAttributes(c.requestAttrs(req)...),
	)
	defer span.End()

	start := time.Now()
	resp, err := c.sdk.userPromptFn(ctx, &modelarmorpb.SanitizeUserPromptRequest{
		Name: req.TemplateName,
		UserPromptData: &modelarmorpb.DataItem{
			DataItem: &modelarmorpb.DataItem_Text{Text: req.Text},
		},
	})
	latencyMs := time.Since(start).Milliseconds()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "modelarmor sanitize user prompt failed")
		return ScreenResult{}, fmt.Errorf("modelarmor: SanitizeUserPrompt: %w", err)
	}

	result := c.toScreenResult(resp.GetSanitizationResult(), req.TemplateName, latencyMs)
	c.recordResultAttrs(span, result)
	return result, nil
}

// SanitizeModelResponse screens a model response post-LLM.
func (c *Client) SanitizeModelResponse(ctx context.Context, req ScreenRequest) (ScreenResult, error) {
	if err := validateRequest(req); err != nil {
		return ScreenResult{}, err
	}
	ctx, span := c.tracer.Start(ctx, "modelarmor.SanitizeModelResponse",
		trace.WithAttributes(c.requestAttrs(req)...),
	)
	defer span.End()

	start := time.Now()
	resp, err := c.sdk.modelRespFn(ctx, &modelarmorpb.SanitizeModelResponseRequest{
		Name: req.TemplateName,
		ModelResponseData: &modelarmorpb.DataItem{
			DataItem: &modelarmorpb.DataItem_Text{Text: req.Text},
		},
	})
	latencyMs := time.Since(start).Milliseconds()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "modelarmor sanitize model response failed")
		return ScreenResult{}, fmt.Errorf("modelarmor: SanitizeModelResponse: %w", err)
	}

	result := c.toScreenResult(resp.GetSanitizationResult(), req.TemplateName, latencyMs)
	c.recordResultAttrs(span, result)
	return result, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	if c == nil || c.sdk == nil || c.sdk.closer == nil {
		return nil
	}
	return c.sdk.closer()
}

// toScreenResult converts the SDK SanitizationResult into the canonical
// Chora ScreenResult. See package doc for the verdict-mapping rules.
func (c *Client) toScreenResult(sr *modelarmorpb.SanitizationResult, templateName string, latencyMs int64) ScreenResult {
	result := ScreenResult{
		Filters:     []FilterHit{},
		LatencyMs:   latencyMs,
		RawResponse: map[string]any{},
	}
	if sr == nil {
		// Empty response from SDK — treat as Allow per fail-open audit
		// principle (no MATCH_FOUND signal received). Caller still owns
		// fail-loud policy.
		result.Verdict = VerdictAllow
		return result
	}

	hits, hitNames, raw := flattenFilterResults(sr.GetFilterResults())
	result.Filters = hits
	result.RawResponse = raw
	result.RawResponse["overall_match_state"] = sr.GetFilterMatchState().String()
	result.RawResponse["invocation_result"] = sr.GetInvocationResult().String()

	switch sr.GetFilterMatchState() {
	case modelarmorpb.FilterMatchState_NO_MATCH_FOUND:
		result.Verdict = VerdictAllow
	case modelarmorpb.FilterMatchState_MATCH_FOUND:
		if c.templateMode(templateName) == TemplateModeInspectOnly {
			result.Verdict = VerdictInspectOnly
		} else {
			result.Verdict = VerdictBlock
		}
		result.Reason = summariseHits(hitNames)
	default:
		// FILTER_MATCH_STATE_UNSPECIFIED — defensive fail-loud: treat as
		// Block. The caller's policy can downgrade if its audit shows
		// the unspecified state is normal for the chosen template.
		result.Verdict = VerdictBlock
		result.Reason = "modelarmor returned FILTER_MATCH_STATE_UNSPECIFIED"
	}
	return result
}

// flattenFilterResults projects the SDK per-filter map into the
// canonical FilterHit slice + RawResponse map. The slice preserves a
// stable order (alphabetical by filter name) so traces + audit logs
// diff cleanly across runs.
func flattenFilterResults(in map[string]*modelarmorpb.FilterResult) ([]FilterHit, []string, map[string]any) {
	hits := make([]FilterHit, 0, len(in))
	hitNames := make([]string, 0, len(in))
	raw := map[string]any{}

	// Stable order — alphabetical by filter name.
	names := make([]string, 0, len(in))
	for k := range in {
		names = append(names, k)
	}
	sortStrings(names)

	for _, name := range names {
		fr := in[name]
		if fr == nil {
			continue
		}
		hit := FilterHit{FilterName: name}
		switch name {
		case FilterNameRAI:
			rai := fr.GetRaiFilterResult()
			if rai == nil {
				continue
			}
			hit.MatchState = rai.GetMatchState().String()
			// Surface per-RAI-type breakdown into hits. Each sub-type
			// becomes its own FilterHit so audit traces enumerate hate /
			// harassment / sexually_explicit / dangerous independently.
			subNames := make([]string, 0, len(rai.GetRaiFilterTypeResults()))
			for subName := range rai.GetRaiFilterTypeResults() {
				subNames = append(subNames, subName)
			}
			sortStrings(subNames)
			for _, subName := range subNames {
				sub := rai.GetRaiFilterTypeResults()[subName]
				subHit := FilterHit{
					FilterName:  FilterNameRAI,
					MatchState:  sub.GetMatchState().String(),
					Severity:    severityFromConfidence(sub.GetConfidenceLevel()),
					Subcategory: strings.ToUpper(subName),
				}
				if sub.GetMatchState() == modelarmorpb.FilterMatchState_MATCH_FOUND {
					hitNames = append(hitNames, FilterNameRAI+":"+subHit.Subcategory)
				}
				hits = append(hits, subHit)
				raw["rai."+subName+".match_state"] = subHit.MatchState
				raw["rai."+subName+".severity"] = subHit.Severity
			}
			if rai.GetMatchState() == modelarmorpb.FilterMatchState_MATCH_FOUND && len(rai.GetRaiFilterTypeResults()) == 0 {
				hitNames = append(hitNames, FilterNameRAI)
			}
			raw["rai.match_state"] = hit.MatchState
			// Skip pushing the parent "rai" hit when sub-hits already
			// captured the detail — avoids double counting in audit.
			continue
		case FilterNamePIAndJailbreak:
			pj := fr.GetPiAndJailbreakFilterResult()
			if pj == nil {
				continue
			}
			hit.MatchState = pj.GetMatchState().String()
			hit.Severity = severityFromConfidence(pj.GetConfidenceLevel())
			raw["pi_and_jailbreak.match_state"] = hit.MatchState
			raw["pi_and_jailbreak.severity"] = hit.Severity
		case FilterNameSDP:
			sdp := fr.GetSdpFilterResult()
			if sdp == nil {
				continue
			}
			if ir := sdp.GetInspectResult(); ir != nil {
				hit.MatchState = ir.GetMatchState().String()
				raw["sdp.match_state"] = hit.MatchState
				raw["sdp.findings_count"] = int64(len(ir.GetFindings()))
			} else if dr := sdp.GetDeidentifyResult(); dr != nil {
				hit.MatchState = dr.GetMatchState().String()
				raw["sdp.match_state"] = hit.MatchState
				raw["sdp.deidentified_bytes"] = dr.GetTransformedBytes()
			}
		case FilterNameMaliciousURI:
			mu := fr.GetMaliciousUriFilterResult()
			if mu == nil {
				continue
			}
			hit.MatchState = mu.GetMatchState().String()
			raw["malicious_uri.match_state"] = hit.MatchState
			raw["malicious_uri.matched_count"] = int64(len(mu.GetMaliciousUriMatchedItems()))
		case FilterNameCSAM:
			cs := fr.GetCsamFilterFilterResult()
			if cs == nil {
				continue
			}
			hit.MatchState = cs.GetMatchState().String()
			raw["csam.match_state"] = hit.MatchState
		case FilterNameVirusScan:
			vs := fr.GetVirusScanFilterResult()
			if vs == nil {
				continue
			}
			hit.MatchState = vs.GetMatchState().String()
			raw["virus_scan.match_state"] = hit.MatchState
			raw["virus_scan.virus_count"] = int64(len(vs.GetVirusDetails()))
		default:
			// Unknown filter key — keep the hit but mark match state as
			// the SDK's default (UNSPECIFIED) so audit captures it.
			hit.MatchState = MatchStateUnspecified
			raw[name+".match_state"] = hit.MatchState
		}

		if hit.MatchState == MatchStateMatchFound {
			hitNames = append(hitNames, name)
		}
		hits = append(hits, hit)
	}
	return hits, hitNames, raw
}

// severityFromConfidence maps DetectionConfidenceLevel → canonical
// Severity strings.
func severityFromConfidence(c modelarmorpb.DetectionConfidenceLevel) string {
	switch c {
	case modelarmorpb.DetectionConfidenceLevel_LOW_AND_ABOVE:
		return SeverityLow
	case modelarmorpb.DetectionConfidenceLevel_MEDIUM_AND_ABOVE:
		return SeverityMedium
	case modelarmorpb.DetectionConfidenceLevel_HIGH:
		return SeverityHigh
	default:
		return ""
	}
}

// summariseHits formats the hit list into a short human-readable Reason.
func summariseHits(names []string) string {
	if len(names) == 0 {
		return "modelarmor: match_found but no filter detail returned"
	}
	return "modelarmor match: " + strings.Join(names, ",")
}

// requestAttrs builds the OTel attribute slice for the span open.
// Mirrors the chora.* / modelarmor.* attribute naming used by
// `libs/chora-go-common/observability`.
func (c *Client) requestAttrs(req ScreenRequest) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("chora.tenant_id", req.TenantID),
		attribute.String("chora.agent_id", req.AgentID),
		attribute.String("chora.gcid", req.GCID),
		attribute.String("modelarmor.template", req.TemplateName),
		attribute.String("modelarmor.project", c.project),
		attribute.String("modelarmor.location", c.location),
	}
}

// recordResultAttrs adds verdict + latency + hit-count attributes to the
// span before End().
func (c *Client) recordResultAttrs(span trace.Span, result ScreenResult) {
	hitCount := 0
	for _, h := range result.Filters {
		if h.MatchState == MatchStateMatchFound {
			hitCount++
		}
	}
	span.SetAttributes(
		attribute.String("modelarmor.verdict", string(result.Verdict)),
		attribute.Int64("modelarmor.latency_ms", result.LatencyMs),
		attribute.Int("modelarmor.filter_hits", hitCount),
		attribute.Int("modelarmor.filters_evaluated", len(result.Filters)),
	)
	if result.Verdict != VerdictAllow {
		span.SetStatus(codes.Ok, string(result.Verdict))
	}
}

// sortStrings is a small helper (stdlib `sort.Strings`) — kept local so
// the file's import list stays explicit + the rest of the package
// doesn't grow a sort dependency.
func sortStrings(s []string) {
	// insertion sort — n is small (<=6 filter names), avoids the
	// `sort` import to keep the impl footprint minimal.
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
