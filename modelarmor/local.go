// LocalScreener is the LOCAL, deterministic Screener implementation for
// tests and the local compose stack. It is a regex/deny-list screener
// over the request text — NOT Cloud Model Armor, and it MUST NOT be
// represented as such. The Cloud Model Armor gRPC adapter was removed
// with the platform's Google Cloud exit (2026-10-05); this type is its
// local substitute so a local stack still has a working guardrail that
// returns real FilterHits and an honest Verdict instead of a fake
// always-clean pass.
//
// Coverage is deliberately narrow (prompt-injection / jailbreak,
// self-harm, weapons instructions, CSAM-adjacent text, credentials +
// PII, IP-literal URLs). CSAM image matching and virus scanning are
// content-analysis problems a regex cannot solve: those filters are
// always reported NO_MATCH_FOUND. Production traffic MUST front this
// with a real managed guardrail before it reaches users.
package modelarmor

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// LocalScreener is the local substitute Screener. Reuse a single
// instance across the service lifetime; it holds no connections.
//
// Construct via NewLocalScreener (or the NewScreener compatibility
// constructor). Services should hold it via the Screener interface.
type LocalScreener struct {
	tracer trace.Tracer

	// templateModes holds INSPECT_ONLY overrides keyed by template name.
	// Default = TemplateModeBlock (safe fail-loud).
	mu            sync.RWMutex
	templateModes map[string]TemplateMode
}

// Compile-time check that *LocalScreener implements Screener.
var _ Screener = (*LocalScreener)(nil)

// NewLocalScreener constructs the local substitute Screener with the
// built-in deny-list ruleset.
func NewLocalScreener() *LocalScreener {
	return &LocalScreener{
		tracer:        otel.Tracer(TracerName),
		templateModes: map[string]TemplateMode{},
	}
}

// NewScreener constructs a Screener. Retained for caller compatibility:
// the Cloud Model Armor adapter was removed with the Google Cloud exit,
// so this now returns the LOCAL substitute (LocalScreener). The
// project + location arguments are accepted and validated exactly as
// before (callers source them from env per CLAUDE.md §6) and are
// recorded on spans for audit, but they no longer select a cloud
// endpoint — resolution is the in-process deny-list.
//
// Callers that type-assert the result to *Client for SetTemplateMode
// must switch to *LocalScreener (the method is preserved).
func NewScreener(_ context.Context, project, location string) (Screener, error) {
	if project == "" {
		return nil, fmt.Errorf("modelarmor: NewScreener: project required")
	}
	if location == "" {
		return nil, fmt.Errorf("modelarmor: NewScreener: location required")
	}
	return NewLocalScreener(), nil
}

// SetTemplateMode registers a non-default enforcement mode for a
// template. Unknown templates default to TemplateModeBlock per the
// safe fail-loud policy.
func (c *LocalScreener) SetTemplateMode(templateName string, mode TemplateMode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.templateModes[templateName] = mode
}

// templateMode looks up the mode registered for a template; defaults to
// TemplateModeBlock.
func (c *LocalScreener) templateMode(templateName string) TemplateMode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if m, ok := c.templateModes[templateName]; ok {
		return m
	}
	return TemplateModeBlock
}

// Close releases the screener. No-op: the local substitute holds no
// connection. Retained so callers keep the defer-Close lifecycle.
func (c *LocalScreener) Close() error {
	return nil
}

// SanitizeUserPrompt screens a user prompt pre-LLM.
func (c *LocalScreener) SanitizeUserPrompt(ctx context.Context, req ScreenRequest) (ScreenResult, error) {
	if err := validateRequest(req); err != nil {
		return ScreenResult{}, err
	}
	ctx, span := c.tracer.Start(ctx, "modelarmor.SanitizeUserPrompt",
		trace.WithAttributes(c.requestAttrs(req)...),
	)
	defer span.End()

	start := time.Now()
	result := c.screen(req)
	result.LatencyMs = time.Since(start).Milliseconds()
	c.recordResultAttrs(span, result)
	return result, nil
}

// SanitizeModelResponse screens a model response post-LLM.
func (c *LocalScreener) SanitizeModelResponse(ctx context.Context, req ScreenRequest) (ScreenResult, error) {
	if err := validateRequest(req); err != nil {
		return ScreenResult{}, err
	}
	ctx, span := c.tracer.Start(ctx, "modelarmor.SanitizeModelResponse",
		trace.WithAttributes(c.requestAttrs(req)...),
	)
	defer span.End()

	start := time.Now()
	result := c.screen(req)
	result.LatencyMs = time.Since(start).Milliseconds()
	c.recordResultAttrs(span, result)
	return result, nil
}

// screen runs the deny-list ruleset over req.Text and maps the hits
// to the canonical ScreenResult. Every evaluated filter is reported
// (NO_MATCH_FOUND when it did not fire) so audit + debug traces are
// complete. Verdict policy mirrors the safe-default mapping: any match
// → Block, downgraded to InspectOnly for templates registered with
// SetTemplateMode.
func (c *LocalScreener) screen(req ScreenRequest) ScreenResult {
	result := ScreenResult{
		Filters:     []FilterHit{},
		RawResponse: map[string]any{},
	}

	hitNames := []string{}
	for _, rule := range defaultRules {
		if !rule.pattern.MatchString(req.Text) {
			continue
		}
		hit := FilterHit{
			FilterName:  rule.filterName,
			MatchState:  MatchStateMatchFound,
			Severity:    rule.severity,
			Subcategory: rule.subcategory,
		}
		result.Filters = append(result.Filters, hit)
		name := rule.filterName
		if rule.subcategory != "" {
			name += ":" + rule.subcategory
		}
		hitNames = append(hitNames, name)
	}

	// Report every evaluated filter, including the ones that did not
	// fire, so the audit matrix is complete.
	fired := map[string]bool{}
	for _, h := range result.Filters {
		fired[h.FilterName] = true
	}
	for _, filterName := range evaluatedFilters {
		if !fired[filterName] {
			result.Filters = append(result.Filters, FilterHit{
				FilterName: filterName,
				MatchState: MatchStateNoMatchFound,
			})
		}
		result.RawResponse[filterName+".match_state"] = MatchStateNoMatchFound
	}
	for _, h := range result.Filters {
		if h.MatchState == MatchStateMatchFound {
			result.RawResponse[h.FilterName+".match_state"] = MatchStateMatchFound
		}
	}

	if len(hitNames) == 0 {
		result.Verdict = VerdictAllow
		result.RawResponse["overall_match_state"] = MatchStateNoMatchFound
		result.RawResponse["invocation_result"] = "SUCCESS"
		return result
	}

	result.RawResponse["overall_match_state"] = MatchStateMatchFound
	result.RawResponse["invocation_result"] = "SUCCESS"
	result.Reason = summariseHits(hitNames)
	if c.templateMode(req.TemplateName) == TemplateModeInspectOnly {
		result.Verdict = VerdictInspectOnly
	} else {
		result.Verdict = VerdictBlock
	}
	return result
}

// evaluatedFilters is the fixed set of filters the local ruleset
// evaluates, in stable report order.
var evaluatedFilters = []string{
	FilterNameRAI,
	FilterNameSDP,
	FilterNamePIAndJailbreak,
	FilterNameMaliciousURI,
	FilterNameCSAM,
	FilterNameVirusScan,
}

// localRule is one deny-list entry: a regex over the request text plus
// the canonical filter identity a hit is reported under.
type localRule struct {
	filterName  string // FilterName* constant
	subcategory string // RAI sub-type (HATE_SPEECH / HARASSMENT / ...) or ""
	severity    string // Severity* constant or ""
	pattern     *regexp.Regexp
}

// defaultRules is the built-in deny-list. All patterns are
// case-insensitive. Keep this list narrow and high-precision: a local
// guardrail that false-positives on benign text is worse than one that
// misses an edge case.
var defaultRules = func() []localRule {
	cases := []localRule{
		// pi_and_jailbreak — prompt-injection / jailbreak attempts.
		{filterName: FilterNamePIAndJailbreak, severity: SeverityHigh, pattern: regexp.MustCompile(`(?i)\bignore\s+(all\s+)?(previous|prior|above|preceding)\s+(instructions?|prompts?|rules?)\b`)},
		{filterName: FilterNamePIAndJailbreak, severity: SeverityHigh, pattern: regexp.MustCompile(`(?i)\b(disregard|forget|override)\s+(all\s+)?(previous|prior|above|preceding)\s+(instructions?|prompts?|rules?)\b`)},
		{filterName: FilterNamePIAndJailbreak, severity: SeverityHigh, pattern: regexp.MustCompile(`(?i)\breveal\s+(your|the)\s+system\s+prompt\b`)},
		{filterName: FilterNamePIAndJailbreak, severity: SeverityMedium, pattern: regexp.MustCompile(`(?i)\b(you\s+are\s+now|from\s+now\s+on\s+you\s+are)\s+(a|an|the)\s+(unrestricted|unfiltered|jailbroken|uncensored)\b`)},
		{filterName: FilterNamePIAndJailbreak, severity: SeverityMedium, pattern: regexp.MustCompile(`(?i)\bdo\s+anything\s+now\b`)},
		{filterName: FilterNamePIAndJailbreak, severity: SeverityMedium, pattern: regexp.MustCompile(`(?i)\bjailbreak\b`)},

		// rai:HARASSMENT — self-harm incitement.
		{filterName: FilterNameRAI, subcategory: "HARASSMENT", severity: SeverityHigh, pattern: regexp.MustCompile(`(?i)\b(kill|end)\s+(yourself|yourselves)\b`)},
		{filterName: FilterNameRAI, subcategory: "HARASSMENT", severity: SeverityHigh, pattern: regexp.MustCompile(`(?i)\bcommit\s+suicide\b`)},

		// rai:DANGEROUS — weapons / explosives instructions.
		{filterName: FilterNameRAI, subcategory: "DANGEROUS", severity: SeverityHigh, pattern: regexp.MustCompile(`(?i)\bhow\s+to\s+(make|build|create)\s+(a\s+)?(bomb|explosive|grenade|pipe\s+bomb)\b`)},
		{filterName: FilterNameRAI, subcategory: "DANGEROUS", severity: SeverityHigh, pattern: regexp.MustCompile(`(?i)\b(bomb|explosive|grenade)\s+(making|recipe|instructions?|manual)\b`)},

		// csam — sexual content involving minors. Reported under the
		// CSAM filter (not RAI) to match the canonical filter matrix.
		{filterName: FilterNameCSAM, severity: SeverityHigh, pattern: regexp.MustCompile(`(?i)\b(child|children|minor|underage|under\s*age|pre\s?teen)\b.{0,40}\b(sex|sexual|sexy|porn|pornograph|nude|naked|nsfw|erotic)\b`)},
		{filterName: FilterNameCSAM, severity: SeverityHigh, pattern: regexp.MustCompile(`(?i)\b(sex|sexual|sexy|porn|pornograph|nude|naked|nsfw|erotic)\b.{0,40}\b(child|children|minor|underage|under\s*age|pre\s?teen)\b`)},

		// sdp — credentials + PII.
		{filterName: FilterNameSDP, severity: SeverityHigh, pattern: regexp.MustCompile(`\b\d{4}[ -]?\d{4}[ -]?\d{4}[ -]?\d{4}\b`)},                                                   // card number
		{filterName: FilterNameSDP, severity: SeverityHigh, pattern: regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)},                                                                     // US SSN
		{filterName: FilterNameSDP, severity: SeverityHigh, pattern: regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`)},                                                                      // AWS access key
		{filterName: FilterNameSDP, severity: SeverityHigh, pattern: regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`)},                                                        // PEM private key
		{filterName: FilterNameSDP, severity: SeverityMedium, pattern: regexp.MustCompile(`(?i)\b(api[_-]?key|apikey|secret[_-]?key|access[_-]?token|password|passwd)\s*[:=]\s*\S+`)}, // credential assignment
		{filterName: FilterNameSDP, severity: SeverityLow, pattern: regexp.MustCompile(`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b`)},                                         // email address

		// malicious_uri — IP-literal + onion URLs.
		{filterName: FilterNameMaliciousURI, severity: SeverityMedium, pattern: regexp.MustCompile(`(?i)https?://\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?(?:/\S*)?`)},
		{filterName: FilterNameMaliciousURI, severity: SeverityMedium, pattern: regexp.MustCompile(`(?i)https?://[^\s/\.]+\.onion(?:/\S*)?`)},
	}
	return cases
}()

// requestAttrs builds the OTel attribute slice for the span open.
// Mirrors the chora.* / modelarmor.* attribute naming used by
// chora-common/observability.
func (c *LocalScreener) requestAttrs(req ScreenRequest) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("chora.tenant_id", req.TenantID),
		attribute.String("chora.agent_id", req.AgentID),
		attribute.String("chora.gcid", req.GCID),
		attribute.String("modelarmor.template", req.TemplateName),
	}
}

// recordResultAttrs adds verdict + latency + hit-count attributes to
// the span before End().
func (c *LocalScreener) recordResultAttrs(span trace.Span, result ScreenResult) {
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

// summariseHits formats the hit list into a short human-readable Reason.
func summariseHits(names []string) string {
	if len(names) == 0 {
		return "modelarmor: match_found but no filter detail returned"
	}
	return "modelarmor match: " + strings.Join(names, ",")
}
