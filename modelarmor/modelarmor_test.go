// Unit tests for the modelarmor adapter. NO network — every test
// exercises either the StubScreener or constructs a Client with a
// hand-rolled sdkInvoker so toScreenResult / flattenFilterResults /
// verdict mapping can be driven without dialling Cloud Model Armor.

package modelarmor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/modelarmor/apiv1/modelarmorpb"
	"go.opentelemetry.io/otel"
)

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

// newTestClient returns a Client wired with a hand-rolled sdkInvoker
// (no network). The closure functions can be overridden per test.
func newTestClient(t *testing.T) *Client {
	t.Helper()
	return &Client{
		project:       "chora-test",
		location:      "us-central1",
		sdk:           &sdkInvoker{closer: func() error { return nil }},
		tracer:        otel.Tracer(TracerName),
		templateModes: map[string]TemplateMode{},
	}
}

// stubResponse builds a SanitizationResult for tests.
func stubResponse(overall modelarmorpb.FilterMatchState, filters map[string]*modelarmorpb.FilterResult) *modelarmorpb.SanitizationResult {
	return &modelarmorpb.SanitizationResult{
		FilterMatchState: overall,
		FilterResults:    filters,
		InvocationResult: modelarmorpb.InvocationResult_SUCCESS,
	}
}

const validTemplate = "projects/chora-test/locations/us-central1/templates/chora-guardrail-strict-dev"

func validReq(text string) ScreenRequest {
	return ScreenRequest{
		TenantID:     "tenant-uuid-v7-aaa",
		AgentID:      "qgen-generator",
		GCID:         "gcid-uuid-v7-bbb",
		TemplateName: validTemplate,
		Text:         text,
	}
}

// ---------------------------------------------------------------------
// validateRequest
// ---------------------------------------------------------------------

func TestValidateRequest_RejectsMissingFields(t *testing.T) {
	cases := []struct {
		name string
		req  ScreenRequest
	}{
		{"missing tenant", ScreenRequest{AgentID: "a", TemplateName: "t", Text: "x"}},
		{"missing agent", ScreenRequest{TenantID: "t", TemplateName: "t", Text: "x"}},
		{"missing template", ScreenRequest{TenantID: "t", AgentID: "a", Text: "x"}},
		{"missing text", ScreenRequest{TenantID: "t", AgentID: "a", TemplateName: "t"}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validateRequest(tc.req)
			if err == nil {
				t.Fatal("expected error")
			}
			if !errors.Is(err, ErrInvalidScreenRequest) {
				t.Errorf("expected ErrInvalidScreenRequest, got %v", err)
			}
		})
	}
}

func TestValidateRequest_AcceptsValidRequest(t *testing.T) {
	req := validReq("hello world")
	if err := validateRequest(req); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateRequest_AcceptsEmptyGCID(t *testing.T) {
	// GCID is the only optional field — system-initiated calls may omit it.
	req := validReq("hello")
	req.GCID = ""
	if err := validateRequest(req); err != nil {
		t.Errorf("expected GCID empty to be valid, got %v", err)
	}
}

// ---------------------------------------------------------------------
// toScreenResult — verdict mapping
// ---------------------------------------------------------------------

func TestToScreenResult_NilSanitizationResult_AllowFallback(t *testing.T) {
	c := newTestClient(t)
	r := c.toScreenResult(nil, validTemplate, 7)
	if r.Verdict != VerdictAllow {
		t.Errorf("nil sr → Verdict=%s, want Allow", r.Verdict)
	}
	if r.LatencyMs != 7 {
		t.Errorf("latency not propagated: got %d", r.LatencyMs)
	}
}

func TestToScreenResult_NoMatchFound_Allow(t *testing.T) {
	c := newTestClient(t)
	sr := stubResponse(modelarmorpb.FilterMatchState_NO_MATCH_FOUND, nil)
	r := c.toScreenResult(sr, validTemplate, 5)
	if r.Verdict != VerdictAllow {
		t.Errorf("got Verdict=%s, want Allow", r.Verdict)
	}
	if r.Reason != "" {
		t.Errorf("Allow result should have empty Reason, got %q", r.Reason)
	}
}

func TestToScreenResult_MatchFound_DefaultBlock(t *testing.T) {
	c := newTestClient(t)
	sr := stubResponse(modelarmorpb.FilterMatchState_MATCH_FOUND, map[string]*modelarmorpb.FilterResult{
		FilterNamePIAndJailbreak: {
			FilterResult: &modelarmorpb.FilterResult_PiAndJailbreakFilterResult{
				PiAndJailbreakFilterResult: &modelarmorpb.PiAndJailbreakFilterResult{
					MatchState:      modelarmorpb.FilterMatchState_MATCH_FOUND,
					ConfidenceLevel: modelarmorpb.DetectionConfidenceLevel_HIGH,
				},
			},
		},
	})
	r := c.toScreenResult(sr, validTemplate, 12)
	if r.Verdict != VerdictBlock {
		t.Errorf("match without registered mode → Verdict=%s, want Block (safe default)", r.Verdict)
	}
	if !strings.Contains(r.Reason, FilterNamePIAndJailbreak) {
		t.Errorf("Reason should mention triggered filter; got %q", r.Reason)
	}
	if len(r.Filters) != 1 {
		t.Fatalf("expected 1 filter hit, got %d", len(r.Filters))
	}
	if r.Filters[0].Severity != SeverityHigh {
		t.Errorf("severity mapping wrong: got %q", r.Filters[0].Severity)
	}
}

func TestToScreenResult_MatchFound_InspectOnlyTemplate(t *testing.T) {
	c := newTestClient(t)
	c.SetTemplateMode(validTemplate, TemplateModeInspectOnly)
	sr := stubResponse(modelarmorpb.FilterMatchState_MATCH_FOUND, map[string]*modelarmorpb.FilterResult{
		FilterNamePIAndJailbreak: {
			FilterResult: &modelarmorpb.FilterResult_PiAndJailbreakFilterResult{
				PiAndJailbreakFilterResult: &modelarmorpb.PiAndJailbreakFilterResult{
					MatchState:      modelarmorpb.FilterMatchState_MATCH_FOUND,
					ConfidenceLevel: modelarmorpb.DetectionConfidenceLevel_MEDIUM_AND_ABOVE,
				},
			},
		},
	})
	r := c.toScreenResult(sr, validTemplate, 9)
	if r.Verdict != VerdictInspectOnly {
		t.Errorf("InspectOnly template + match → Verdict=%s, want InspectOnly", r.Verdict)
	}
}

func TestToScreenResult_UnspecifiedMatchState_DefensiveBlock(t *testing.T) {
	c := newTestClient(t)
	sr := stubResponse(modelarmorpb.FilterMatchState_FILTER_MATCH_STATE_UNSPECIFIED, nil)
	r := c.toScreenResult(sr, validTemplate, 1)
	if r.Verdict != VerdictBlock {
		t.Errorf("UNSPECIFIED → Verdict=%s, want Block (defensive fail-loud)", r.Verdict)
	}
	if r.Reason == "" {
		t.Error("unspecified-state Block should carry a Reason")
	}
}

// ---------------------------------------------------------------------
// flattenFilterResults — per-filter shape projection
// ---------------------------------------------------------------------

func TestFlattenFilterResults_RaiSubcategoryBreakdown(t *testing.T) {
	in := map[string]*modelarmorpb.FilterResult{
		FilterNameRAI: {
			FilterResult: &modelarmorpb.FilterResult_RaiFilterResult{
				RaiFilterResult: &modelarmorpb.RaiFilterResult{
					MatchState: modelarmorpb.FilterMatchState_MATCH_FOUND,
					RaiFilterTypeResults: map[string]*modelarmorpb.RaiFilterResult_RaiFilterTypeResult{
						"hate_speech": {
							MatchState:      modelarmorpb.FilterMatchState_MATCH_FOUND,
							ConfidenceLevel: modelarmorpb.DetectionConfidenceLevel_HIGH,
						},
						"harassment": {
							MatchState:      modelarmorpb.FilterMatchState_NO_MATCH_FOUND,
							ConfidenceLevel: modelarmorpb.DetectionConfidenceLevel_LOW_AND_ABOVE,
						},
					},
				},
			},
		},
	}
	hits, names, raw := flattenFilterResults(in)
	// 2 RAI sub-filter hits, each as its own FilterHit row.
	if len(hits) != 2 {
		t.Fatalf("expected 2 RAI sub-hits, got %d (%+v)", len(hits), hits)
	}
	// hitNames captures only MATCH_FOUND sub-types.
	if len(names) != 1 || !strings.Contains(names[0], "HATE_SPEECH") {
		t.Errorf("expected HATE_SPEECH in hitNames, got %v", names)
	}
	// raw should have severity + match_state per sub-type.
	if raw["rai.hate_speech.severity"] != SeverityHigh {
		t.Errorf("hate_speech severity not High: %v", raw["rai.hate_speech.severity"])
	}
	if raw["rai.harassment.match_state"] != MatchStateNoMatchFound {
		t.Errorf("harassment match_state not propagated: %v", raw["rai.harassment.match_state"])
	}
}

func TestFlattenFilterResults_SDPInspectAndDeidentify(t *testing.T) {
	// SDP — inspect path
	inInspect := map[string]*modelarmorpb.FilterResult{
		FilterNameSDP: {
			FilterResult: &modelarmorpb.FilterResult_SdpFilterResult{
				SdpFilterResult: &modelarmorpb.SdpFilterResult{
					Result: &modelarmorpb.SdpFilterResult_InspectResult{
						InspectResult: &modelarmorpb.SdpInspectResult{
							MatchState: modelarmorpb.FilterMatchState_MATCH_FOUND,
							Findings:   []*modelarmorpb.SdpFinding{{InfoType: "EMAIL_ADDRESS"}},
						},
					},
				},
			},
		},
	}
	_, _, raw := flattenFilterResults(inInspect)
	if raw["sdp.findings_count"] != int64(1) {
		t.Errorf("sdp findings count wrong: %v", raw["sdp.findings_count"])
	}

	// SDP — deidentify path
	inDeid := map[string]*modelarmorpb.FilterResult{
		FilterNameSDP: {
			FilterResult: &modelarmorpb.FilterResult_SdpFilterResult{
				SdpFilterResult: &modelarmorpb.SdpFilterResult{
					Result: &modelarmorpb.SdpFilterResult_DeidentifyResult{
						DeidentifyResult: &modelarmorpb.SdpDeidentifyResult{
							MatchState:       modelarmorpb.FilterMatchState_MATCH_FOUND,
							TransformedBytes: 42,
						},
					},
				},
			},
		},
	}
	_, _, raw2 := flattenFilterResults(inDeid)
	if raw2["sdp.deidentified_bytes"] != int64(42) {
		t.Errorf("sdp deidentified bytes wrong: %v", raw2["sdp.deidentified_bytes"])
	}
}

func TestFlattenFilterResults_MaliciousURIAndVirusScan(t *testing.T) {
	in := map[string]*modelarmorpb.FilterResult{
		FilterNameMaliciousURI: {
			FilterResult: &modelarmorpb.FilterResult_MaliciousUriFilterResult{
				MaliciousUriFilterResult: &modelarmorpb.MaliciousUriFilterResult{
					MatchState: modelarmorpb.FilterMatchState_MATCH_FOUND,
					MaliciousUriMatchedItems: []*modelarmorpb.MaliciousUriFilterResult_MaliciousUriMatchedItem{
						{}, {},
					},
				},
			},
		},
		FilterNameVirusScan: {
			FilterResult: &modelarmorpb.FilterResult_VirusScanFilterResult{
				VirusScanFilterResult: &modelarmorpb.VirusScanFilterResult{
					MatchState:    modelarmorpb.FilterMatchState_NO_MATCH_FOUND,
					VirusDetails:  nil,
				},
			},
		},
	}
	hits, names, raw := flattenFilterResults(in)
	if len(hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(hits))
	}
	// MATCH_FOUND only for malicious_uri.
	if len(names) != 1 || names[0] != FilterNameMaliciousURI {
		t.Errorf("hit names wrong: %v", names)
	}
	if raw["malicious_uri.matched_count"] != int64(2) {
		t.Errorf("matched_count wrong: %v", raw["malicious_uri.matched_count"])
	}
}

func TestFlattenFilterResults_CSAM(t *testing.T) {
	in := map[string]*modelarmorpb.FilterResult{
		FilterNameCSAM: {
			FilterResult: &modelarmorpb.FilterResult_CsamFilterFilterResult{
				CsamFilterFilterResult: &modelarmorpb.CsamFilterResult{
					MatchState: modelarmorpb.FilterMatchState_NO_MATCH_FOUND,
				},
			},
		},
	}
	hits, names, raw := flattenFilterResults(in)
	if len(hits) != 1 {
		t.Fatalf("expected 1 csam hit (NO_MATCH still counts as a row), got %d", len(hits))
	}
	if len(names) != 0 {
		t.Errorf("no-match should produce empty hit names: %v", names)
	}
	if raw["csam.match_state"] != MatchStateNoMatchFound {
		t.Errorf("csam match_state wrong: %v", raw["csam.match_state"])
	}
}

func TestFlattenFilterResults_StableAlphabeticalOrder(t *testing.T) {
	in := map[string]*modelarmorpb.FilterResult{
		FilterNameVirusScan: {FilterResult: &modelarmorpb.FilterResult_VirusScanFilterResult{
			VirusScanFilterResult: &modelarmorpb.VirusScanFilterResult{MatchState: modelarmorpb.FilterMatchState_NO_MATCH_FOUND},
		}},
		FilterNameCSAM: {FilterResult: &modelarmorpb.FilterResult_CsamFilterFilterResult{
			CsamFilterFilterResult: &modelarmorpb.CsamFilterResult{MatchState: modelarmorpb.FilterMatchState_NO_MATCH_FOUND},
		}},
		FilterNameMaliciousURI: {FilterResult: &modelarmorpb.FilterResult_MaliciousUriFilterResult{
			MaliciousUriFilterResult: &modelarmorpb.MaliciousUriFilterResult{MatchState: modelarmorpb.FilterMatchState_NO_MATCH_FOUND},
		}},
	}
	hits, _, _ := flattenFilterResults(in)
	got := make([]string, len(hits))
	for i, h := range hits {
		got[i] = h.FilterName
	}
	want := []string{FilterNameCSAM, FilterNameMaliciousURI, FilterNameVirusScan}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("order[%d]: got %q want %q (full=%v)", i, got[i], want[i], got)
		}
	}
}

// ---------------------------------------------------------------------
// Client — end-to-end via sdkInvoker hook
// ---------------------------------------------------------------------

func TestClient_SanitizeUserPrompt_HappyPath(t *testing.T) {
	c := newTestClient(t)
	c.sdk.userPromptFn = func(_ context.Context, _ *modelarmorpb.SanitizeUserPromptRequest) (*modelarmorpb.SanitizeUserPromptResponse, error) {
		return &modelarmorpb.SanitizeUserPromptResponse{
			SanitizationResult: stubResponse(modelarmorpb.FilterMatchState_NO_MATCH_FOUND, nil),
		}, nil
	}

	r, err := c.SanitizeUserPrompt(context.Background(), validReq("hello"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Verdict != VerdictAllow {
		t.Errorf("got Verdict=%s, want Allow", r.Verdict)
	}
}

func TestClient_SanitizeUserPrompt_BlockOnMatch(t *testing.T) {
	c := newTestClient(t)
	c.sdk.userPromptFn = func(_ context.Context, req *modelarmorpb.SanitizeUserPromptRequest) (*modelarmorpb.SanitizeUserPromptResponse, error) {
		if req.GetName() != validTemplate {
			t.Errorf("template name not propagated: %s", req.GetName())
		}
		if req.GetUserPromptData().GetText() != "leak SSN 123-45-6789" {
			t.Errorf("prompt text not propagated: %s", req.GetUserPromptData().GetText())
		}
		return &modelarmorpb.SanitizeUserPromptResponse{
			SanitizationResult: stubResponse(modelarmorpb.FilterMatchState_MATCH_FOUND, map[string]*modelarmorpb.FilterResult{
				FilterNameSDP: {
					FilterResult: &modelarmorpb.FilterResult_SdpFilterResult{
						SdpFilterResult: &modelarmorpb.SdpFilterResult{
							Result: &modelarmorpb.SdpFilterResult_InspectResult{
								InspectResult: &modelarmorpb.SdpInspectResult{
									MatchState: modelarmorpb.FilterMatchState_MATCH_FOUND,
									Findings:   []*modelarmorpb.SdpFinding{{InfoType: "US_SOCIAL_SECURITY_NUMBER"}},
								},
							},
						},
					},
				},
			}),
		}, nil
	}

	r, err := c.SanitizeUserPrompt(context.Background(), validReq("leak SSN 123-45-6789"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Verdict != VerdictBlock {
		t.Errorf("got Verdict=%s, want Block", r.Verdict)
	}
	if !strings.Contains(r.Reason, FilterNameSDP) {
		t.Errorf("Reason should mention sdp; got %q", r.Reason)
	}
}

func TestClient_SanitizeUserPrompt_ReturnsValidationError(t *testing.T) {
	c := newTestClient(t)
	_, err := c.SanitizeUserPrompt(context.Background(), ScreenRequest{})
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !errors.Is(err, ErrInvalidScreenRequest) {
		t.Errorf("expected ErrInvalidScreenRequest, got %v", err)
	}
}

func TestClient_SanitizeUserPrompt_PropagatesSDKError(t *testing.T) {
	c := newTestClient(t)
	sdkErr := errors.New("rpc deadline exceeded")
	c.sdk.userPromptFn = func(_ context.Context, _ *modelarmorpb.SanitizeUserPromptRequest) (*modelarmorpb.SanitizeUserPromptResponse, error) {
		return nil, sdkErr
	}
	_, err := c.SanitizeUserPrompt(context.Background(), validReq("anything"))
	if err == nil {
		t.Fatal("expected SDK error")
	}
	if !errors.Is(err, sdkErr) {
		t.Errorf("error chain dropped SDK error: %v", err)
	}
}

func TestClient_SanitizeModelResponse_HappyPath(t *testing.T) {
	c := newTestClient(t)
	c.sdk.modelRespFn = func(_ context.Context, _ *modelarmorpb.SanitizeModelResponseRequest) (*modelarmorpb.SanitizeModelResponseResponse, error) {
		return &modelarmorpb.SanitizeModelResponseResponse{
			SanitizationResult: stubResponse(modelarmorpb.FilterMatchState_NO_MATCH_FOUND, nil),
		}, nil
	}
	r, err := c.SanitizeModelResponse(context.Background(), validReq("llm output"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Verdict != VerdictAllow {
		t.Errorf("got Verdict=%s, want Allow", r.Verdict)
	}
}

func TestClient_SanitizeModelResponse_PropagatesSDKError(t *testing.T) {
	c := newTestClient(t)
	sdkErr := errors.New("rpc unavailable")
	c.sdk.modelRespFn = func(_ context.Context, _ *modelarmorpb.SanitizeModelResponseRequest) (*modelarmorpb.SanitizeModelResponseResponse, error) {
		return nil, sdkErr
	}
	_, err := c.SanitizeModelResponse(context.Background(), validReq("anything"))
	if err == nil {
		t.Fatal("expected SDK error")
	}
	if !errors.Is(err, sdkErr) {
		t.Errorf("error chain dropped SDK error: %v", err)
	}
}

func TestClient_LatencyMs_RecordedNonNegative(t *testing.T) {
	c := newTestClient(t)
	c.sdk.userPromptFn = func(ctx context.Context, _ *modelarmorpb.SanitizeUserPromptRequest) (*modelarmorpb.SanitizeUserPromptResponse, error) {
		// Burn a small fraction so latencyMs > 0 even on fast CI.
		time.Sleep(2 * time.Millisecond)
		return &modelarmorpb.SanitizeUserPromptResponse{
			SanitizationResult: stubResponse(modelarmorpb.FilterMatchState_NO_MATCH_FOUND, nil),
		}, nil
	}
	r, err := c.SanitizeUserPrompt(context.Background(), validReq("hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.LatencyMs < 0 {
		t.Errorf("latency negative: %d", r.LatencyMs)
	}
}

// ---------------------------------------------------------------------
// SetTemplateMode
// ---------------------------------------------------------------------

func TestSetTemplateMode_DefaultsToBlock(t *testing.T) {
	c := newTestClient(t)
	if got := c.templateMode("unknown-template"); got != TemplateModeBlock {
		t.Errorf("default mode = %s, want Block", got)
	}
}

func TestSetTemplateMode_RegisterAndLookup(t *testing.T) {
	c := newTestClient(t)
	c.SetTemplateMode("audit-only-template", TemplateModeInspectOnly)
	if got := c.templateMode("audit-only-template"); got != TemplateModeInspectOnly {
		t.Errorf("registered mode = %s, want InspectOnly", got)
	}
}

// ---------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------

func TestClient_Close_DelegatesToCloser(t *testing.T) {
	closed := false
	c := newTestClient(t)
	c.sdk.closer = func() error {
		closed = true
		return nil
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}
	if !closed {
		t.Error("closer was not invoked")
	}
}

func TestClient_Close_NilSafe(t *testing.T) {
	var c *Client
	if err := c.Close(); err != nil {
		t.Errorf("nil receiver Close should be no-op, got %v", err)
	}
}

// ---------------------------------------------------------------------
// NewScreener
// ---------------------------------------------------------------------

func TestNewScreener_RejectsEmptyProject(t *testing.T) {
	_, err := NewScreener(context.Background(), "", "us-central1")
	if err == nil {
		t.Fatal("expected error for empty project")
	}
	if !strings.Contains(err.Error(), "project required") {
		t.Errorf("expected project-required error, got %v", err)
	}
}

func TestNewScreener_RejectsEmptyLocation(t *testing.T) {
	_, err := NewScreener(context.Background(), "chora-test", "")
	if err == nil {
		t.Fatal("expected error for empty location")
	}
	if !strings.Contains(err.Error(), "location required") {
		t.Errorf("expected location-required error, got %v", err)
	}
}

// ---------------------------------------------------------------------
// StubScreener — surface coverage
// ---------------------------------------------------------------------

func TestStubScreener_DefaultsToAllow(t *testing.T) {
	s := NewStubScreener()
	r, err := s.SanitizeUserPrompt(context.Background(), validReq("hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Verdict != VerdictAllow {
		t.Errorf("default Verdict=%s, want Allow", r.Verdict)
	}
	r2, err := s.SanitizeModelResponse(context.Background(), validReq("hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r2.Verdict != VerdictAllow {
		t.Errorf("default model-resp Verdict=%s, want Allow", r2.Verdict)
	}
}

func TestStubScreener_HookOverride(t *testing.T) {
	s := NewStubScreener()
	s.SetUserPromptResult(func(req ScreenRequest) (ScreenResult, error) {
		if strings.Contains(req.Text, "block-me") {
			return ScreenResult{Verdict: VerdictBlock, Reason: "stub: matched"}, nil
		}
		return ScreenResult{Verdict: VerdictAllow}, nil
	})
	r, err := s.SanitizeUserPrompt(context.Background(), validReq("please block-me"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Verdict != VerdictBlock {
		t.Errorf("expected Block via hook, got %s", r.Verdict)
	}
}

func TestStubScreener_ModelResponseHookOverride(t *testing.T) {
	s := NewStubScreener()
	s.SetModelResponseResult(func(req ScreenRequest) (ScreenResult, error) {
		return ScreenResult{
			Verdict: VerdictInspectOnly,
			Reason:  "stub: audit-only",
		}, nil
	})
	r, err := s.SanitizeModelResponse(context.Background(), validReq("llm output"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Verdict != VerdictInspectOnly {
		t.Errorf("expected InspectOnly via hook, got %s", r.Verdict)
	}
}

func TestStubScreener_ErrStubExplicit(t *testing.T) {
	s := NewStubScreener()
	s.SetUserPromptResult(func(_ ScreenRequest) (ScreenResult, error) {
		return ScreenResult{}, ErrStubExplicit
	})
	_, err := s.SanitizeUserPrompt(context.Background(), validReq("anything"))
	if !errors.Is(err, ErrStubExplicit) {
		t.Errorf("expected ErrStubExplicit, got %v", err)
	}
}

func TestStubScreener_CaptureCalls(t *testing.T) {
	s := NewStubScreener()
	s.EnableCallCapture()
	_, _ = s.SanitizeUserPrompt(context.Background(), validReq("one"))
	_, _ = s.SanitizeModelResponse(context.Background(), validReq("two"))
	calls := s.Calls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 captured calls, got %d", len(calls))
	}
	if calls[0].Method != "SanitizeUserPrompt" || calls[0].Request.Text != "one" {
		t.Errorf("call[0] wrong: %+v", calls[0])
	}
	if calls[1].Method != "SanitizeModelResponse" || calls[1].Request.Text != "two" {
		t.Errorf("call[1] wrong: %+v", calls[1])
	}
}

func TestStubScreener_RejectsInvalidRequest(t *testing.T) {
	s := NewStubScreener()
	_, err := s.SanitizeUserPrompt(context.Background(), ScreenRequest{})
	if err == nil || !errors.Is(err, ErrInvalidScreenRequest) {
		t.Errorf("expected ErrInvalidScreenRequest, got %v", err)
	}
}

func TestStubScreener_CloseHook(t *testing.T) {
	s := NewStubScreener()
	called := false
	s.SetCloseHook(func() error { called = true; return nil })
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("Close hook not invoked")
	}
}

// ---------------------------------------------------------------------
// severityFromConfidence
// ---------------------------------------------------------------------

func TestSeverityFromConfidence_AllLevels(t *testing.T) {
	cases := []struct {
		in   modelarmorpb.DetectionConfidenceLevel
		want string
	}{
		{modelarmorpb.DetectionConfidenceLevel_LOW_AND_ABOVE, SeverityLow},
		{modelarmorpb.DetectionConfidenceLevel_MEDIUM_AND_ABOVE, SeverityMedium},
		{modelarmorpb.DetectionConfidenceLevel_HIGH, SeverityHigh},
		{modelarmorpb.DetectionConfidenceLevel_DETECTION_CONFIDENCE_LEVEL_UNSPECIFIED, ""},
	}
	for _, tc := range cases {
		if got := severityFromConfidence(tc.in); got != tc.want {
			t.Errorf("severity(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------
// summariseHits + sortStrings
// ---------------------------------------------------------------------

func TestSummariseHits_EmptyName(t *testing.T) {
	got := summariseHits(nil)
	if !strings.Contains(got, "no filter detail") {
		t.Errorf("empty summarise wrong: %q", got)
	}
}

func TestSummariseHits_JoinedNames(t *testing.T) {
	got := summariseHits([]string{"rai:HATE_SPEECH", "sdp"})
	if !strings.Contains(got, "rai:HATE_SPEECH") || !strings.Contains(got, "sdp") {
		t.Errorf("joined names lost: %q", got)
	}
}

func TestSortStrings_StableOnSmallSlice(t *testing.T) {
	s := []string{"banana", "apple", "cherry"}
	sortStrings(s)
	want := []string{"apple", "banana", "cherry"}
	for i := range want {
		if s[i] != want[i] {
			t.Errorf("sortStrings[%d]: got %q want %q", i, s[i], want[i])
		}
	}
}
