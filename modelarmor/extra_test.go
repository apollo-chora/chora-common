// Package modelarmor — supplementary edge tests: ModelResponse request
// validation, stub default-close, and flattenFilterResults nil sub-result
// projection. No network — reuses the hand-rolled sdkInvoker harness.
package modelarmor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/modelarmor/apiv1/modelarmorpb"
)

func TestSanitizeModelResponse_RejectsInvalidRequest(t *testing.T) {
	t.Parallel()
	c := newTestClient(t)
	_, err := c.SanitizeModelResponse(context.Background(), ScreenRequest{})
	if err == nil {
		t.Fatal("expected validation error for empty request")
	}
	if !errors.Is(err, ErrInvalidScreenRequest) {
		t.Errorf("err = %v, want ErrInvalidScreenRequest", err)
	}
}

func TestStubScreener_ModelResponseRejectsInvalidRequest(t *testing.T) {
	t.Parallel()
	s := NewStubScreener()
	if _, err := s.SanitizeModelResponse(context.Background(), ScreenRequest{}); err == nil {
		t.Fatal("expected validation error for empty request")
	}
}

func TestStubScreener_CloseNoHookReturnsNil(t *testing.T) {
	t.Parallel()
	s := NewStubScreener()
	if err := s.Close(); err != nil {
		t.Fatalf("Close (no hook) = %v, want nil", err)
	}
}

// TestFlattenFilterResults_NilSubResultsAndUnknownFilter — every filter's
// sub-result can be absent (nil inner oneof); those entries must be
// skipped without panic, and an unknown filter key falls through to the
// defensive UNSPECIFIED default.
func TestFlattenFilterResults_NilSubResultsAndUnknownFilter(t *testing.T) {
	t.Parallel()
	in := map[string]*modelarmorpb.FilterResult{
		FilterNameRAI:            {FilterResult: &modelarmorpb.FilterResult_RaiFilterResult{}},
		FilterNamePIAndJailbreak: {FilterResult: &modelarmorpb.FilterResult_PiAndJailbreakFilterResult{}},
		FilterNameSDP:            {FilterResult: &modelarmorpb.FilterResult_SdpFilterResult{}},
		FilterNameMaliciousURI:   {FilterResult: &modelarmorpb.FilterResult_MaliciousUriFilterResult{}},
		FilterNameCSAM:           {FilterResult: &modelarmorpb.FilterResult_CsamFilterFilterResult{}},
		FilterNameVirusScan:      {FilterResult: &modelarmorpb.FilterResult_VirusScanFilterResult{}},
		"custom_unknown_filter":  {},
	}
	hits, names, raw := flattenFilterResults(in)
	if len(hits) != 1 {
		t.Fatalf("expected exactly 1 hit (unknown filter); got %d: %+v", len(hits), hits)
	}
	if hits[0].FilterName != "custom_unknown_filter" {
		t.Errorf("hit name = %q", hits[0].FilterName)
	}
	if hits[0].MatchState != MatchStateUnspecified {
		t.Errorf("unknown-filter match state = %q, want UNSPECIFIED", hits[0].MatchState)
	}
	if len(names) != 0 {
		t.Errorf("names = %v, want none", names)
	}
	if raw["custom_unknown_filter.match_state"] != MatchStateUnspecified {
		t.Errorf("raw unknown match state = %v", raw["custom_unknown_filter.match_state"])
	}
}

// TestFlattenFilterResults_RaiMatchNoSubtypesAppendsName — a RAI match
// with zero sub-type results still surfaces the parent rai name so the
// audit trail is not empty.
func TestFlattenFilterResults_RaiMatchNoSubtypesAppendsName(t *testing.T) {
	t.Parallel()
	in := map[string]*modelarmorpb.FilterResult{
		FilterNameRAI: {
			FilterResult: &modelarmorpb.FilterResult_RaiFilterResult{
				RaiFilterResult: &modelarmorpb.RaiFilterResult{
					MatchState: modelarmorpb.FilterMatchState_MATCH_FOUND,
				},
			},
		},
	}
	hits, names, _ := flattenFilterResults(in)
	if len(hits) != 0 {
		t.Errorf("hits = %d, want 0 (sub-results only)", len(hits))
	}
	if len(names) != 1 || names[0] != FilterNameRAI {
		t.Errorf("names = %v, want [rai]", names)
	}
}

// TestNewScreener_LiveConstructionDefensive — NewScreener either fails
// fast (no ADC / endpoint creds) or builds a real client whose Close must
// succeed. Both outcomes are acceptable environments; the assertion pins
// the wrapped-error shape when construction fails.
func TestNewScreener_LiveConstructionDefensive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cl, err := NewScreener(ctx, "chora-489812", "us-central1")
	if err != nil {
		if !strings.Contains(err.Error(), "modelarmor: NewScreener: new SDK client") {
			t.Fatalf("unexpected NewScreener error: %v", err)
		}
		return
	}
	if err := cl.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
