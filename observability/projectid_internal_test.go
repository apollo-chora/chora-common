// projectid_internal_test.go — unit tests for the GCP project-id resolution
// precedence used by the cloudtrace exporter path in defaultExporterFactory.
//
// Bug context (2026-07-01): defaultExporterFactory resolved the project ONLY
// from GOOGLE_CLOUD_PROJECT. Under Workload Identity every Chora service
// manifest sets GCP_PROJECT instead (chora-a2a-gateway deployment.yaml
// confirmed), so projectID stayed empty and cloudtrace.New() fell through to
// its own ADC lookup (google.FindDefaultCredentials), which returns an empty
// ProjectID in that configuration. Result: cloudtrace.New() failed with
// "stackdriver: no project found with application default credentials" and
// traces were silently dropped platform-wide.
//
// resolveProjectID adds a GCP_PROJECT + GCE/GKE metadata-server fallback so
// an explicit cloudtrace.WithProjectID(...) is set BEFORE the ADC path is
// ever reached, while still returning "" (never a fabricated value) when
// every source is genuinely empty — preserving the existing loud-failure
// signal from cloudtrace.New()'s own ADC fallback.
package observability

import (
	"context"
	"testing"
)

func fakeGetenvObs(env map[string]string) func(string) string {
	return func(k string) string { return env[k] }
}

func fakeMetadataObs(v string, ok bool) func() (string, bool) {
	return func() (string, bool) { return v, ok }
}

func TestResolveProjectID_PrefersGoogleCloudProject(t *testing.T) {
	getenv := fakeGetenvObs(map[string]string{
		"GOOGLE_CLOUD_PROJECT": "from-google-cloud-project",
		"GCP_PROJECT":          "from-gcp-project",
	})
	got := resolveProjectID(getenv, fakeMetadataObs("from-metadata", true))
	if got != "from-google-cloud-project" {
		t.Errorf("got %q, want GOOGLE_CLOUD_PROJECT to win precedence", got)
	}
}

func TestResolveProjectID_FallsBackToGCPProject(t *testing.T) {
	getenv := fakeGetenvObs(map[string]string{
		"GCP_PROJECT": "from-gcp-project",
	})
	got := resolveProjectID(getenv, fakeMetadataObs("from-metadata", true))
	if got != "from-gcp-project" {
		t.Errorf("got %q, want GCP_PROJECT fallback (GOOGLE_CLOUD_PROJECT unset)", got)
	}
}

func TestResolveProjectID_FallsBackToMetadataServer(t *testing.T) {
	got := resolveProjectID(fakeGetenvObs(nil), fakeMetadataObs("from-metadata", true))
	if got != "from-metadata" {
		t.Errorf("got %q, want metadata-server fallback (both env vars unset)", got)
	}
}

func TestResolveProjectID_AllSourcesEmpty_ReturnsEmpty(t *testing.T) {
	got := resolveProjectID(fakeGetenvObs(nil), fakeMetadataObs("", false))
	if got != "" {
		t.Errorf("got %q, want empty so cloudtrace.New()'s own ADC loud-failure path still fires", got)
	}
}

func TestResolveProjectID_NilMetadataFunc_DoesNotPanic(t *testing.T) {
	got := resolveProjectID(fakeGetenvObs(nil), nil)
	if got != "" {
		t.Errorf("got %q, want empty (no metadata source available)", got)
	}
}

func TestResolveProjectID_WhitespaceEnvTreatedAsUnset(t *testing.T) {
	getenv := fakeGetenvObs(map[string]string{
		"GOOGLE_CLOUD_PROJECT": "   ",
		"GCP_PROJECT":          "  from-gcp-project  ",
	})
	got := resolveProjectID(getenv, fakeMetadataObs("", false))
	if got != "from-gcp-project" {
		t.Errorf("got %q, want trimmed GCP_PROJECT fallback", got)
	}
}

func TestResolveProjectID_MetadataOKButBlank_FallsThroughToEmpty(t *testing.T) {
	got := resolveProjectID(fakeGetenvObs(nil), fakeMetadataObs("   ", true))
	if got != "" {
		t.Errorf("got %q, want empty (metadata reported ok but blank value)", got)
	}
}

// TestGCEMetadataProjectID_OffGCE_ReturnsNotOK exercises the real (non-faked)
// wiring function's guard deterministically: the test sandbox / CI runner is
// never a GCE/GKE instance, so metadata.OnGCEWithContext must report false
// and gceMetadataProjectID must return ("", false) without attempting a
// metadata-server round-trip.
func TestGCEMetadataProjectID_OffGCE_ReturnsNotOK(t *testing.T) {
	id, ok := gceMetadataProjectID(context.Background())
	if ok {
		t.Fatalf("expected ok=false off-GCE, got id=%q ok=%v", id, ok)
	}
	if id != "" {
		t.Errorf("expected empty id off-GCE, got %q", id)
	}
}
