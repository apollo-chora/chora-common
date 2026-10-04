// devexport_internal_test.go — unit tests for the stdout-dev-mode fallback
// heuristic used by defaultExporterFactory.
//
// Bug context (2026-07-01, same incident as projectid_internal_test.go):
// isDevExport gated its project-presence leg on GOOGLE_CLOUD_PROJECT alone,
// while resolveProjectID (a few lines above it in observability.go) accepts
// GOOGLE_CLOUD_PROJECT, GCP_PROJECT, and the GCE/GKE metadata server. A
// service that sets only GCP_PROJECT (every Chora manifest does, under
// Workload Identity) and has no OTEL_EXPORTER_OTLP_ENDPOINT configured would
// satisfy isDevExport's old AND-condition and silently degrade to stdout
// export — never reaching the cloudtrace path resolveProjectID was fixed to
// serve.
package observability

import "testing"

func TestIsDevExport_ProjectPresencePrecedence(t *testing.T) {
	cases := []struct {
		name              string
		env               map[string]string
		metadataProjectID string
		metadataOK        bool
		wantDev           bool
	}{
		{
			name:    "explicit stdout override wins regardless of project",
			env:     map[string]string{"OTEL_EXPORTER": "stdout", "GOOGLE_CLOUD_PROJECT": "chora-489812"},
			wantDev: true,
		},
		{
			name:    "OTLP endpoint set is never dev mode",
			env:     map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "telemetry.googleapis.com:443"},
			wantDev: false,
		},
		{
			name:    "GOOGLE_CLOUD_PROJECT alone resolves a project - not dev mode",
			env:     map[string]string{"GOOGLE_CLOUD_PROJECT": "chora-489812"},
			wantDev: false,
		},
		{
			name:    "GCP_PROJECT alone resolves a project - not dev mode",
			env:     map[string]string{"GCP_PROJECT": "chora-489812"},
			wantDev: false,
		},
		{
			name:              "GCE metadata alone resolves a project - not dev mode",
			env:               map[string]string{},
			metadataProjectID: "chora-489812",
			metadataOK:        true,
			wantDev:           false,
		},
		{
			name:    "GOOGLE_APPLICATION_CREDENTIALS set is never dev mode",
			env:     map[string]string{"GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/sa.json"},
			wantDev: false,
		},
		{
			name:    "no project resolvable anywhere and no ADC file - dev mode",
			env:     map[string]string{},
			wantDev: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			getenv := fakeGetenvObs(tc.env)
			metadataProjectID := fakeMetadataObs(tc.metadataProjectID, tc.metadataOK)
			got := isDevExport(getenv, metadataProjectID)
			if got != tc.wantDev {
				t.Errorf("isDevExport() = %v, want %v", got, tc.wantDev)
			}
		})
	}
}
