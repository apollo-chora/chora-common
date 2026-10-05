// devexport_internal_test.go — unit tests for the stdout-dev-mode
// fallback heuristic used by defaultExporterFactory.
//
// 2026-10-05: the heuristic lost its GCP project/metadata legs with the
// Google Cloud exit. It is now purely env-driven: OTEL_EXPORTER=stdout
// opts in explicitly, and an unset OTEL_EXPORTER_OTLP_ENDPOINT means
// local dev. Sequential (not parallel): t.Setenv mutates process env.
package observability

import "testing"

func TestIsDevExport_EnvMatrix(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		wantDev bool
	}{
		{
			name:    "explicit stdout override wins",
			env:     map[string]string{"OTEL_EXPORTER": "stdout"},
			wantDev: true,
		},
		{
			name:    "OTLP endpoint set is never dev mode",
			env:     map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "otel-collector:4317"},
			wantDev: false,
		},
		{
			name:    "endpoint with surrounding whitespace is still set",
			env:     map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "  otel-collector:4317  "},
			wantDev: false,
		},
		{
			name:    "no endpoint and no override - dev mode",
			env:     map[string]string{},
			wantDev: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if got := isDevExport(); got != tc.wantDev {
				t.Errorf("isDevExport() = %v, want %v", got, tc.wantDev)
			}
		})
	}
}
