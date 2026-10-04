package otel

import (
	"context"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// ExporterFactory is the function signature the otel package uses to build a
// SpanExporter. Tests swap in fakes via SwapExporterFactoryForTest to
// exercise the cloudtrace branch without real ADC.
type ExporterFactory func(ctx context.Context, endpoint, serviceName, version string) (sdktrace.SpanExporter, error)

// SwapExporterFactoryForTest replaces the package-level exporter factory and
// returns a restore func. Test-only — never call from production code paths.
// Kept in the production file (not _test.go) so external packages can swap
// during integration tests without copying the lib's internals.
func SwapExporterFactoryForTest(fn ExporterFactory) func() {
	prev := exporterFactory
	exporterFactory = fn
	return func() { exporterFactory = prev }
}

// DefaultExporterFactoryForTest exposes the production exporter factory so
// tests can exercise both the stdout and cloudtrace branches end-to-end.
// Test-only — production callers should go via Init.
func DefaultExporterFactoryForTest() ExporterFactory {
	return defaultExporterFactory
}

// ProjectIDForLogForTest exposes the project-id log formatter for coverage.
func ProjectIDForLogForTest(p string) string {
	return projectIDForLog(p)
}

// IsDevExportForTest exposes the dev-export detector for coverage. Reads
// env at call time.
func IsDevExportForTest() bool {
	return isDevExport()
}
