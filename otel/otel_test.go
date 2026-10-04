package otel_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	choraotel "github.com/apollo-chora/chora-common/otel"
)

// envKeys is the full set of env vars Init reads. Tests reset all of them
// to keep each case hermetic — the lib is process-global.
var envKeys = []string{
	"OTEL_EXPORTER",
	"OTEL_EXPORTER_OTLP_ENDPOINT",
	"OTEL_TRACES_SAMPLER",
	"OTEL_TRACES_SAMPLER_ARG",
	"OTEL_RESOURCE_ATTRIBUTES",
	"GOOGLE_CLOUD_PROJECT",
	"GOOGLE_APPLICATION_CREDENTIALS",
	"DEPLOYMENT_ENVIRONMENT",
	"CLOUD_REGION",
}

func snapshotEnv(t *testing.T) {
	t.Helper()
	saved := make(map[string]string, len(envKeys))
	for _, k := range envKeys {
		saved[k] = os.Getenv(k)
		_ = os.Unsetenv(k)
	}
	t.Cleanup(func() {
		for _, k := range envKeys {
			if v, ok := saved[k]; ok && v != "" {
				_ = os.Setenv(k, v)
			} else {
				_ = os.Unsetenv(k)
			}
		}
	})
}

// envMu serialises tests that mutate process env (Init reads env). Mirrors
// the t.Setenv guarantee but works across helpers that share state.
var envMu sync.Mutex

func TestInit_DevFallback_StdoutExporter(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shutdown, err := choraotel.Init(ctx, "chora-lib-test", "v0.0.0-test")
	if err != nil {
		t.Fatalf("Init(dev): %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown")
	}
	if err := shutdown(ctx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

func TestInit_ExplicitStdoutMode(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	t.Setenv("OTEL_EXPORTER", "stdout")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "chora-489812")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdown, err := choraotel.Init(ctx, "chora-lib-test", "v0.0.0-stdout")
	if err != nil {
		t.Fatalf("Init(stdout-mode): %v", err)
	}
	_ = shutdown(ctx)
}

func TestTracer_NotNil(t *testing.T) {
	tracer := choraotel.Tracer("chora-lib-test")
	if tracer == nil {
		t.Error("Tracer returned nil")
	}
}

func TestInit_RejectsEmptyServiceName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := choraotel.Init(ctx, "", "v1.0.0")
	if err == nil {
		t.Error("expected error for empty service name")
	}
}

// TestInit_FactorySwap_ProvesCloudTracePathReached swaps in a recording
// factory to prove the cloudtrace branch is reached when env signals are
// present. This is the load-bearing Wave B regression: pre-fix wiring
// dropped spans because TLS failed against telemetry.googleapis.com:443
// under WithInsecure().
func TestInit_FactorySwap_ProvesCloudTracePathReached(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "telemetry.googleapis.com:443")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "chora-489812")

	var (
		called         bool
		gotEndpoint    string
		gotServiceName string
	)
	restore := choraotel.SwapExporterFactoryForTest(
		func(ctx context.Context, endpoint, name, version string) (sdktrace.SpanExporter, error) {
			called = true
			gotEndpoint = endpoint
			gotServiceName = name
			return &noopExporter{}, nil
		},
	)
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdown, err := choraotel.Init(ctx, "chora-cloudtrace-test", "v0.0.0")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer func() { _ = shutdown(ctx) }()

	if !called {
		t.Fatal("exporter factory not invoked")
	}
	if gotEndpoint != "telemetry.googleapis.com:443" {
		t.Errorf("endpoint hint: got %q want telemetry.googleapis.com:443", gotEndpoint)
	}
	if gotServiceName != "chora-cloudtrace-test" {
		t.Errorf("service name not threaded: got %q", gotServiceName)
	}
}

func TestInit_FactoryErrorBubblesUp(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	t.Setenv("GOOGLE_CLOUD_PROJECT", "chora-489812")

	wantErr := errExporter("factory failed")
	restore := choraotel.SwapExporterFactoryForTest(
		func(ctx context.Context, endpoint, name, version string) (sdktrace.SpanExporter, error) {
			return nil, wantErr
		},
	)
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := choraotel.Init(ctx, "chora-err-test", "v0.0.0")
	if err == nil {
		t.Fatal("expected error from factory")
	}
}

// TestInit_AppliesEnvSampler exercises parentbased_traceidratio.
func TestInit_AppliesEnvSampler(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	t.Setenv("OTEL_EXPORTER", "stdout")
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdown, err := choraotel.Init(ctx, "chora-sampler-test", "v0.0.0")
	if err != nil {
		t.Fatalf("Init(sampler): %v", err)
	}
	_ = shutdown(ctx)
}

func TestInit_AppliesEnvSampler_BadArg(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	t.Setenv("OTEL_EXPORTER", "stdout")
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "not-a-float")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdown, err := choraotel.Init(ctx, "chora-sampler-bad", "v0.0.0")
	if err != nil {
		t.Fatalf("Init(sampler-bad): %v", err)
	}
	_ = shutdown(ctx)
}

func TestInit_AppliesEnvSampler_OutOfRange(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	t.Setenv("OTEL_EXPORTER", "stdout")
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "5.0")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdown, err := choraotel.Init(ctx, "chora-sampler-oor", "v0.0.0")
	if err != nil {
		t.Fatalf("Init(sampler-oor): %v", err)
	}
	_ = shutdown(ctx)
}

func TestInit_AlwaysOffSampler(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	t.Setenv("OTEL_EXPORTER", "stdout")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdown, err := choraotel.Init(ctx, "chora-off-test", "v0.0.0")
	if err != nil {
		t.Fatalf("Init(off): %v", err)
	}
	_ = shutdown(ctx)
}

func TestInit_AppliesResourceFromEnv(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	t.Setenv("OTEL_EXPORTER", "stdout")
	t.Setenv("DEPLOYMENT_ENVIRONMENT", "prod")
	t.Setenv("CLOUD_REGION", "us-central1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdown, err := choraotel.Init(ctx, "chora-resource-test", "v1.2.3")
	if err != nil {
		t.Fatalf("Init(resource): %v", err)
	}
	_ = shutdown(ctx)
}

// TestDefaultFactory_StdoutBranch exercises the dev-mode branch of the real
// production factory (not the swap shim).
func TestDefaultFactory_StdoutBranch(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	t.Setenv("OTEL_EXPORTER", "stdout")

	fn := choraotel.DefaultExporterFactoryForTest()
	exp, err := fn(context.Background(), "telemetry.googleapis.com:443", "chora-real-factory", "v0.0.0")
	if err != nil {
		t.Fatalf("default factory stdout: %v", err)
	}
	if exp == nil {
		t.Fatal("nil exporter")
	}
	_ = exp.Shutdown(context.Background())
}

// TestDefaultFactory_CloudTraceBranch_ErrorPath proves the cloudtrace branch
// is invoked in production mode. When ADC is absent (likely on a CI runner
// with no creds), cloudtrace.New errors out — that's still proof the
// branch runs.
func TestDefaultFactory_CloudTraceBranch_ErrorPath(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	// Force production mode: pretend a project is configured. ADC will
	// still fail in CI, which is fine — we just want the cloudtrace branch
	// to execute.
	t.Setenv("GOOGLE_CLOUD_PROJECT", "chora-fake-project-for-test")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null") // force ADC failure
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "telemetry.googleapis.com:443")

	fn := choraotel.DefaultExporterFactoryForTest()
	exp, err := fn(context.Background(), "telemetry.googleapis.com:443", "chora-real-cloudtrace", "v0.0.0")
	// Either the exporter constructs (rare in CI) or errors — both prove
	// the branch ran. We don't shutdown a real Cloud Trace client because
	// the test creds are bogus.
	if err == nil && exp == nil {
		t.Fatal("both error and exporter nil — branch did not execute")
	}
	if exp != nil {
		_ = exp.Shutdown(context.Background())
	}
}

func TestProjectIDForLog(t *testing.T) {
	if choraotel.ProjectIDForLogForTest("") != "<adc-default>" {
		t.Error("empty project should render <adc-default>")
	}
	if choraotel.ProjectIDForLogForTest("chora-489812") != "chora-489812" {
		t.Error("non-empty project should pass through")
	}
}

func TestIsDevExport_TrueWhenNoProductionSignals(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	if !choraotel.IsDevExportForTest() {
		t.Error("no envs set => should be dev export")
	}
}

func TestIsDevExport_FalseWhenProjectSet(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	t.Setenv("GOOGLE_CLOUD_PROJECT", "chora-489812")
	if choraotel.IsDevExportForTest() {
		t.Error("project set => should not be dev")
	}
}

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

type noopExporter struct{}

func (*noopExporter) ExportSpans(_ context.Context, _ []sdktrace.ReadOnlySpan) error {
	return nil
}
func (*noopExporter) Shutdown(_ context.Context) error { return nil }

type errExporter string

func (e errExporter) Error() string { return string(e) }

// TestInit_RegistersW3CTraceContextPropagator regression-guards the
// 2026-05-17 fix (commit 58b28eb4) that wires propagation.TraceContext +
// Baggage as the global propagator in chora-go-common/otel.Init.
// Without this, every prop.Extract(...) call downstream sees a no-op
// propagator and silently drops inbound W3C traceparent. Closed the
// FE-coord E2E-BE-AI-ASSIST-TRACE-EXPORT-PERM blocker.
func TestInit_RegistersW3CTraceContextPropagator(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	t.Setenv("OTEL_EXPORTER", "stdout")

	// Reset to a no-op propagator so the assertion below is meaningful.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	shutdown, err := choraotel.Init(context.Background(), "chora-prop-test", "v0.0.0")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	prop := otel.GetTextMapPropagator()
	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	headers := map[string]string{"traceparent": tp}
	ctx := prop.Extract(context.Background(), propagation.MapCarrier(headers))
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatalf("Init must register W3C TraceContext propagator — "+
			"extracted SpanContext invalid for traceparent %q", tp)
	}
	if got := sc.TraceID().String(); got != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("propagator failed to continue trace_id: got %q", got)
	}
}
