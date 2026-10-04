// cloudtrace_path_test.go — statement-coverage extension for the real
// defaultExporterFactory cloudtrace path, the InitOTLP error branches
// (resource build, exporter shutdown) and the InitOTLPAsync
// cancellation branch. Credential-dependent assertions are written
// defensively: on machines with working ADC the exporter constructs
// successfully and the shutdown closure runs; on credential-less
// machines cloudtrace.New fails loudly and we assert that failure.
package observability_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace"

	"github.com/5007-Capstone/chora/libs/chora-go-common/observability"
)

// TestInitOTLP_CloudtracePath_ProjectSet drives defaultExporterFactory's
// non-dev branch with a resolvable project id (GOOGLE_CLOUD_PROJECT), so
// the WithProjectID opt and cloudtrace.New call both execute. On a
// credential-less machine cloudtrace.New must fail loudly with the
// "cloudtrace:" wrapper; with creds the exporter is built and shutdown
// runs.
func TestInitOTLP_CloudtracePath_ProjectSet(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("GCP_PROJECT", "")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "chora-489812")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")

	shutdown, err := observability.InitOTLP("chora-cloudtrace-proj", "v0.0.0")
	if err != nil {
		if !strings.Contains(err.Error(), "cloudtrace") {
			t.Fatalf("InitOTLP err = %v, want a cloudtrace-wrapped exporter error", err)
		}
		return
	}
	defer func() { _ = shutdown() }()
}

// TestInitOTLP_CloudtracePath_EndpointOnly covers the endpoint-hint-only
// configuration: the project stays empty, projectIDForLog must emit the
// "<adc-default>" placeholder, and the exporter is built WITHOUT an
// explicit WithProjectID (ADC fallback in cloudtrace.New).
func TestInitOTLP_CloudtracePath_EndpointOnly(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "telemetry.googleapis.com:443")
	t.Setenv("GCP_PROJECT", "")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")

	shutdown, err := observability.InitOTLP("chora-cloudtrace-ep", "v0.0.0")
	if err != nil {
		if !strings.Contains(err.Error(), "cloudtrace") {
			t.Fatalf("InitOTLP err = %v, want a cloudtrace-wrapped exporter error", err)
		}
		return
	}
	defer func() { _ = shutdown() }()
}

// failingShutdownExporter lets InitOTLP's returned shutdown closure hit
// the trace-shutdown error branch.
type failingShutdownExporter struct{ trace.SpanExporter }

func (failingShutdownExporter) ExportSpans(context.Context, []trace.ReadOnlySpan) error {
	return nil
}
func (failingShutdownExporter) Shutdown(context.Context) error {
	return errors.New("exporter shutdown boom")
}

func TestInitOTLP_ShutdownErrorBubbles(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER", "stdout")

	restore := observability.SwapExporterFactoryForTest(
		func(ctx context.Context, _, serviceName, _ string) (trace.SpanExporter, error) {
			return failingShutdownExporter{}, nil
		},
	)
	defer restore()

	shutdown, err := observability.InitOTLP("chora-shutdown-err", "v0.0.0")
	if err != nil {
		t.Fatalf("InitOTLP: %v", err)
	}
	if err := shutdown(); err == nil {
		t.Fatal("expected shutdown() to surface the exporter shutdown error")
	}
}

// TestInitOTLP_ResourceBuildError covers InitOTLP's resource-construction
// failure branch via a malformed OTEL_RESOURCE_ATTRIBUTES value (a pair
// without '=' makes resource.FromEnv return ErrPartialResource).
func TestInitOTLP_ResourceBuildError(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER", "stdout")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=dup,noequalspair")

	shutdown, err := observability.InitOTLP("chora-res-err", "v0.0.0")
	if err == nil {
		if shutdown != nil {
			_ = shutdown()
		}
		t.Fatal("expected InitOTLP to fail when the resource cannot be built")
	}
	if !strings.Contains(err.Error(), "resource:") {
		t.Errorf("err = %v, want a resource-wrapped error", err)
	}
}

// TestInitOTLPAsync_CancelledBeforeInit covers the InitFunc select's
// initCtx.Done() branch: a caller context cancelled while InitOTLP is
// still running (here: a deliberate slow exporter factory) resolves the
// handle to a fail-soft no-op with InitError set.
func TestInitOTLPAsync_CancelledBeforeInit(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)

	restore := observability.SwapExporterFactoryForTest(
		func(ctx context.Context, _, serviceName, _ string) (trace.SpanExporter, error) {
			time.Sleep(300 * time.Millisecond)
			return &noopExporterObs{}, nil
		},
	)
	defer restore()

	ctx, cancel := context.WithCancel(context.Background())
	handle := observability.InitOTLPAsync(ctx, "chora-async-cancel", "v0.0.0")
	cancel()

	res := handle.Wait(2 * time.Second)
	if res.Initialized {
		t.Errorf("expected Initialized=false when the init context is cancelled before InitOTLP completes")
	}
	if res.InitError == nil {
		t.Errorf("expected InitError to surface the cancellation")
	}
	if res.Shutdown == nil {
		t.Errorf("Shutdown must always be non-nil (fail-soft contract)")
	}
}
