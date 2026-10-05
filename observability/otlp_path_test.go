// otlp_path_test.go — statement-coverage extension for the real
// defaultExporterFactory OTLP path, the InitOTLP error branches
// (resource build, exporter shutdown) and the InitOTLPAsync
// cancellation branch.
package observability_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace"

	"github.com/apollo-chora/chora-common/observability"
)

// TestInitOTLP_OTLPEndpoint_ConstructsExporter drives
// defaultExporterFactory's OTLP branch with an endpoint configured.
// otlptracegrpc dials lazily, so construction succeeds even with no
// collector listening — the exporter is real and shuts down cleanly.
func TestInitOTLP_OTLPEndpoint_ConstructsExporter(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")

	shutdown, err := observability.InitOTLP("chora-otlp-endpoint", "v0.0.0")
	if err != nil {
		t.Fatalf("InitOTLP with OTLP endpoint: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown")
	}
	if err := shutdown(); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

// TestInitOTLP_OTLPEndpoint_ExplicitHostPort covers a host:port endpoint
// with an explicit scheme-less value, matching the compose collector
// address (otel-collector:4317).
func TestInitOTLP_OTLPEndpoint_ExplicitHostPort(t *testing.T) {
	envMuObs.Lock()
	defer envMuObs.Unlock()
	snapshotEnvObs(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")

	shutdown, err := observability.InitOTLP("chora-otlp-hostport", "v0.0.0")
	if err != nil {
		t.Fatalf("InitOTLP with host:port endpoint: %v", err)
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
