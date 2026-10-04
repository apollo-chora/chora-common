// Package otel — supplementary Init error-path tests (external, mirrors
// the existing otel_test.go conventions incl. env snapshotting).
package otel_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	choraotel "github.com/apollo-chora/chora-common/otel"
)

// TestInit_ResourceError_BubblesUp is intentionally NOT present: the otel
// sdk resource.New error branch (Init's `resource:` wrap) is unreachable —
// resource.New only errors when every detector fails, and WithFromEnv /
// WithTelemetrySDK / WithAttributes never fail. Left uncovered by design.

// failingShutdownExporter fails on Shutdown so the init-shutdown error
// path is reachable.
type failingShutdownExporter struct{}

func (*failingShutdownExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return nil
}
func (*failingShutdownExporter) Shutdown(context.Context) error {
	return errors.New("exporter shutdown failed")
}

// TestInit_ShutdownError_BubblesUp — the shutdown closure must wrap a
// failed provider shutdown in "trace shutdown: ...".
func TestInit_ShutdownError_BubblesUp(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)
	t.Setenv("OTEL_EXPORTER", "stdout")

	restore := choraotel.SwapExporterFactoryForTest(
		func(context.Context, string, string, string) (sdktrace.SpanExporter, error) {
			return &failingShutdownExporter{}, nil
		},
	)
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdown, err := choraotel.Init(ctx, "chora-shut-err", "v0.0.0")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	err = shutdown(ctx)
	if err == nil {
		t.Fatal("expected shutdown error")
	}
	if !strings.Contains(err.Error(), "trace shutdown") {
		t.Errorf("unexpected shutdown error: %v", err)
	}
}
