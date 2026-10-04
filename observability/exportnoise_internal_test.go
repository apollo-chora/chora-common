// exportnoise_internal_test.go — unit test for the trace-export-noise filter.
//
// Regression context (2026-05-29 trace wave): the cloudtrace exporter's
// underlying Google Cloud Go client auto-creates a client span for every
// BatchWriteSpans RPC once a global TracerProvider is registered. Those spans
// are themselves exported, producing a self-referential flood (307
// BatchWriteSpans traces observed in chora-489812).
//
// The fix (2026-06-01, CR_QGEN_GLOBAL_TIER_IMAGE_TRACE §3 Phase B1) moved the
// filter from a sdktrace.Sampler to a sdktrace.SpanExporter wrapper, because
// otelgrpc assigns the gRPC client span name AFTER ShouldSample has run — so
// the name-matching sampler never fired. At ExportSpans time the name is final.
package observability

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// captureExporter records the names handed to its ExportSpans so the test can
// assert which spans survived the filtering wrapper.
type captureExporter struct {
	names []string
	calls int
}

func (c *captureExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	c.calls++
	for _, s := range spans {
		c.names = append(c.names, s.Name())
	}
	return nil
}

func (c *captureExporter) Shutdown(context.Context) error { return nil }

func TestFilteringExporter_DropsExporterOwnRPCs(t *testing.T) {
	cap := &captureExporter{}
	filter := newFilteringExporter(cap)

	spans := recordSpans(t, []string{
		"google.devtools.cloudtrace.v2.TraceService/BatchWriteSpans",
		"opentelemetry.proto.collector.trace.v1.TraceService/Export",
		"GET /healthz",
		"chora.services.model_gateway.v1.ModelGatewayService/Invoke",
		"invoke_agent qgen_critic",
	})
	if err := filter.ExportSpans(context.Background(), spans); err != nil {
		t.Fatalf("ExportSpans: %v", err)
	}

	got := map[string]bool{}
	for _, n := range cap.names {
		got[n] = true
	}
	// Self-spans must be filtered.
	for _, n := range []string{
		"google.devtools.cloudtrace.v2.TraceService/BatchWriteSpans",
		"opentelemetry.proto.collector.trace.v1.TraceService/Export",
	} {
		if got[n] {
			t.Errorf("self-span %q leaked to delegate", n)
		}
	}
	// Application spans must pass through.
	for _, n := range []string{
		"GET /healthz",
		"chora.services.model_gateway.v1.ModelGatewayService/Invoke",
		"invoke_agent qgen_critic",
	} {
		if !got[n] {
			t.Errorf("application span %q was dropped", n)
		}
	}
}

// TestFilteringExporter_AllDroppedSkipsDelegate proves the wrapper never calls
// the delegate when every span is a self-span — exporting them would itself
// mint a new BatchWriteSpans span and re-arm the loop.
func TestFilteringExporter_AllDroppedSkipsDelegate(t *testing.T) {
	cap := &captureExporter{}
	filter := newFilteringExporter(cap)

	spans := recordSpans(t, []string{
		"google.devtools.cloudtrace.v2.TraceService/BatchWriteSpans",
		"opentelemetry.proto.collector.trace.v1.TraceService/Export",
	})
	if err := filter.ExportSpans(context.Background(), spans); err != nil {
		t.Fatalf("ExportSpans: %v", err)
	}
	if cap.calls != 0 {
		t.Errorf("delegate ExportSpans called %d times; want 0 (all spans filtered)", cap.calls)
	}
}

// spanRecorder + recordSpans mint genuine ReadOnlySpan values with chosen names
// (the same span type the filter sees in production from otelgrpc).
type spanRecorder struct{ out *[]sdktrace.ReadOnlySpan }

func (r spanRecorder) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (r spanRecorder) OnEnd(s sdktrace.ReadOnlySpan)                   { *r.out = append(*r.out, s) }
func (r spanRecorder) Shutdown(context.Context) error                 { return nil }
func (r spanRecorder) ForceFlush(context.Context) error               { return nil }

func recordSpans(t *testing.T, names []string) []sdktrace.ReadOnlySpan {
	t.Helper()
	var out []sdktrace.ReadOnlySpan
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder{out: &out}))
	tr := tp.Tracer("test")
	for _, n := range names {
		_, span := tr.Start(context.Background(), n)
		span.End()
	}
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	return out
}
