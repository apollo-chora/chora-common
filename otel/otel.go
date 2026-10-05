// Package otel wires the OpenTelemetry SDK to emit traces via standard
// OTLP/gRPC per CLAUDE.md (Tier 3 D13): "OTLP everywhere — every service
// emits from day-1; no opt-in flags". The root compose runs an OTel
// Collector at otel-collector:4317; services point at it through
// OTEL_EXPORTER_OTLP_ENDPOINT.
//
// 2026-10-05: HISTORY — the dedicated Cloud Trace exporter from
// GoogleCloudPlatform/opentelemetry-operations-go was removed with the
// platform's Google Cloud exit. The package now builds the standard
// OTLP/gRPC exporter (otlptracegrpc) against OTEL_EXPORTER_OTLP_ENDPOINT;
// when the endpoint is unset (local dev) traces stream to stdout via
// stdouttrace, so startup never fails on trace wiring.
package otel

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// exporterFactory builds a SpanExporter for the given endpoint hint.
// Swapped out in tests via SwapExporterFactoryForTest so the OTLP
// happy-path is exercised without a live collector. Default uses
// the OTLP/gRPC exporter.
var exporterFactory ExporterFactory = defaultExporterFactory

// Init wires the trace exporter and registers a global TracerProvider.
// Returns a shutdown func the caller MUST defer in main.
//
// serviceName becomes the OTLP service.name resource attribute (must be
// non-empty). version is the build SHA / semver tag (free-form; emitted as
// service.version resource attribute).
//
// Public API is intentionally backward-compatible with the pre-Wave-B
// signature: existing callers (`Init(ctx, name, version)`) keep working
// with no edits required.
func Init(ctx context.Context, serviceName, version string) (func(context.Context) error, error) {
	if serviceName == "" {
		return nil, errors.New("otel: serviceName must be non-empty")
	}

	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))

	exp, err := exporterFactory(ctx, endpoint, serviceName, version)
	if err != nil {
		return nil, err
	}

	res, err := buildResource(ctx, serviceName, version)
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(parseSampler()),
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(5*time.Second),
		),
	)
	otel.SetTracerProvider(tp)

	// Global W3C TraceContext + Baggage propagator. Without this, the
	// OTel SDK's default propagator is a no-op — meaning every HTTP
	// middleware that calls prop.Extract(...) ignores inbound traceparent
	// headers and starts a new root span instead of continuing the
	// browser/upstream trace. This is what kept gateway/creation/sharing/
	// tenancy/etc. invisible end-to-end in Cloud Trace pre-2026-05-17
	// (E2E-BE-AI-ASSIST-TRACE-EXPORT-PERM).
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func(ctx context.Context) error {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("trace shutdown: %w", err)
		}
		return nil
	}, nil
}

// Tracer returns a Tracer scoped to the given service — sugar for otel.Tracer.
func Tracer(serviceName string) trace.Tracer { return otel.Tracer(serviceName) }

// -----------------------------------------------------------------------------
// Exporter factory
// -----------------------------------------------------------------------------

// defaultExporterFactory routes to the OTLP/gRPC exporter when
// OTEL_EXPORTER_OTLP_ENDPOINT is set and stdout in dev. The endpoint is
// passed to the exporter verbatim (host:port); the exporter dials
// lazily, so construction never blocks or fails on an unreachable
// collector.
func defaultExporterFactory(ctx context.Context, endpoint, serviceName, version string) (sdktrace.SpanExporter, error) {
	if isDevExport() {
		log.Printf("otel: dev mode; spans -> stdout (service=%s version=%s)", serviceName, version)
		exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("stdouttrace: %w", err)
		}
		return exp, nil
	}

	log.Printf("otel: otlp exporter (endpoint=%s) service=%s version=%s",
		endpoint, serviceName, version)
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlptracegrpc: %w", err)
	}
	return exp, nil
}

// isDevExport returns true when the operator has explicitly opted into
// stdout export (OTEL_EXPORTER=stdout) or when OTEL_EXPORTER_OTLP_ENDPOINT
// is unset (local dev). The historical dev fallback (unset endpoint ->
// stdout) is preserved for local `go run`.
func isDevExport() bool {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_EXPORTER")), "stdout") {
		return true
	}
	return strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) == ""
}

func projectIDForLog(p string) string {
	if p == "" {
		return "<adc-default>"
	}
	return p
}

// -----------------------------------------------------------------------------
// Resource + sampler
// -----------------------------------------------------------------------------

// buildResource assembles the OpenTelemetry Resource that Cloud Trace will
// stamp on every emitted span. service.name + service.version are mandatory;
// deployment.environment + service.namespace=chora are added so traces
// segregate cleanly per-environment in the Cloud Trace UI.
func buildResource(ctx context.Context, serviceName, version string) (*resource.Resource, error) {
	env := strings.TrimSpace(os.Getenv("DEPLOYMENT_ENVIRONMENT"))
	if env == "" {
		env = "dev"
	}
	attrs := []attribute.KeyValue{
		attribute.String("service.name", serviceName),
		attribute.String("service.version", version),
		attribute.String("service.namespace", "chora"),
		attribute.String("deployment.environment", env),
	}
	if region := strings.TrimSpace(os.Getenv("CLOUD_REGION")); region != "" {
		attrs = append(attrs, attribute.String("cloud.region", region))
	}
	return resource.New(ctx,
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),      // honours OTEL_RESOURCE_ATTRIBUTES
		resource.WithTelemetrySDK(), // adds telemetry.sdk.* attrs
	)
}

// parseSampler returns the configured sampler. Honours OTEL_TRACES_SAMPLER_ARG
// (a decimal in [0,1]) when OTEL_TRACES_SAMPLER=parentbased_traceidratio is
// set; defaults to AlwaysSample. Cost-aware sampling can be flipped on at
// the env layer without re-deploying.
func parseSampler() sdktrace.Sampler {
	switch strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER")) {
	case "parentbased_traceidratio":
		ratio := 1.0
		if v := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER_ARG")); v != "" {
			if f, err := parseRatio(v); err == nil {
				ratio = f
			}
		}
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	case "always_off":
		return sdktrace.NeverSample()
	default:
		return sdktrace.AlwaysSample()
	}
}

func parseRatio(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	if err != nil {
		return 0, err
	}
	if f < 0 || f > 1 {
		return 0, fmt.Errorf("ratio out of range: %v", f)
	}
	return f, nil
}
