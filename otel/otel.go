// Package otel wires the OpenTelemetry SDK to emit traces directly to Cloud
// Trace per CLAUDE.md (Tier 3 D13): "OTLP everywhere — every service emits
// directly to Cloud Trace from day-1; no opt-in flags; no OTel Collector".
//
// 2026-05-14 (Wave B / tracker #146): swapped the OTLP gRPC exporter for the
// dedicated Cloud Trace exporter from GoogleCloudPlatform/opentelemetry-
// operations-go. The previous wiring called otlptracegrpc.New with
// WithInsecure() against telemetry.googleapis.com:443 — the TLS handshake
// failed silently and every span was dropped. The cloudtrace exporter
// handles TLS + ADC bearer-token auth natively, which is the
// Google-recommended path for emitting from any Google-managed compute.
//
// Endpoint env vars (OTEL_EXPORTER_OTLP_ENDPOINT) are still honoured as a
// no-op pass-through so deployment manifests don't have to change. When
// unset (local dev), traces stream to stdout via stdouttrace.
package otel

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	cloudtrace "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// exporterFactory builds a SpanExporter for the given endpoint hint.
// Swapped out in tests via SwapExporterFactoryForTest so the cloudtrace
// happy-path is exercised without needing real ADC + project. Default uses
// the Cloud Trace exporter.
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

// defaultExporterFactory routes to Cloud Trace when running against Google
// infra (the common case) and stdout in dev. The OTEL_EXPORTER_OTLP_ENDPOINT
// env var, when set, is recorded in the log line for operator debugging but
// no longer changes the exporter — cloudtrace.New does its own project
// discovery via ADC and the gRPC endpoint is fixed at cloudtrace.googleapis.com.
// This is the documented Google-recommended path; see CLAUDE.md §1 +
// project memory project_chora_gcp_stack.md.
func defaultExporterFactory(ctx context.Context, endpoint, serviceName, version string) (sdktrace.SpanExporter, error) {
	if isDevExport() {
		log.Printf("otel: dev mode (OTEL_EXPORTER=stdout); spans -> stdout (service=%s version=%s)", serviceName, version)
		exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("stdouttrace: %w", err)
		}
		return exp, nil
	}

	projectID := strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_PROJECT"))
	log.Printf("otel: cloudtrace exporter (project=%s endpoint_hint=%s) service=%s version=%s",
		projectIDForLog(projectID), endpoint, serviceName, version)

	opts := []cloudtrace.Option{cloudtrace.WithContext(ctx)}
	if projectID != "" {
		opts = append(opts, cloudtrace.WithProjectID(projectID))
	}

	exp, err := cloudtrace.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("cloudtrace: %w", err)
	}
	return exp, nil
}

// isDevExport returns true when the operator has explicitly opted into stdout
// export (OTEL_EXPORTER=stdout) or when no GOOGLE_APPLICATION_CREDENTIALS /
// GOOGLE_CLOUD_PROJECT is detectable AND OTEL_EXPORTER_OTLP_ENDPOINT is
// unset. The historical dev fallback (unset endpoint -> stdout) is preserved
// for local `go run`.
func isDevExport() bool {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_EXPORTER")), "stdout") {
		return true
	}
	// Mirror historical behaviour: if neither the endpoint nor ADC project
	// is set, we're almost certainly on a developer laptop without a key.
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" &&
		os.Getenv("GOOGLE_CLOUD_PROJECT") == "" &&
		os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		return true
	}
	return false
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
