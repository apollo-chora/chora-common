// Package log wraps zap with Chora-flavoured defaults: structured JSON
// output, trace-correlated fields drawn from context, and a CHORA_LOG_LEVEL
// env knob (debug / info / warn / error). Per CLAUDE.md §6, every log line
// emitted from a service should correlate with the active Cloud Trace span
// via the traceparent stamped on context (see tracing package).
//
// This package intentionally does NOT export raw zap types — services depend
// only on the Field constructors here, which insulates them from upstream zap
// API churn.
package log

import (
	"context"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/apollo-chora/chora-common/tracing"
)

// Logger is the Chora structured logger.
type Logger struct {
	z       *zap.Logger
	service string
}

// Field is a typed structured-log field. Backed by zap.Field but exposed via
// constructors so callers don't import zap directly.
type Field struct {
	Key   string
	value zap.Field
}

// String returns a string-typed field.
func String(key, val string) Field { return Field{Key: key, value: zap.String(key, val)} }

// Int returns an int-typed field.
func Int(key string, val int) Field { return Field{Key: key, value: zap.Int(key, val)} }

// Err returns an error-typed field. Always uses key "error" for consistency.
func Err(err error) Field { return Field{Key: "error", value: zap.Error(err)} }

// stdoutSyncer writes to os.Stdout and treats Sync as a no-op.
//
// Why: os.Stdout is almost never a regular file. In a terminal it is a TTY, in
// CI and any piped invocation it is a pipe, and fsync on either returns EINVAL
// ("sync /dev/stdout: invalid argument") — which surfaced as a test failure
// only when the suite ran with output redirected, and as a spurious main()
// error path for any service that defers logger.Sync() under a pipe. Syncing
// stdout is meaningless in every deployment shape this package has: there is no
// OS-level buffer for a pipe or TTY that zap needs to flush (zap writes are
// unbuffered through the core). A no-op is therefore the honest contract, and
// it makes Logger.Sync() deterministic regardless of how the process is run.
type stdoutSyncer struct{}

func (stdoutSyncer) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (stdoutSyncer) Sync() error                 { return nil }

// New constructs a Chora logger for the given service name. The CHORA_LOG_LEVEL
// env var (debug / info / warn / error) controls verbosity; defaults to info.
func New(serviceName string) *Logger {
	level := levelFromEnv()

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "ts"
	encCfg.MessageKey = "msg"
	encCfg.LevelKey = "level"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encCfg),
		stdoutSyncer{},
		level,
	)
	z := zap.New(core).With(zap.String("service", serviceName))
	return &Logger{z: z, service: serviceName}
}

// ServiceName returns the canonical service name attached to every log line.
func (l *Logger) ServiceName() string { return l.service }

// WithContext returns a logger that adds traceparent/tenant_id/gcid fields
// drawn from ctx to every emitted line.
func (l *Logger) WithContext(ctx context.Context) *Logger {
	if ctx == nil {
		return l
	}
	fields := make([]zap.Field, 0, 3)
	if tp := tracing.TraceparentFromContext(ctx); tp != "" {
		fields = append(fields, zap.String("traceparent", tp))
	}
	if tenant := tracing.TenantIDFromContext(ctx); tenant != "" {
		fields = append(fields, zap.String("tenant_id", tenant))
	}
	if gcid := tracing.GCIDFromContext(ctx); gcid != "" {
		fields = append(fields, zap.String("gcid", gcid))
	}
	if len(fields) == 0 {
		return l
	}
	return &Logger{z: l.z.With(fields...), service: l.service}
}

// Info logs at info level.
func (l *Logger) Info(msg string, fields ...Field) { l.z.Info(msg, unwrap(fields)...) }

// Warn logs at warn level.
func (l *Logger) Warn(msg string, fields ...Field) { l.z.Warn(msg, unwrap(fields)...) }

// Error logs at error level.
func (l *Logger) Error(msg string, fields ...Field) { l.z.Error(msg, unwrap(fields)...) }

// Debug logs at debug level.
func (l *Logger) Debug(msg string, fields ...Field) { l.z.Debug(msg, unwrap(fields)...) }

// Sync flushes buffered log entries. Services should defer logger.Sync() in main.
func (l *Logger) Sync() error { return l.z.Sync() }

func unwrap(fields []Field) []zap.Field {
	out := make([]zap.Field, len(fields))
	for i, f := range fields {
		out[i] = f.value
	}
	return out
}

func levelFromEnv() zapcore.Level {
	switch strings.ToLower(os.Getenv("CHORA_LOG_LEVEL")) {
	case "debug":
		return zapcore.DebugLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}
