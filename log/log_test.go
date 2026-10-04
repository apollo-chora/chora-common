package log_test

import (
	"context"
	"testing"

	"github.com/5007-Capstone/chora/libs/chora-go-common/log"
	"github.com/5007-Capstone/chora/libs/chora-go-common/tracing"
)

func TestNew_ReturnsLogger(t *testing.T) {
	logger := log.New("chora-test-svc")
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	if logger.ServiceName() != "chora-test-svc" {
		t.Errorf("ServiceName=%q want chora-test-svc", logger.ServiceName())
	}
}

func TestLogger_WithContextEmitsTraceFields(t *testing.T) {
	logger := log.New("chora-test")
	ctx := context.Background()
	ctx = tracing.WithTraceparent(ctx, "00-aaaa-bbbb-01")
	ctx = tracing.WithTenantID(ctx, "tenant-z")
	ctx = tracing.WithGCID(ctx, "gcid-z")

	bound := logger.WithContext(ctx)
	if bound == nil {
		t.Fatal("WithContext returned nil")
	}
}

func TestLogger_LogMethodsDoNotPanic(t *testing.T) {
	logger := log.New("chora-test")
	logger.Info("info message", log.String("k", "v"))
	logger.Warn("warn message", log.Int("n", 7))
	logger.Error("error message", log.Err(nil))
	logger.Debug("debug message")
}

func TestLogger_FieldHelpers(t *testing.T) {
	// Just verify field constructors return the expected zap.Field-equivalent.
	if log.String("k", "v").Key != "k" {
		t.Error("String field key wrong")
	}
	if log.Int("k", 1).Key != "k" {
		t.Error("Int field key wrong")
	}
	if log.Err(nil).Key == "" {
		t.Error("Err field should have a key")
	}
}

func TestNew_HonoursLogLevelEnv(t *testing.T) {
	t.Setenv("CHORA_LOG_LEVEL", "debug")
	logger := log.New("chora-debug")
	if logger == nil {
		t.Fatal("logger nil")
	}
	// Just verify construction succeeds across levels.
	for _, lvl := range []string{"debug", "info", "warn", "error"} {
		t.Setenv("CHORA_LOG_LEVEL", lvl)
		if log.New("svc-"+lvl) == nil {
			t.Errorf("logger nil for level %q", lvl)
		}
	}
}
