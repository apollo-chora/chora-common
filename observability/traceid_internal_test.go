// traceid_internal_test.go — malformed-input branches of the W3C
// traceparent → trace_id helper used by the slog traceHandler.
package observability

import "testing"

func TestExtractTraceIDFromW3C_MalformedInputs(t *testing.T) {
	// Not a 4-part traceparent.
	if got := extractTraceIDFromW3C("no-dashes-here"); got != "" {
		t.Errorf("extractTraceIDFromW3C(no-dashes) = %q, want empty", got)
	}
	// 4 parts but a trace chunk shorter than 32 hex chars.
	if got := extractTraceIDFromW3C("00-shortid-b7ad6b7169203331-01"); got != "" {
		t.Errorf("extractTraceIDFromW3C(short trace) = %q, want empty", got)
	}
	// 4 parts but a trace chunk longer than 32 hex chars.
	if got := extractTraceIDFromW3C("00-0af7651916cd43dd8448eb211c80319cffff-b7ad6b7169203331-01"); got != "" {
		t.Errorf("extractTraceIDFromW3C(long trace) = %q, want empty", got)
	}
}

func TestExtractTraceIDFromW3C_Valid(t *testing.T) {
	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	if got := extractTraceIDFromW3C(tp); got != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("extractTraceIDFromW3C(valid) = %q, want 0af7651916cd43dd8448eb211c80319c", got)
	}
}
