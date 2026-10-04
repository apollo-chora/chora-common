// ensure_classify_test.go — CHO-2128 F3: EnsureSubscription PermissionDenied
// classification. Workload GSAs deliberately lack pubsub.subscriptions.get/
// create (subscriptions are provisioned OOB), so every boot's ensure calls
// fail PermissionDenied and log a multi-line IAM wall (ErrorInfo metadata +
// troubleshooter URL) — noise that buried sharing's closure subscriber and
// reads as an outage. PermissionDenied ensure failures are classified into a
// SHORT single-line error tagged ErrEnsurePermissionDenied so call sites can
// log one informative line and bind directly; every other code keeps its full
// detail (fail-loud unchanged).
package pubsub

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const iamWall = "User not authorized to perform this action.\nerror details: name = ErrorInfo reason = IAM_PERMISSION_DENIED domain = iam.googleapis.com metadata = map[permission:pubsub.subscriptions.get troubleshooter_url:https://console.cloud.google.com/iam-admin/troubleshooter]"

func TestClassifyEnsureError_PermissionDenied_SentinelAndSingleLine(t *testing.T) {
	in := status.Error(codes.PermissionDenied, iamWall)
	got := classifyEnsureError("get", "chora-sharing.closure-pseudonymise", in)

	if !errors.Is(got, ErrEnsurePermissionDenied) {
		t.Fatalf("errors.Is(ErrEnsurePermissionDenied) = false; want true\ngot: %v", got)
	}
	msg := got.Error()
	if !strings.Contains(msg, "chora-sharing.closure-pseudonymise") {
		t.Errorf("message must name the subscription; got %q", msg)
	}
	if strings.Contains(msg, "\n") || strings.Contains(msg, "error details") || strings.Contains(msg, "troubleshooter") {
		t.Errorf("message must be a single line without the IAM wall; got %q", msg)
	}
	if !strings.Contains(msg, "provisioned OOB") {
		t.Errorf("message should state the OOB-provisioning posture; got %q", msg)
	}
}

func TestClassifyEnsureError_OtherCodes_PreserveFullDetail(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"not_found", status.Error(codes.NotFound, "topic not found")},
		{"internal", status.Error(codes.Internal, "backend blew up")},
		{"plain", errors.New("plain non-grpc failure")},
	} {
		got := classifyEnsureError("create", "some-sub", tc.err)
		if errors.Is(got, ErrEnsurePermissionDenied) {
			t.Errorf("%s: classified as permission-denied; want untouched class", tc.name)
		}
		if !errors.Is(got, tc.err) {
			t.Errorf("%s: original error not wrapped (%%w); got %v", tc.name, got)
		}
		if !strings.Contains(got.Error(), "some-sub") {
			t.Errorf("%s: message must name the subscription; got %q", tc.name, got.Error())
		}
	}
}
