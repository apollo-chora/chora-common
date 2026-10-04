package pubsubinbox_test

import (
	"testing"

	"github.com/5007-Capstone/chora/libs/chora-go-common/pubsubinbox"
)

// Every inbox has exactly one owner. A Go map literal cannot hold a duplicate
// key (it is a compile error), so the real risk is not duplication but an inbox
// that is silently absent — which Owner() reports honestly.
func TestOwner_UnownedInboxIsReportedNotGuessed(t *testing.T) {
	if _, ok := pubsubinbox.Owner("no-such-inbox"); ok {
		t.Fatal("Owner() claimed to own an inbox that is not registered")
	}
	// The fail-loud contract: callers branch on ok. If Owner ever started
	// returning a plausible default instead of false, the gateway would go back
	// to silently misrouting unknown inboxes to chora-delivery.
	svc, ok := pubsubinbox.Owner("weakness-grown")
	if !ok || svc != pubsubinbox.Sharing {
		t.Fatalf("Owner(weakness-grown) = %q,%v; want chora-sharing,true", svc, ok)
	}
}

// The counts are asserted so that adding an inbox to the registry without
// adding it to the owning service's mux is impossible to do quietly: this test
// pins the shape, and the service's own contract test pins the mounts.
func TestInboxesFor_OwnershipCounts(t *testing.T) {
	for _, tc := range []struct {
		svc  pubsubinbox.Service
		want int
	}{
		{pubsubinbox.Delivery, 10}, // +2: the CHO-1827 course-progress pair
		{pubsubinbox.Identity, 1},
		{pubsubinbox.Sharing, 3},
		{pubsubinbox.Consumption, 31}, // +2: ADR-244 D5 atom-updated + kg-retention delivery fix
	} {
		if got := len(pubsubinbox.InboxesFor(tc.svc)); got != tc.want {
			t.Errorf("InboxesFor(%s) = %d inboxes, want %d", tc.svc, got, tc.want)
		}
	}
	if got, want := len(pubsubinbox.All()), 45; got != want {
		t.Errorf("registry holds %d inboxes, want %d — every inbox must have an owner", got, want)
	}
}

// The gateway forwards the FULL path; downstreams must mount exactly this.
func TestPath_IsTheMountedRoute(t *testing.T) {
	if got, want := pubsubinbox.Path("weakness-grown"), "/api/internal/pubsub/weakness-grown"; got != want {
		t.Fatalf("Path() = %q, want %q", got, want)
	}
}

// InboxesFor must be deterministic — it feeds the gateway's boot log and the
// services' contract tests.
func TestInboxesFor_IsSorted(t *testing.T) {
	got := pubsubinbox.InboxesFor(pubsubinbox.Sharing)
	want := []string{"course-published", "live-quiz-scores", "weakness-grown"}
	if len(got) != len(want) {
		t.Fatalf("InboxesFor(Sharing) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("InboxesFor(Sharing) = %v, want %v", got, want)
		}
	}
}
