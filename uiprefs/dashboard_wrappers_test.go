package uiprefs_test

import (
	"testing"

	"github.com/apollo-chora/chora-common/uiprefs"
)

// TestDashboardWrapperKeys_CanonicalSet pins the canonical vocabulary. It must
// mirror the FE WrapperKey union + DEFAULT_ORDER (see the package godoc). A
// change here is a deliberate vocabulary change, never an accident; keep this in
// lockstep with the FE and this test guards the backend side.
func TestDashboardWrapperKeys_CanonicalSet(t *testing.T) {
	want := []string{"map", "cast", "courses", "study", "transcript"}
	got := uiprefs.DashboardWrapperKeys()
	if len(got) != len(want) {
		t.Fatalf("canonical wrapper keys = %v; want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("canonical wrapper keys = %v; want %v (order matters, it is the default order)", got, want)
		}
	}
}

func TestDashboardWrapperKeys_ReturnsCopy(t *testing.T) {
	a := uiprefs.DashboardWrapperKeys()
	a[0] = "mutated"
	if uiprefs.DashboardWrapperKeys()[0] != "map" {
		t.Fatal("DashboardWrapperKeys must return a copy; the canonical source was mutated")
	}
}

func TestIsKnownDashboardWrapperKey(t *testing.T) {
	for _, k := range []string{"map", "cast", "courses", "study", "transcript"} {
		if !uiprefs.IsKnownDashboardWrapperKey(k) {
			t.Errorf("IsKnownDashboardWrapperKey(%q) = false; want true", k)
		}
	}
	for _, k := range []string{"atlas", "", "MAP", "study "} {
		if uiprefs.IsKnownDashboardWrapperKey(k) {
			t.Errorf("IsKnownDashboardWrapperKey(%q) = true; want false", k)
		}
	}
}

func TestDashboardWrapperKeySet(t *testing.T) {
	set := uiprefs.DashboardWrapperKeySet()
	if len(set) != 5 {
		t.Fatalf("set size = %d; want 5", len(set))
	}
	if _, ok := set["study"]; !ok {
		t.Error("set must contain study")
	}
	if _, ok := set["transcript"]; !ok {
		t.Error("set must contain transcript")
	}
}
