// Package uiprefs holds cross-service UI-preference contracts. The A+
// dashboard-layout wrapper (card) vocabulary lives here as ONE canonical
// definition that both the chora-gateway edge validator and the chora-identity
// persistence domain derive from, so the two backend validators can never drift
// apart by hand.
//
// CHO-2274: before this package, the wrapper key set was hand-maintained in
// three places (the FE WrapperKey union, the gateway edge map, and the identity
// domain slice). When "study" (CHO-2226) and "transcript" (CHO-2237) were added
// to the FE, the two backend lists were not updated, so every dashboard-layout
// save 422'd at the edge ("unknown dashboard wrapper key: study"). Collapsing
// the two backend copies into this single source removes that whole drift class
// on the backend side.
package uiprefs

// dashboardWrapperKeys is the canonical, ordered A+ dashboard wrapper (card)
// vocabulary. Order is the canonical DEFAULT order (map-dominant) used to
// reconcile added wrappers.
//
// HARD INVARIANT: these string values are PERSISTED in users.ui_preferences
// (chora-identity migration 0034); renaming one silently orphans every stored
// layout. Append only.
//
// This slice MUST mirror the frontend WrapperKey union + DEFAULT_ORDER in
// chora-web/src/app/features/surfaces/aplus/dashboard/dashboard-layout.model.ts.
// The FE owns the vocabulary (a wrapper key is a FE card); adding a dashboard
// card means appending the key to that FE file AND here (the single backend
// place). The backend never interprets the keys, it only validates membership.
var dashboardWrapperKeys = []string{"map", "cast", "courses", "study", "transcript"}

// DashboardWrapperKeys returns the canonical ordered wrapper keys. It returns a
// copy so callers cannot mutate the source.
func DashboardWrapperKeys() []string {
	out := make([]string, len(dashboardWrapperKeys))
	copy(out, dashboardWrapperKeys)
	return out
}

// DashboardWrapperKeySet returns the canonical wrapper keys as a membership set
// for O(1) lookups by the edge and domain validators.
func DashboardWrapperKeySet() map[string]struct{} {
	set := make(map[string]struct{}, len(dashboardWrapperKeys))
	for _, k := range dashboardWrapperKeys {
		set[k] = struct{}{}
	}
	return set
}

// IsKnownDashboardWrapperKey reports whether k is a canonical wrapper key.
func IsKnownDashboardWrapperKey(k string) bool {
	for _, known := range dashboardWrapperKeys {
		if k == known {
			return true
		}
	}
	return false
}
