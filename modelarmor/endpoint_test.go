// Unit test for the regional endpoint resolver. Cloud Model Armor
// templates are REGIONAL — the default global endpoint
// (modelarmor.googleapis.com) returns TEMPLATE_NOT_FOUND against a
// regional template. NewScreener MUST dial the regional gRPC endpoint
// modelarmor.{location}.rep.googleapis.com:443
// (feedback_model_armor_regional_endpoint; mirrors armorplugin +
// chora-model-gateway which already pin the regional host).
package modelarmor

import "testing"

func TestRegionalEndpoint(t *testing.T) {
	cases := map[string]string{
		"us-central1":    "modelarmor.us-central1.rep.googleapis.com:443",
		"asia-southeast1": "modelarmor.asia-southeast1.rep.googleapis.com:443",
	}
	for loc, want := range cases {
		if got := regionalEndpoint(loc); got != want {
			t.Errorf("regionalEndpoint(%q) = %q; want %q", loc, got, want)
		}
	}
}
