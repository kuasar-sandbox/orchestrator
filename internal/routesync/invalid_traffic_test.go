package routesync

import (
	"encoding/json"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func TestTrafficPolicyInvalidWireIsStrictAndIndependentOfState(t *testing.T) {
	for _, raw := range []string{
		`{"traffic_policy_invalid":null}`, `{"traffic_policy_invalid":1}`, `{"traffic_policy_invalid":"true"}`, `{"traffic_policy_invalid":{}}`,
		`{"traffic_policy_invalid":[]}`, `{"traffic_policy_invalid":true,"traffic_policy_invalid":false}`,
		`{"TRAFFIC_POLICY_INVALID":null}`, `{"traffic_policy_invalid":true,"TRAFFIC_POLICY_INVALID":false}`,
		`{"traffic_policy_invalid":true,"max_inflight":{"total":1}}`,
		`{"traffic_policy_invalid":true,"max_inflight":{}}`,
		`{"MAX_INFLIGHT":null}`,
	} {
		var route RouteEntry
		if err := json.Unmarshal([]byte(raw), &route); err == nil {
			t.Errorf("accepted malformed projection %s", raw)
		}
	}
	for _, state := range []string{StateStarting, StateRunning, StatePaused} {
		for _, invalid := range []bool{true, false} {
			original := RouteEntry{SandboxID: "s1", StableID: "stable-s1", State: state, TrafficPolicyInvalid: invalid}
			raw, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			var route RouteEntry
			if err := json.Unmarshal(raw, &route); err != nil {
				t.Fatal(err)
			}
			if route.State != state || route.TrafficPolicyInvalid != invalid || route.MaxInflightPatch != nil {
				t.Fatalf("roundtrip: %+v", route)
			}
		}
	}
}

func TestTrafficPolicyInvalidCannotBeSetByPublicTrafficMetadata(t *testing.T) {
	for _, raw := range []string{`{"traffic_policy_invalid":true}`, `{"traffic_policy_invalid":false}`, `{"max_inflight":{},"traffic_policy_invalid":true}`} {
		if _, err := sandboxcfg.ParseTrafficPatch(raw); err == nil {
			t.Fatalf("public metadata accepted internal flag: %s", raw)
		}
	}
}
