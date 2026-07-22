package placement

import (
	"bytes"
	"testing"
)

func TestNormalizedDemandCanonicalRoundTrip(t *testing.T) {
	encoded, err := NormalizeSandboxDemand(SandboxDemand{SlotUnits: 1, StartupBudgetMemory: 1024})
	if err != nil {
		t.Fatal(err)
	}
	demand, err := ParseNormalizedDemand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if demand.Kind != ObjectSandbox || demand.Sandbox == nil || demand.Sandbox.StartupBudgetMemory != 1024 {
		t.Fatalf("demand = %+v", demand)
	}
	nonCanonical := append([]byte{' ', '\n'}, encoded...)
	if _, err := ParseNormalizedDemand(nonCanonical); err == nil {
		t.Fatal("non-canonical demand was accepted")
	}
	if bytes.Contains(encoded, []byte("build")) {
		t.Fatalf("canonical sandbox encoding contains inactive union arm: %s", encoded)
	}
}

func TestNormalizedDemandRejectsUnknownOrTrailingData(t *testing.T) {
	for _, encoded := range [][]byte{
		[]byte(`{"version":1,"kind":"sandbox","sandbox":{"slot_units":1},"unknown":true}`),
		[]byte(`{"version":1,"kind":"sandbox","sandbox":{"slot_units":1}} {}`),
		[]byte(`{"version":1,"kind":"build","build":{"slots":0}}`),
		[]byte(`{"version":1,"kind":"build","build":{"slots":1,"cpu":1000}}`),
		[]byte(`{"version":1,"kind":"build","build":{"slots":1,"memory":536870912}}`),
	} {
		if _, err := ParseNormalizedDemand(encoded); err == nil {
			t.Fatalf("invalid demand accepted: %s", encoded)
		}
	}
}
