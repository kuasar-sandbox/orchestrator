package sandboxcfg

import (
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestTrafficMetadataAbsentRemainsAbsent(t *testing.T) {
	in := map[string]string{"ordinary": "value"}
	out, err := NormalizeTrafficMetadata(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := out[NsTraffic]; present || !reflect.DeepEqual(out, in) {
		t.Fatalf("normalized absent traffic = %+v", out)
	}
	merged, err := MergeCreateMetadata(in, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := merged[NsTraffic]; present {
		t.Fatalf("merge materialized traffic: %+v", merged)
	}
}

func TestTrafficPatchCanonicalExplicitZeroAndLeafMerge(t *testing.T) {
	base := map[string]string{NsTraffic: `{"max_inflight":{"total":32,"exec":4,"forward":7}}`}
	over := map[string]string{NsTraffic: `{"max_inflight":{"exec":0,"e2b:envd":3}}`}
	merged, err := MergeMetadata(base, over)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"max_inflight":{"total":32,"forward":7,"e2b:envd":3,"exec":0}}`
	if got := merged[NsTraffic]; got != want {
		t.Fatalf("traffic merge = %s, want %s", got, want)
	}
	patch, err := ParseTrafficPatch(merged[NsTraffic])
	if err != nil {
		t.Fatal(err)
	}
	if patch.MaxInflight == nil || patch.MaxInflight.Exec == nil || *patch.MaxInflight.Exec != 0 {
		t.Fatalf("explicit zero was lost: %+v", patch)
	}
	if patch.MaxInflight.E2BCodeInterpreter != nil {
		t.Fatalf("absent leaf was materialized: %+v", patch)
	}
}

func TestTrafficPatchStrictDecode(t *testing.T) {
	tests := map[string]string{
		"empty":          "",
		"null root":      "null",
		"array root":     "[]",
		"unknown root":   `{"future":{}}`,
		"null object":    `{"max_inflight":null}`,
		"unknown leaf":   `{"max_inflight":{"future":1}}`,
		"null leaf":      `{"max_inflight":{"total":null}}`,
		"negative":       `{"max_inflight":{"total":-1}}`,
		"fraction":       `{"max_inflight":{"total":1.5}}`,
		"string":         `{"max_inflight":{"total":"1"}}`,
		"overflow":       `{"max_inflight":{"total":4294967296}}`,
		"duplicate root": `{"max_inflight":{},"max_inflight":{}}`,
		"duplicate leaf": `{"max_inflight":{"total":1,"total":2}}`,
		"trailing":       `{"max_inflight":{}} {}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseTrafficPatch(raw); err == nil {
				t.Fatalf("ParseTrafficPatch accepted %q", raw)
			}
		})
	}
}

func TestTrafficPatchProfileValidation(t *testing.T) {
	for _, service := range []string{`"e2b:envd":0`, `"e2b:code-interpreter":1`} {
		patch, err := ParseTrafficPatch(`{"max_inflight":{` + service + `}}`)
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidateTrafficForProfile(types.ProfileBare, patch); err == nil || !strings.Contains(err.Error(), "bare") {
			t.Fatalf("bare validation error = %v", err)
		}
		if err := ValidateTrafficForProfile(types.ProfileE2B, patch); err != nil {
			t.Fatalf("e2b validation: %v", err)
		}
	}
	patch, err := ParseTrafficPatch(`{"max_inflight":{"total":1,"forward":0,"exec":2}}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateTrafficForProfile(types.ProfileBare, patch); err != nil {
		t.Fatalf("bare applicable traffic: %v", err)
	}
}

func TestMergeCreateMetadataMergesTrafficLeaves(t *testing.T) {
	defaults := map[string]string{NsTraffic: `{"max_inflight":{"total":10,"forward":4}}`}
	request := map[string]string{NsTraffic: `{"max_inflight":{"forward":0,"exec":2}}`}
	got, err := MergeCreateMetadata(defaults, request)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"max_inflight":{"total":10,"forward":0,"exec":2}}`
	if got[NsTraffic] != want {
		t.Fatalf("MergeCreateMetadata traffic = %s, want %s", got[NsTraffic], want)
	}
}
