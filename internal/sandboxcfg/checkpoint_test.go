package sandboxcfg

import (
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func checkpointBool(value bool) *bool { return &value }

func TestParseCheckpointPolicyJSON(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want CheckpointPolicy
	}{
		{name: "empty object", raw: `{}`, want: CheckpointPolicy{}},
		{name: "both values", raw: ` { "merge_ref" : false, "drop_caches" : true } `,
			want: CheckpointPolicy{MergeRef: checkpointBool(false), DropCaches: checkpointBool(true)}},
		{name: "null inherits", raw: `{"merge_ref":null,"drop_caches":false}`,
			want: CheckpointPolicy{DropCaches: checkpointBool(false)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseCheckpointPolicyJSON(tc.raw)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("policy = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseCheckpointPolicyJSONRejectsInvalidShape(t *testing.T) {
	invalid := map[string]string{
		"empty":              "",
		"top null":           `null`,
		"array":              `[]`,
		"string":             `"value"`,
		"number":             `1`,
		"unknown":            `{"other":true}`,
		"wrong field case":   `{"Merge_Ref":true}`,
		"merge string":       `{"merge_ref":"false"}`,
		"merge number":       `{"merge_ref":0}`,
		"merge array":        `{"merge_ref":[]}`,
		"merge object":       `{"merge_ref":{}}`,
		"drop string":        `{"drop_caches":"true"}`,
		"second value":       `{} {}`,
		"trailing non-space": `{} trailing`,
	}
	for name, raw := range invalid {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseCheckpointPolicyJSON(raw); err == nil {
				t.Fatalf("ParseCheckpointPolicyJSON(%q) succeeded", raw)
			}
		})
	}
}

func TestCheckpointPolicyCloneAndOverlay(t *testing.T) {
	baseMerge := true
	baseDrop := false
	base := CheckpointPolicy{MergeRef: &baseMerge, DropCaches: &baseDrop}
	clone := CloneCheckpointPolicy(base)
	if clone.MergeRef == base.MergeRef || clone.DropCaches == base.DropCaches {
		t.Fatal("clone shares bool pointers")
	}
	*base.MergeRef = false
	if !*clone.MergeRef {
		t.Fatal("mutating source changed clone")
	}

	overrideMerge := false
	overlaid := OverlayCheckpointPolicy(clone, CheckpointPolicy{MergeRef: &overrideMerge})
	if overlaid.MergeRef == &overrideMerge || overlaid.DropCaches == clone.DropCaches {
		t.Fatal("overlay shares bool pointers")
	}
	if *overlaid.MergeRef || *overlaid.DropCaches {
		t.Fatalf("overlay = %+v, want merge=false drop=false", overlaid)
	}
	if (CheckpointPolicy{}).Empty() != true || overlaid.Empty() {
		t.Fatal("Empty returned the wrong result")
	}
}

func TestMarshalAndNormalizeCheckpointMetadata(t *testing.T) {
	canonical, err := MarshalCheckpointPolicyJSON(CheckpointPolicy{
		MergeRef: checkpointBool(false), DropCaches: checkpointBool(true),
	})
	if err != nil || canonical != `{"merge_ref":false,"drop_caches":true}` {
		t.Fatalf("canonical = %q, %v", canonical, err)
	}
	canonical, err = MarshalCheckpointPolicyJSON(CheckpointPolicy{DropCaches: checkpointBool(false)})
	if err != nil || canonical != `{"drop_caches":false}` {
		t.Fatalf("single-field canonical = %q, %v", canonical, err)
	}

	original := map[string]string{
		NsCheckpoint: ` { "merge_ref" : false, "drop_caches" : null } `,
		"keep":       "value",
	}
	got, err := NormalizeCheckpointMetadata(original)
	if err != nil {
		t.Fatal(err)
	}
	if got[NsCheckpoint] != `{"merge_ref":false}` || got["keep"] != "value" {
		t.Fatalf("normalized metadata = %+v", got)
	}
	got["keep"] = "changed"
	if original["keep"] != "value" {
		t.Fatal("normalization mutated its input map")
	}

	for _, raw := range []string{`{}`, `{"merge_ref":null}`, `{"drop_caches":null}`} {
		empty, err := NormalizeCheckpointMetadata(map[string]string{NsCheckpoint: raw, "keep": "value"})
		if err != nil {
			t.Fatalf("normalize %s: %v", raw, err)
		}
		if _, ok := empty[NsCheckpoint]; ok || empty["keep"] != "value" {
			t.Fatalf("empty policy was retained: %+v", empty)
		}
	}
	if _, err := NormalizeCheckpointMetadata(map[string]string{NsCheckpoint: `{"unknown":true}`}); err == nil {
		t.Fatal("invalid checkpoint metadata was accepted")
	}

	absent := map[string]string{"keep": "value"}
	absentClone, err := NormalizeCheckpointMetadata(absent)
	if err != nil {
		t.Fatal(err)
	}
	absentClone["keep"] = "changed"
	if absent["keep"] != "value" {
		t.Fatal("absent namespace path returned the caller's map")
	}
}

func TestCheckpointMetadataIsCreateScopedAndHostOnly(t *testing.T) {
	defaults := map[string]string{NsCheckpoint: `{"merge_ref":true}`, NsNetwork: `{"hostname":"template"}`}
	withoutRequest := MergeCreateMetadata(defaults, nil)
	if _, ok := withoutRequest[NsCheckpoint]; ok {
		t.Fatalf("template checkpoint policy leaked into Create: %+v", withoutRequest)
	}
	requestRaw := `{"drop_caches":false}`
	withRequest := MergeCreateMetadata(defaults, map[string]string{NsCheckpoint: requestRaw})
	if withRequest[NsCheckpoint] != requestRaw {
		t.Fatalf("explicit Create policy was not retained: %+v", withRequest)
	}

	spec, err := ParseSpec(map[string]string{NsCheckpoint: `{"merge_ref":false,"drop_caches":false}`})
	if err != nil {
		t.Fatal(err)
	}
	p := baseParams(types.ProfileBare)
	p.Spec = spec
	body, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{NsCheckpoint, "merge_ref", "drop_caches"} {
		if strings.Contains(string(body), forbidden) {
			t.Fatalf("host-only checkpoint policy rendered into SANDBOX_CONFIG:\n%s", body)
		}
	}
}
