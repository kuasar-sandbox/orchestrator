package sandboxcfg

import (
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func checkpointInt(value int) *int { return &value }

func TestParseCreateCheckpointPolicyJSON(t *testing.T) {
	for _, tc := range []struct {
		name  string
		raw   string
		want  CreateCheckpointPolicy
		quota int
	}{
		{name: "omitted", raw: `{}`, quota: 1},
		{name: "null inherits", raw: `{"max_checkpoints":null,"merge_ref":null}`, quota: 1},
		{name: "quota", raw: `{"max_checkpoints":4}`, want: CreateCheckpointPolicy{MaxCheckpoints: checkpointInt(4)}, quota: 4},
		{name: "combined", raw: `{"max_checkpoints":2,"merge_ref":false,"drop_caches":true}`,
			want: CreateCheckpointPolicy{SnapshotPolicy: SnapshotPolicy{MergeRef: checkpointBool(false), DropCaches: checkpointBool(true)}, MaxCheckpoints: checkpointInt(2)}, quota: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseCreateCheckpointPolicyJSON(tc.raw)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("policy = %+v, want %+v", got, tc.want)
			}
			quota, err := types.ResolveMaxCheckpoints(got.MaxCheckpoints)
			if err != nil || quota != tc.quota {
				t.Fatalf("quota = %d, %v; want %d", quota, err, tc.quota)
			}
			canonical, err := MarshalCreateCheckpointPolicyJSON(got)
			if err != nil {
				t.Fatal(err)
			}
			roundTrip, err := ParseCreateCheckpointPolicyJSON(canonical)
			if err != nil || !reflect.DeepEqual(roundTrip, got) {
				t.Fatalf("canonical %s did not round trip: %+v, %v", canonical, roundTrip, err)
			}
		})
	}
}

func TestParseCreateCheckpointPolicyJSONRejectsInvalidInput(t *testing.T) {
	invalid := map[string]string{
		"empty":                    "",
		"null":                     `null`,
		"array":                    `[]`,
		"zero":                     `{"max_checkpoints":0}`,
		"negative":                 `{"max_checkpoints":-1}`,
		"fraction":                 `{"max_checkpoints":1.5}`,
		"overflow":                 `{"max_checkpoints":9223372036854775808}`,
		"string":                   `{"max_checkpoints":"3"}`,
		"bool":                     `{"max_checkpoints":true}`,
		"quota array":              `{"max_checkpoints":[]}`,
		"quota object":             `{"max_checkpoints":{}}`,
		"unknown":                  `{"max_checkpoints":3,"extra":false}`,
		"wrong field case":         `{"Max_Checkpoints":3}`,
		"duplicate quota":          `{"max_checkpoints":1,"max_checkpoints":3}`,
		"duplicate snapshot field": `{"max_checkpoints":3,"merge_ref":true,"merge_ref":false}`,
		"wrong snapshot type":      `{"max_checkpoints":3,"drop_caches":"false"}`,
		"trailing JSON":            `{"max_checkpoints":3} {}`,
	}
	for name, raw := range invalid {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseCreateCheckpointPolicyJSON(raw); err == nil {
				t.Fatalf("accepted %s", raw)
			}
		})
	}
}

func TestCreateCheckpointOverlayPreservesUnsetFields(t *testing.T) {
	base, err := ParseCreateCheckpointPolicyJSON(`{"max_checkpoints":4,"merge_ref":true,"drop_caches":true}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		header string
		quota  int
		merge  bool
		drop   bool
	}{
		{header: `{"max_checkpoints":null,"merge_ref":false}`, quota: 4, merge: false, drop: true},
		{header: `{"max_checkpoints":2,"drop_caches":false}`, quota: 2, merge: true, drop: false},
		{header: `{}`, quota: 4, merge: true, drop: true},
	} {
		higher, err := ParseCreateCheckpointPolicyJSON(tc.header)
		if err != nil {
			t.Fatal(err)
		}
		got := OverlayCreateCheckpointPolicy(base, higher)
		if got.Empty() || got.MaxCheckpoints == nil || *got.MaxCheckpoints != tc.quota ||
			got.MergeRef == nil || *got.MergeRef != tc.merge || got.DropCaches == nil || *got.DropCaches != tc.drop {
			t.Fatalf("header %s produced %+v", tc.header, got)
		}
		*got.MaxCheckpoints = 99
		*got.MergeRef = false
		*got.DropCaches = false
		if *base.MaxCheckpoints != 4 || !*base.MergeRef || !*base.DropCaches {
			t.Fatal("overlay mutated its base")
		}
		if higher.MaxCheckpoints != nil && *higher.MaxCheckpoints != tc.quota {
			t.Fatal("overlay shares the header quota pointer")
		}
	}
	if !(CreateCheckpointPolicy{}).Empty() {
		t.Fatal("empty Create policy is not empty")
	}
}

func TestExtractCreateCheckpointPolicyKeepsOnlySnapshotDefaults(t *testing.T) {
	raw := `{"max_checkpoints":4,"merge_ref":false,"drop_caches":null}`
	input := map[string]string{NsCheckpoint: raw, "keep": "value"}
	policy, cleaned, err := ExtractCreateCheckpointPolicy(input)
	if err != nil {
		t.Fatal(err)
	}
	if policy.MaxCheckpoints == nil || *policy.MaxCheckpoints != 4 || cleaned[NsCheckpoint] != `{"merge_ref":false}` || cleaned["keep"] != "value" {
		t.Fatalf("extracted policy=%+v metadata=%+v", policy, cleaned)
	}
	if _, err := NormalizeCheckpointMetadata(cleaned); err != nil {
		t.Fatalf("subsequent Pause metadata rejected: %v", err)
	}
	cleaned["keep"] = "changed"
	if input[NsCheckpoint] != raw || input["keep"] != "value" {
		t.Fatal("extraction mutated the caller's metadata")
	}
	for _, quotaOnly := range []string{`{"max_checkpoints":4}`, `{"max_checkpoints":null}`, `{}`} {
		_, cleaned, err := ExtractCreateCheckpointPolicy(map[string]string{NsCheckpoint: quotaOnly, "keep": "value"})
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := cleaned[NsCheckpoint]; exists || cleaned["keep"] != "value" {
			t.Fatalf("quota leaked into Pause defaults: %+v", cleaned)
		}
	}
	policy, cleaned, err = ExtractCreateCheckpointPolicy(nil)
	if err != nil || !policy.Empty() || cleaned != nil {
		t.Fatalf("absent policy = %+v, %+v, %v", policy, cleaned, err)
	}
	if _, _, err := ExtractCreateCheckpointPolicy(map[string]string{NsCheckpoint: `{"max_checkpoints":0}`}); err == nil || !strings.Contains(err.Error(), NsCheckpoint) {
		t.Fatalf("invalid metadata quota error = %v", err)
	}
}

func TestSnapshotPolicyDoesNotAcceptCreateQuota(t *testing.T) {
	for _, raw := range []string{`{"max_checkpoints":4}`, `{"max_checkpoints":null}`, `{"merge_ref":true,"max_checkpoints":1}`} {
		if _, err := ParseSnapshotPolicyJSON(raw); err == nil {
			t.Fatalf("action/Build parser accepted Create quota: %s", raw)
		}
		if _, err := NormalizeCheckpointMetadata(map[string]string{NsCheckpoint: raw}); err == nil {
			t.Fatalf("action/Build metadata accepted Create quota: %s", raw)
		}
	}
}
