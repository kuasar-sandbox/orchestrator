package sandboxcfg

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseIdentity(t *testing.T) {
	for _, test := range []struct {
		raw  string
		want Identity
	}{
		{`{}`, Identity{}},
		{`{"id":"","stable_id":""}`, Identity{}},
		{`{"id":"worker-42"}`, Identity{ID: "worker-42"}},
		{`{"stable_id":"worker-42"}`, Identity{StableID: "worker-42"}},
		{` {"id":"instance-3","stable_id":"worker-42"} `, Identity{ID: "instance-3", StableID: "worker-42"}},
		{`{"id":"` + strings.Repeat("a", 57) + `","stable_id":"` + strings.Repeat("b", 57) + `"}`, Identity{ID: strings.Repeat("a", 57), StableID: strings.Repeat("b", 57)}},
	} {
		t.Run(test.raw, func(t *testing.T) {
			got, err := ParseIdentity(test.raw)
			if err != nil || got != test.want {
				t.Fatalf("ParseIdentity = %+v, %v; want %+v", got, err, test.want)
			}
		})
	}
	for _, raw := range []string{
		"", " ", "null", "[]", `"x"`, `{} {}`, `{"id":null}`, `{"stable_id":null}`,
		`{"id":1}`, `{"stable_id":false}`, `{"id":{}}`, `{"id":"a","id":"b"}`,
		`{"stable_id":"a","stable_id":"b"}`, `{"unknown":"a"}`, `{"ID":"a"}`,
		`{"sandbox_id":"a"}`, `{"id":"A"}`, `{"id":"a/b"}`, `{"id":".."}`,
		`{"id":"-a"}`, `{"id":"a-"}`, `{"stable_id":"a b"}`, `{"id":"a\u0000b"}`,
		`{"id":"` + strings.Repeat("a", 58) + `"}`, `{"stable_id":"` + strings.Repeat("a", 58) + `"}`,
	} {
		t.Run("invalid:"+raw, func(t *testing.T) {
			if _, err := ParseIdentity(raw); err == nil {
				t.Fatal("invalid identity accepted")
			}
		})
	}
}

func TestExtractIdentityDoesNotMutateInput(t *testing.T) {
	raw := `{"id":"instance","stable_id":"logical"}`
	input := map[string]string{NsIdentity: raw, "keep": "value"}
	got, cleaned, err := ExtractIdentity(input)
	if err != nil || got != (Identity{ID: "instance", StableID: "logical"}) || !reflect.DeepEqual(cleaned, map[string]string{"keep": "value"}) {
		t.Fatalf("ExtractIdentity = %+v, %+v, %v", got, cleaned, err)
	}
	cleaned["keep"] = "changed"
	if input[NsIdentity] != raw || input["keep"] != "value" {
		t.Fatalf("input mutated: %+v", input)
	}
	if got, cleaned, err := ExtractIdentity(nil); err != nil || got != (Identity{}) || cleaned != nil {
		t.Fatalf("absent identity = %+v, %+v, %v", got, cleaned, err)
	}
}

func TestIdentityIsRequestScoped(t *testing.T) {
	defaults := map[string]string{NsIdentity: `{"id":"must-not-inherit"}`, "keep": "default"}
	for _, request := range []map[string]string{nil, {NsIdentity: `{"stable_id":"requested"}`}} {
		got, err := MergeCreateMetadata(defaults, request)
		if err != nil {
			t.Fatal(err)
		}
		value, present := got[NsIdentity]
		want, supplied := request[NsIdentity]
		if present != supplied || value != want || got["keep"] != "default" {
			t.Fatalf("identity inherited/merged: %+v", got)
		}
	}
	if defaults[NsIdentity] != `{"id":"must-not-inherit"}` {
		t.Fatal("defaults mutated")
	}
}
