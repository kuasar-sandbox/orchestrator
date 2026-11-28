package types

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestExportReportPresenceAndLegacyNormalization(t *testing.T) {
	root := "manifest://" + strings.Repeat("a", 64)
	for _, test := range []struct {
		name, report      string
		complete, invalid bool
	}{
		{"complete", `"refs":true,"sandboxRef":"` + root + `","sandboxRefs":["` + root + `"],"removedRefs":[]`, true, false},
		{"unknown", `"refs":false,"sandboxRef":"` + root + `"`, false, false},
		{"legacy", `"sandboxRef":"` + root + `","removedRefs":["file://old.sandbox"]`, false, false},
		{"unknown empty delta", `"refs":false,"sandboxRef":"` + root + `","removedRefs":[]`, false, true},
		{"unknown null delta", `"refs":false,"sandboxRef":"` + root + `","removedRefs":null`, false, true},
		{"unknown graph", `"refs":false,"sandboxRef":"` + root + `","sandboxRefs":[]`, false, true},
		{"missing refs with graph", `"sandboxRef":"` + root + `","sandboxRefs":["` + root + `"],"removedRefs":[]`, false, true},
		{"missing delta", `"refs":true,"sandboxRef":"` + root + `","sandboxRefs":["` + root + `"]`, false, true},
		{"null delta", `"refs":true,"sandboxRef":"` + root + `","sandboxRefs":["` + root + `"],"removedRefs":null`, false, true},
		{"duplicate refs", `"refs":false,"refs":false,"sandboxRef":"` + root + `"`, false, true},
		{"unknown field", `"refs":false,"sandboxRef":"` + root + `","diskRef":"` + root + `"`, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var out ExportResult
			err := json.Unmarshal([]byte(`{"result":"kmt1.test",`+test.report+`}`), &out)
			if (err != nil) != test.invalid {
				t.Fatalf("decode: %v", err)
			}
			if test.invalid {
				return
			}
			if out.Result != "kmt1.test" || out.Refs != test.complete {
				t.Fatalf("result: %+v", out)
			}
			raw, err := json.Marshal(out)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			if string(fields["result"]) != `"kmt1.test"` {
				t.Fatalf("lost platform result: %s", raw)
			}
			if !test.complete {
				if out.RemovedRefs != nil || fields["removedRefs"] != nil || fields["sandboxRefs"] != nil {
					t.Fatalf("unknown report kept collections: %s", raw)
				}
			} else if string(fields["removedRefs"]) != "[]" {
				t.Fatalf("empty known delta: %s", raw)
			}
		})
	}
}

func TestWorkspaceExportResultContract(t *testing.T) {
	root := "manifest://" + strings.Repeat("b", 64)
	valid := `{"result":"kwt1.test","refs":true,"diskRefs":["` + root + `"],"removedRefs":[]}`
	for _, raw := range []string{
		valid,
		`{"result":"kwt1.test","refs":false}`,
	} {
		var out WorkspaceExportResult
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(out)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != raw {
			t.Fatalf("workspace shape: %s", encoded)
		}
	}
	for _, raw := range []string{
		strings.Replace(valid, `"result"`, `"token"`, 1),
		strings.Replace(valid, `"kwt1.test"`, `"kmt1.test"`, 1),
		strings.Replace(valid, `"refs":true`, `"refs":false`, 1),
		strings.Replace(valid, `"refs":true,`, ``, 1),
		strings.Replace(valid, `"diskRefs":`, `"diskRef":"`+root+`","diskRefs":`, 1),
		strings.Replace(valid, `"result":`, `"result":"kwt1.other","result":`, 1),
		strings.Replace(valid, `"removedRefs":[]`, `"removedRefs":null`, 1),
		valid + `{}`,
	} {
		var out WorkspaceExportResult
		if err := json.Unmarshal([]byte(raw), &out); err == nil {
			t.Fatalf("invalid workspace result accepted: %s", raw)
		}
	}
}
