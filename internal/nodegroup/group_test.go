package nodegroup

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestPutContract(t *testing.T) {
	legacy := "bare-snp-" + base64.RawURLEncoding.EncodeToString([]byte("manifest://"+strings.Repeat("a", 64)))
	annotated := "bare-snp-" + base64.RawURLEncoding.EncodeToString([]byte("manifest://"+strings.Repeat("a", 64)+"@runtime:runtime.bundle:digest:"+strings.Repeat("b", 64)))
	for _, raw := range []string{
		`{"expected_version":0,"context":{}}`,
		`{"expected_version":2,"context":{"templateID":null,"target_port":49983}}`,
		`{"expected_version":0,"context":{"templateID":"` + legacy + `"}}`,
		`{"expected_version":0,"context":{"templateID":"` + annotated + `"}}`,
		`{"expected_version":0,"context":{"image_repo":"repo","registry_auth":"secret"}}`,
	} {
		var p PutRequest
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			t.Fatalf("valid input rejected: %s: %v", raw, err)
		}
		if _, _, err := Normalize(p.Context); err != nil {
			t.Fatalf("valid input rejected: %s: %v", raw, err)
		}
	}
	for _, raw := range []string{
		`{}`, `{"context":{}}`, `{"expected_version":0}`, `{"expected_version":null,"context":{}}`,
		`{"expected_version":-1,"context":{}}`, `{"expected_version":0,"context":null}`,
		`{"expected_version":0,"context":[]}`, `{"expected_version":0,"definition":{}}`,
		`{"expected_version":0,"context":{},"key_put":{}}`,
		`{"expected_version":0,"context":{"templateID":""}}`,
		`{"expected_version":0,"context":{"sandbox_config":{"x":"y"}}}`,
		`{"expected_version":0,"context":{"target_port":65536}}`,
		`{"expected_version":0,"context":{"target_port":-1}}`,
		`{"expected_version":0,"context":{"node_selectors":[]}}`,
		`{"expected_version":0,"context":{"boot_runtime":"something"}}`,
		`{"expected_version":0,"context":{"registry_auth":{"type":"ref","value":"private"}}}`,
		`{"expected_version":0,"context":{"templateID":"` + legacy + `","sandbox_config":{"kuasar-sandbox.placement":"{}"}}}`,
	} {
		var p PutRequest
		err := json.Unmarshal([]byte(raw), &p)
		if err == nil {
			_, _, err = Normalize(p.Context)
		}
		if err == nil {
			t.Fatalf("invalid input accepted: %s", raw)
		}
	}
}

func TestContextCanonicalAndPrivateProjection(t *testing.T) {
	var empty Context
	if err := json.Unmarshal([]byte(`{"templateID":null,"sandbox_config":{},"registry_auth":null,"metadata":{}}`), &empty); err != nil {
		t.Fatal(err)
	}
	_, raw, err := Normalize(empty)
	if err != nil || string(raw) != `{}` {
		t.Fatalf("empty context = %s, %v", raw, err)
	}
	if err := json.Unmarshal([]byte(`{"image_repo":"private-repo","registry_auth":"password","metadata":{"key":"private-value"}}`), &empty); err != nil {
		t.Fatal(err)
	}
	projection, _ := json.Marshal(empty.Support("/cell/g", 5, "owner", "api"))
	for _, private := range []string{"private-repo", "password", "private-value", "default_context"} {
		if strings.Contains(string(projection), private) {
			t.Fatalf("private/defaultless field leaked: %s", projection)
		}
	}
	if _, _, err := Normalize(Context{Metadata: map[string]string{"huge": strings.Repeat("x", MaxContextBytes)}}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized context: %v", err)
	}
}
