package cluster

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestNodeRequestEnvelopePreservesOrdinaryRequestData(t *testing.T) {
	header := http.Header{
		"Content-Type":             {"application/json; charset=utf-8"},
		"X-Extension":              {"one", "two"},
		"X-Kuasar-Sandbox-Network": {`{"hostname":"worker"}`},
		"Authorization":            {"Bearer caller-secret"},
		"X-API-KEY":                {"caller-secret"},
		"X-Kuasar-Migration-Token": {"caller-secret"},
		"X-Kuasar-Node-Epoch":      {"forged"},
		"Connection":               {"X-Hop"},
		"X-Hop":                    {"drop"},
	}
	envelope, err := NewNodeRequestEnvelopeV1(http.MethodPost, "/v3/templates", "z=2&a=1&a=3", header,
		[]byte(`{"future":{"enabled":true},"name":"build"}`))
	if err != nil {
		t.Fatal(err)
	}
	if envelope.RawQuery != "a=1&a=3&z=2" {
		t.Fatalf("canonical query = %q", envelope.RawQuery)
	}
	if !reflect.DeepEqual(envelope.Header["X-Extension"], []string{"one", "two"}) ||
		envelope.Header["X-Kuasar-Sandbox-Network"] == nil {
		t.Fatalf("ordinary headers were not preserved: %#v", envelope.Header)
	}
	for _, protected := range []string{"Authorization", "X-Api-Key", "X-Kuasar-Migration-Token", "X-Kuasar-Node-Epoch", "Connection", "X-Hop"} {
		if _, exists := envelope.Header[protected]; exists {
			t.Fatalf("protected header %q was persisted", protected)
		}
	}
	if err := envelope.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalJSONObjectRejectsDuplicateKeysAtAnyDepth(t *testing.T) {
	for _, encoded := range []string{
		`{"templateID":"a","templateID":"b"}`,
		`{"extension":{"mode":1,"mode":2}}`,
	} {
		if _, err := CanonicalJSONObject([]byte(encoded)); err == nil {
			t.Fatalf("duplicate request accepted: %s", encoded)
		}
	}
}

func TestDecodeEncodeJSONObjectPreservesUnknownFields(t *testing.T) {
	fields, err := DecodeJSONObject([]byte(`{"empty":[],"future":{"mode":"fast"},"templateID":"caller"}`))
	if err != nil {
		t.Fatal(err)
	}
	fields["templateID"] = json.RawMessage(`"forced"`)
	encoded, err := EncodeJSONObject(fields)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"future":{"mode":"fast"}`) ||
		!strings.Contains(string(encoded), `"empty":[]`) ||
		!strings.Contains(string(encoded), `"templateID":"forced"`) || strings.Contains(string(encoded), "caller") {
		t.Fatalf("rewritten request = %s", encoded)
	}
}

func TestNodeRequestEnvelopeRejectsNonCanonicalOrOversizedInput(t *testing.T) {
	envelope, err := NewNodeRequestEnvelopeV1(http.MethodPost, "/sandboxes", "", nil, []byte(`{"x":1}`))
	if err != nil {
		t.Fatal(err)
	}
	envelope.Header = map[string][]string{"Authorization": {"secret"}}
	if err := envelope.Validate(); err == nil {
		t.Fatal("persisted caller credential accepted")
	}
	if _, err := CanonicalJSONObject([]byte(`{"data":"` + strings.Repeat("x", MaxNodeRequestBodyBytes) + `"}`)); err == nil {
		t.Fatal("oversized request accepted")
	}
	if _, err := CanonicalJSONObject(append([]byte("{}"), []byte(strings.Repeat(" ", MaxNodeRequestBodyBytes))...)); err == nil {
		t.Fatal("oversized raw request with a small canonical form accepted")
	}
	if _, err := NewNodeRequestEnvelopeV1(http.MethodPost, "/sandboxes", "", http.Header{
		"Bad Header": {"value"},
	}, []byte(`{}`)); err == nil {
		t.Fatal("invalid HTTP header name accepted")
	}
	for name, value := range map[string]string{
		"invalid UTF-8": string([]byte{'x', 0xff}),
		"control":       "before\x01after",
		"delete":        "before\x7fafter",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewNodeRequestEnvelopeV1(http.MethodPost, "/sandboxes", "", http.Header{
				"X-Extension": {value},
			}, []byte(`{}`)); err == nil {
				t.Fatal("invalid HTTP header value accepted")
			}
		})
	}
}
