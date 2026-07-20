package api

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRewriteSandboxCreateEnvelopePreservesExtensions(t *testing.T) {
	original, err := clusterstate.NewNodeRequestEnvelopeV1(
		http.MethodPost, "/sandboxes", "future=enabled&future=again",
		http.Header{"Content-Type": {"application/json"}, "X-Future": {"kept"}},
		[]byte(`{"templateID":"caller","timeout":1,"metadata":{"caller":"old"},"future":{"mode":"fast"}}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	rewritten, err := RewriteSandboxCreateEnvelope(original, "e2b-img-"+strings.Repeat("a", 64), 30, map[string]string{"caller": "forced"})
	if err != nil {
		t.Fatal(err)
	}
	if rewritten.RawQuery != original.RawQuery || !reflect.DeepEqual(rewritten.Header, original.Header) {
		t.Fatalf("request extensions changed: before=%+v after=%+v", original, rewritten)
	}
	fields, err := clusterstate.DecodeJSONObject(rewritten.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(fields["future"]) != `{"mode":"fast"}` || string(fields["timeout"]) != "30" ||
		string(fields["metadata"]) != `{"caller":"forced"}` {
		t.Fatalf("rewritten body = %s", rewritten.Body)
	}
}

func TestRewriteBuildRegisterEnvelopeNormalizesOnlyOwnedFields(t *testing.T) {
	original, err := clusterstate.NewNodeRequestEnvelopeV1(
		http.MethodPost, "/v3/templates", "extension=true", nil,
		[]byte(`{"cpu_count":1,"memory_mb":128,"future":[1,2,3]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	rewritten, err := RewriteBuildRegisterEnvelope(original, RegisterSpec{
		Name: "name", Tags: []string{"tag"}, Profile: types.ProfileE2B,
		CPUCount: 2, MemoryMB: 1024, Metadata: map[string]string{"kept": "yes"},
	})
	if err != nil {
		t.Fatal(err)
	}
	fields, err := clusterstate.DecodeJSONObject(rewritten.Body)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := fields["cpu_count"]; found {
		t.Fatal("legacy CPU alias remained in final envelope")
	}
	if string(fields["cpuCount"]) != "2" || string(fields["memoryMB"]) != "1024" ||
		string(fields["future"]) != `[1,2,3]` {
		t.Fatalf("rewritten body = %s", rewritten.Body)
	}
}

func TestRewriteEnvelopeRejectsCaseAliasesForOwnedFields(t *testing.T) {
	for _, body := range []string{
		`{"TemplateId":"forged"}`,
		`{"Metadata":{"caller":"forged"}}`,
	} {
		envelope, err := clusterstate.NewNodeRequestEnvelopeV1(http.MethodPost, "/sandboxes", "", nil, []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := RewriteSandboxCreateEnvelope(envelope, "e2b-img-"+strings.Repeat("b", 64), 1, nil); err == nil {
			t.Fatalf("case alias accepted: %s", body)
		}
	}
}
