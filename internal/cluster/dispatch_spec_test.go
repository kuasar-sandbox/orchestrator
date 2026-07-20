package cluster

import (
	"net/http"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func testNodeRequest(t *testing.T, path, body string) NodeRequestEnvelopeV1 {
	t.Helper()
	request, err := NewNodeRequestEnvelopeV1(http.MethodPost, path, "extension=true", http.Header{
		"Content-Type": {"application/json"},
		"X-Extension":  {"preserved"},
	}, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func TestSandboxDispatchSpecRoundTripAndRejectsSystemMetadata(t *testing.T) {
	spec := SandboxDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateRef: "e2b-img-" + strings.Repeat("c", 64),
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		AccessToken: "capability", Config: map[string]string{"tenant": "value"},
		Request: testNodeRequest(t, "/sandboxes", `{"envVars":{"FUTURE":"kept"},"metadata":{"tenant":"value"}}`),
	}
	encoded, err := MarshalSandboxDispatchSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseSandboxDispatchSpec(encoded)
	if err != nil || got.TemplateRef != spec.TemplateRef || got.Config["tenant"] != "value" || got.Request.RawQuery == "" {
		t.Fatalf("round trip = %+v, %v", got, err)
	}
	spec.Config[ObjectMetadataKey] = "forged"
	if _, err := MarshalSandboxDispatchSpec(spec); err == nil {
		t.Fatal("system-owned metadata accepted in dispatch spec")
	}
}

func TestDispatchSpecRejectsReservedRequestMetadata(t *testing.T) {
	spec := SandboxDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateRef: "e2b-img-" + strings.Repeat("c", 64),
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		AccessToken: "capability",
		Request:     testNodeRequest(t, "/sandboxes", `{"metadata":{"kuasar-sandbox.cluster":"forged"}}`),
	}
	if _, err := MarshalSandboxDispatchSpec(spec); err == nil {
		t.Fatal("system-owned request metadata accepted")
	}
}

func TestBuildDispatchSpecRequiresSeparateKeysAndResourceCeiling(t *testing.T) {
	spec := BuildDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateID: "transient-template",
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		Profile: types.ProfileE2B, CPUCount: 2, MemoryMB: 1024,
		Request: testNodeRequest(t, "/v3/templates", `{"future":{"mode":"fast"},"cpuCount":2,"memoryMB":1024}`),
	}
	encoded, err := MarshalBuildDispatchSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseBuildDispatchSpec(append(encoded, []byte(" trailing")...)); err == nil {
		t.Fatal("trailing dispatch data accepted")
	}
	spec.MemoryMB = 0
	if _, err := MarshalBuildDispatchSpec(spec); err == nil {
		t.Fatal("missing Build memory ceiling accepted")
	}
	spec.MemoryMB = 1024
	spec.ManifestKeyFingerprint = spec.AuthKeyFingerprint
	if _, err := MarshalBuildDispatchSpec(spec); err == nil {
		t.Fatal("shared AuthKey/ManifestKey material accepted")
	}
}
