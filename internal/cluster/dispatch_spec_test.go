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
	templateRef := "e2b-img-" + strings.Repeat("c", 64)
	spec := SandboxDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateRef: templateRef,
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		AccessToken: "capability", Config: map[string]string{"tenant": "value"},
		Request: testNodeRequest(t, "/sandboxes", `{"templateID":"`+templateRef+`","timeout":0,"envVars":{"FUTURE":"kept"},"metadata":{"tenant":"value"}}`),
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
	templateRef := "e2b-img-" + strings.Repeat("c", 64)
	spec := SandboxDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateRef: templateRef,
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		AccessToken: "capability",
		Request:     testNodeRequest(t, "/sandboxes", `{"templateID":"`+templateRef+`","timeout":0,"metadata":{"kuasar-sandbox.cluster":"forged"}}`),
	}
	if _, err := MarshalSandboxDispatchSpec(spec); err == nil {
		t.Fatal("system-owned request metadata accepted")
	}
	spec.Request = testNodeRequest(t, "/sandboxes", `{"templateID":"`+templateRef+`","timeout":0,"Metadata":{"tenant":"value"}}`)
	if _, err := MarshalSandboxDispatchSpec(spec); err == nil {
		t.Fatal("non-canonical metadata field accepted")
	}
}

func TestSandboxDispatchSpecBindsReplayedTemplate(t *testing.T) {
	templateRef := "e2b-img-" + strings.Repeat("c", 64)
	base := SandboxDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateRef: templateRef,
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		AccessToken: "capability",
	}
	for name, body := range map[string]string{
		"missing":    `{}`,
		"mismatch":   `{"templateID":"another-template"}`,
		"case alias": `{"TemplateID":"` + templateRef + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			spec := base
			spec.Request = testNodeRequest(t, "/sandboxes", body)
			if _, err := MarshalSandboxDispatchSpec(spec); err == nil {
				t.Fatal("unbound replay template was accepted")
			}
		})
	}
	valid := base
	valid.Request = testNodeRequest(t, "/sandboxes", `{"templateID":"`+templateRef+`","timeout":0,"metadata":null,"future":true}`)
	if _, err := MarshalSandboxDispatchSpec(valid); err != nil {
		t.Fatalf("bound replay template: %v", err)
	}
}

func TestSandboxDispatchSpecBindsReplayedTimeoutAndMetadata(t *testing.T) {
	templateRef := "e2b-img-" + strings.Repeat("c", 64)
	base := SandboxDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateRef: templateRef,
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		AccessToken: "capability", TimeoutSeconds: 30, Config: map[string]string{"tenant": "value"},
		Request: testNodeRequest(t, "/sandboxes", `{"metadata":{"tenant":"value"},"templateID":"`+templateRef+`","timeout":30}`),
	}
	if _, err := MarshalSandboxDispatchSpec(base); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*SandboxDispatchSpecV1){
		"timeout":  func(spec *SandboxDispatchSpecV1) { spec.TimeoutSeconds++ },
		"metadata": func(spec *SandboxDispatchSpecV1) { spec.Config["tenant"] = "changed" },
	} {
		t.Run(name, func(t *testing.T) {
			spec := base
			spec.Config = map[string]string{"tenant": "value"}
			mutate(&spec)
			if _, err := MarshalSandboxDispatchSpec(spec); err == nil {
				t.Fatal("mismatched replay field was accepted")
			}
		})
	}
}

func TestBuildDispatchSpecRequiresSeparateKeysAndResourceCeiling(t *testing.T) {
	spec := BuildDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateID: "transient-template",
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		Profile: types.ProfileE2B, CPUCount: 2, MemoryMB: 1024,
		Request: testNodeRequest(t, "/v3/templates", `{"name":"","tags":null,"profile":"e2b","metadata":null,"future":{"mode":"fast"},"cpuCount":2,"memoryMB":1024}`),
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
	spec.ManifestKeyFingerprint = strings.Repeat("B", 24)
	if _, err := MarshalBuildDispatchSpec(spec); err == nil {
		t.Fatal("non-canonical key fingerprint accepted")
	}
}

func TestBuildDispatchSpecBindsReplayedResourceCeilings(t *testing.T) {
	base := BuildDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateID: "transient-template",
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		Profile: types.ProfileE2B, CPUCount: 2, MemoryMB: 1024,
	}
	for name, body := range map[string]string{
		"larger CPU":          `{"cpuCount":8,"memoryMB":1024}`,
		"missing memory":      `{"cpuCount":2}`,
		"conflicting aliases": `{"cpuCount":2,"cpu_count":4,"memoryMB":1024}`,
		"case alias":          `{"CPUCount":2,"memoryMB":1024}`,
	} {
		t.Run(name, func(t *testing.T) {
			spec := base
			spec.Request = testNodeRequest(t, "/v3/templates", body)
			if _, err := MarshalBuildDispatchSpec(spec); err == nil {
				t.Fatal("unbound replay resource request was accepted")
			}
		})
	}
	valid := base
	valid.Request = testNodeRequest(t, "/v3/templates", `{"name":"","tags":null,"profile":"e2b","metadata":null,"cpu_count":2,"memory_mb":1024,"future":true}`)
	if _, err := MarshalBuildDispatchSpec(valid); err != nil {
		t.Fatalf("canonical snake-case ceilings: %v", err)
	}
}

func TestBuildDispatchSpecBindsAllReplayedRegistrationFields(t *testing.T) {
	base := BuildDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateID: "transient-template",
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		Profile: types.ProfileBare, CPUCount: 2, MemoryMB: 1024,
		Names: []string{"primary"}, Aliases: []string{"alias"}, Metadata: map[string]string{"tenant": "value"},
		Request: testNodeRequest(t, "/v3/templates", `{"cpuCount":2,"memoryMB":1024,"metadata":{"tenant":"value"},"name":"primary","profile":"bare","tags":["alias"]}`),
	}
	if _, err := MarshalBuildDispatchSpec(base); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*BuildDispatchSpecV1){
		"profile":  func(spec *BuildDispatchSpecV1) { spec.Profile = types.ProfileE2B },
		"name":     func(spec *BuildDispatchSpecV1) { spec.Names = []string{"changed"} },
		"tags":     func(spec *BuildDispatchSpecV1) { spec.Aliases = []string{"changed"} },
		"metadata": func(spec *BuildDispatchSpecV1) { spec.Metadata = map[string]string{"tenant": "changed"} },
	} {
		t.Run(name, func(t *testing.T) {
			spec := base
			mutate(&spec)
			if _, err := MarshalBuildDispatchSpec(spec); err == nil {
				t.Fatal("mismatched replay field was accepted")
			}
		})
	}
}
