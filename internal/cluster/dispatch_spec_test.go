package cluster

import (
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSandboxDispatchSpecRoundTripAndRejectsSystemMetadata(t *testing.T) {
	spec := SandboxDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateRef: "e2b-img-" + strings.Repeat("c", 64),
		KeyFingerprint: strings.Repeat("a", 24), AccessToken: "capability",
		Config: map[string]string{"tenant": "value"},
	}
	encoded, err := MarshalSandboxDispatchSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseSandboxDispatchSpec(encoded)
	if err != nil || got.TemplateRef != spec.TemplateRef || got.Config["tenant"] != "value" {
		t.Fatalf("round trip = %+v, %v", got, err)
	}
	spec.Config[ObjectMetadataKey] = "forged"
	if _, err := MarshalSandboxDispatchSpec(spec); err == nil {
		t.Fatal("system-owned metadata accepted in dispatch spec")
	}
}

func TestBuildDispatchSpecRequiresEncryptedCapability(t *testing.T) {
	spec := BuildDispatchSpecV1{
		Version: DispatchSpecVersionV1, TemplateID: "transient-template",
		KeyFingerprint: strings.Repeat("b", 24), Profile: types.ProfileE2B,
		FromImage: "registry.example/image:tag", PullCapability: "plaintext",
	}
	if _, err := MarshalBuildDispatchSpec(spec); err == nil {
		t.Fatal("plaintext Build credentials accepted")
	}
	spec.PullCapability = "kpt_encrypted"
	encoded, err := MarshalBuildDispatchSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseBuildDispatchSpec(append(encoded, []byte(" trailing")...)); err == nil {
		t.Fatal("trailing dispatch data accepted")
	}
}
