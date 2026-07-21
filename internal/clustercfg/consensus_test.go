package clustercfg

import "testing"

func TestCertificateAuthenticatedRolesRequireTCPListeners(t *testing.T) {
	material := TLS{Cert: "cert", Key: "key", CA: "ca"}
	registry := ConsensusRegistryConfig{Member: MemberConfig{
		ID: "registry-a", Listen: "/run/registry.sock", TLS: material,
	}}
	if err := registry.Validate(); err == nil {
		t.Fatal("Registry accepted a Unix listener that cannot provide peer certificates")
	}
	placer := FinalPlacerConfig{Placer: FinalPlacerProcessConfig{
		ID: "placer-a", Listen: "/run/placer.sock", TLS: material,
	}}
	if err := placer.Validate(); err == nil {
		t.Fatal("Placer accepted a Unix listener that cannot provide peer certificates")
	}
}
