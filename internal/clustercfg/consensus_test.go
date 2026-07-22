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

func TestEndpointSetRequiresCanonicalBaseURLs(t *testing.T) {
	valid := EndpointSet{
		Endpoints: []NamedEndpoint{{Name: "provider-a", Endpoint: "https://provider-a:9443"}},
		TLS:       TLS{Cert: "cert", Key: "key", CA: "ca"},
	}
	if err := valid.Validate("providers"); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{
		"https://provider-a:9443/", "https://provider-a:9443/base",
		"https://provider-a:9443?tenant=a", "https://provider-a:9443#fragment",
	} {
		candidate := valid
		candidate.Endpoints = []NamedEndpoint{{Name: "provider-a", Endpoint: endpoint}}
		if err := candidate.Validate("providers"); err == nil {
			t.Fatalf("noncanonical endpoint %q was accepted", endpoint)
		}
	}
}
