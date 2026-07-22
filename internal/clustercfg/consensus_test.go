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

func TestConsensusRouterRejectsPartialIngressTLS(t *testing.T) {
	valid := ConsensusRouterConfig{
		Domain: "example.test",
		RegistryLayout: RegistryLayoutArtifacts{
			Chain: "/etc/kuasar/registry-layout.json", Keys: "/etc/kuasar/keys.json", Guard: "/var/lib/kuasar/layout.guard",
		},
		RegistryTLS: TLS{Cert: "registry.crt", Key: "registry.key", CA: "ca.crt"},
		NodeTLS:     TLS{Cert: "node.crt", Key: "node.key", CA: "ca.crt"},
		Providers: EndpointSet{
			Endpoints: []NamedEndpoint{{Name: "provider-a", Endpoint: "https://provider-a:9443"}},
			TLS:       TLS{Cert: "provider.crt", Key: "provider.key", CA: "ca.crt"},
		},
		Ingress:                 IngressConfig{Listen: ":443"},
		Auth:                    RouterAuth{APIKey: "enforce", DataPlane: "enforce", CacheTTL: "60s"},
		Cache:                   RouterCache{RouteTTL: "5m", IdleTimeout: "2m"},
		RegistryResponseTimeout: "35s",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("plaintext test config = %v", err)
	}
	withTLS := valid
	withTLS.Ingress.TLS = TLS{Cert: "ingress.crt", Key: "ingress.key"}
	if err := withTLS.Validate(); err != nil {
		t.Fatalf("complete ingress TLS = %v", err)
	}
	for name, material := range map[string]TLS{
		"cert only": {Cert: "ingress.crt"},
		"key only":  {Key: "ingress.key"},
		"CA only":   {CA: "ca.crt"},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			candidate.Ingress.TLS = material
			if err := candidate.Validate(); err == nil {
				t.Fatal("partial ingress TLS was accepted")
			}
		})
	}
}
