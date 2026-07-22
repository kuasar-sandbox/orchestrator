package placement

import "testing"

func TestCatalogIdentityDigestIsCanonicalAndCoversStaticEligibility(t *testing.T) {
	first := CatalogNode{
		NodeID: "node-1", Labels: map[string]string{"pool": "a", "zone": "1"},
		Capabilities: map[string]bool{"kvm": true, "gpu": false}, FailureDomain: "z1",
		RuntimeDigest: "runtime-v1", SandboxSlotCapacity: 8, BuildSlotCapacity: 4,
		BuildCPUCapacity: 4000, BuildMemoryCapacity: 8 << 30, BuildStorageCapacity: 100 << 30,
	}
	second := first
	second.Labels = map[string]string{"zone": "1", "pool": "a"}
	second.Capabilities = map[string]bool{"gpu": false, "kvm": true}
	if CatalogIdentityDigest(first) != CatalogIdentityDigest(second) {
		t.Fatal("Catalog digest depended on map iteration order")
	}
	second.Capabilities["kvm"] = false
	if CatalogIdentityDigest(first) == CatalogIdentityDigest(second) {
		t.Fatal("Catalog digest ignored a static capability change")
	}
}
