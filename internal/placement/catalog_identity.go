package placement

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// CatalogIdentityDigest binds the stable node attributes used by PlaceN. A
// Holder recomputes the same digest from its current authenticated registration
// before a persisted candidate can pass Probe.
func CatalogIdentityDigest(node CatalogNode) string {
	value := struct {
		NodeID               string            `json:"node_id"`
		Labels               map[string]string `json:"labels,omitempty"`
		FailureDomain        string            `json:"failure_domain,omitempty"`
		RuntimeDigest        string            `json:"runtime_digest,omitempty"`
		Capabilities         map[string]bool   `json:"capabilities,omitempty"`
		SandboxSlotCapacity  uint64            `json:"sandbox_slot_capacity"`
		BuildSlotCapacity    uint64            `json:"build_slot_capacity"`
		BuildCPUCapacity     uint64            `json:"build_cpu_capacity"`
		BuildMemoryCapacity  uint64            `json:"build_memory_capacity"`
		BuildStorageCapacity uint64            `json:"build_storage_capacity"`
	}{
		NodeID: node.NodeID, Labels: normalizeStringMap(node.Labels), FailureDomain: node.FailureDomain,
		RuntimeDigest: node.RuntimeDigest, Capabilities: normalizeBoolMap(node.Capabilities),
		SandboxSlotCapacity: node.SandboxSlotCapacity, BuildSlotCapacity: node.BuildSlotCapacity,
		BuildCPUCapacity: node.BuildCPUCapacity, BuildMemoryCapacity: node.BuildMemoryCapacity,
		BuildStorageCapacity: node.BuildStorageCapacity,
	}
	raw, _ := json.Marshal(value)
	digest := sha256.Sum256(append([]byte("kuasar-placement-catalog-v1\x00"), raw...))
	return hex.EncodeToString(digest[:])
}

func normalizeStringMap(value map[string]string) map[string]string {
	if len(value) == 0 {
		return nil
	}
	return value
}

func normalizeBoolMap(value map[string]bool) map[string]bool {
	if len(value) == 0 {
		return nil
	}
	return value
}
