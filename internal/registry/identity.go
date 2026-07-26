package registry

import (
	"strconv"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// EncodeNodeSandboxID returns the opaque node-local ID for one Registry-owned
// sandbox instance. No component may parse this value to recover either input;
// the persisted ownership record is authoritative.
func EncodeNodeSandboxID(sandboxID string, generation uint64) string {
	return sandboxID + "-g" + strconv.FormatUint(generation, 10)
}

func validNodeSandboxIdentity(sandboxID, nodeSandboxID string, generation uint64) bool {
	return types.ValidLocalSandboxID(sandboxID) &&
		types.ValidLocalSandboxID(nodeSandboxID) &&
		nodeSandboxID == EncodeNodeSandboxID(sandboxID, generation)
}
