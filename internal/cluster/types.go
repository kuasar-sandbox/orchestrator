package cluster

import "github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"

// Shared wire records use the storage-neutral domain schema.
type NodeState = metadata.NodeState

const (
	NodeUnknown = metadata.NodeUnknown
	NodeLive    = metadata.NodeLive
	NodeDrained = metadata.NodeDrained
	NodeDead    = metadata.NodeDead
)

type NodeKeyPair = metadata.NodeKeyPair
type NodeSandboxRef = metadata.NodeSandboxRef
type NodeBuildRef = metadata.NodeBuildRef
type NodeListEntry = metadata.NodeListEntry
type PlacerImportSourceState = metadata.PlacerImportSourceState

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
