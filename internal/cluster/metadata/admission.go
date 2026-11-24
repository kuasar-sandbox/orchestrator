package metadata

import (
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// AllowsNode is re-evaluated against the current profile in the intent
// transaction. Heartbeat-only changes do not invalidate a selection version.
func (a GroupAdmission) AllowsNode(node NodeProfileRecord, template string) bool {
	zoneMax := a.ZoneAdmitMax
	if zoneMax == "" {
		zoneMax = "yellow"
	}
	if a.Placement.Validate() != nil || node.Draining || node.State == NodeDead || node.State == NodeDrained || ZoneRank(node.Zone) > ZoneRank(zoneMax) || !a.Placement.Matches(node.Labels) {
		return false
	}
	if a.TargetRuntimeDigest != "" && node.RuntimeDigest != "" && a.TargetRuntimeDigest != node.RuntimeDigest {
		return false
	}
	if template != "" {
		id, err := types.ParseTemplateID(template)
		if err != nil || !placement.RuntimeMatches(id, node.Labels) {
			return false
		}
	}
	return RegistrationHeadroom(node, a.BuildResources)
}

func ZoneRank(zone string) int {
	switch zone {
	case "yellow":
		return 1
	case "red":
		return 2
	case "critical":
		return 3
	default:
		return 0
	}
}

func RegistrationHeadroom(node NodeProfileRecord, requested *routesync.BuildResources) bool {
	if node.BuildRegistrationCapacity == nil || requested == nil {
		return true
	}
	capacity := types.BuildAdmissionLimit{MaxBuilds: node.BuildRegistrationCapacity.MaxBuilds}
	if node.BuildRegistrationCapacity.Resources != nil {
		capacity.Resources = node.BuildRegistrationCapacity.Resources.Types()
	}
	var count int64
	var used types.BuildResources
	if node.BuildRegistrationUsage != nil {
		count = node.BuildRegistrationUsage.Builds
		if node.BuildRegistrationUsage.Resources != nil {
			used = node.BuildRegistrationUsage.Resources.Types()
		}
	}
	return capacity.AllowsAdd(count, used, requested.Types())
}
