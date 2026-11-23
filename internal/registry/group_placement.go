package registry

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand/v2"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	requestplacement "github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// SetPlacementPolicy is startup configuration, independent of import placement.
func (r *Registry) SetPlacementPolicy(candidates int, zoneMax string) error {
	if candidates < 1 || candidates > 64 {
		return fmt.Errorf("registry placement candidates must be 1..64")
	}
	switch zoneMax {
	case "green", "yellow", "red", "critical":
	default:
		return fmt.Errorf("registry placement zone_admit_max is invalid")
	}
	r.mu.Lock()
	r.placementCandidates, r.placementZoneMax = candidates, zoneMax
	r.mu.Unlock()
	return nil
}

type groupCandidate struct {
	placement Placement
	load      float64
}

// selectGroupNode reads only normally published Node support. The result carries
// immutable registration and stable publication versions for the intent commit.
// Request constraints never replace authentication, Group or runtime checks.
func (r *Registry) selectGroupNode(ctx context.Context, req PlaceRequest) (*Placement, error) {
	if err := nodegroup.ValidateName(req.Group); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrReserveBadRequest, err)
	}
	if err := req.Constraints.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrReserveBadRequest, err)
	}
	if req.APIKey == "" {
		return nil, ErrReserveUnauthorized
	}
	parsedKey, err := apikey.Parse(req.APIKey)
	if err != nil {
		return nil, ErrReserveUnauthorized
	}
	var explicit types.TemplateID
	if req.PinnedTemplateSource == "explicit" {
		req.TemplateID = &req.PinnedTemplateID
	}
	if req.PinnedTemplateSource == "default" {
		req.TemplateID = nil
	}
	if req.TemplateID != nil {
		if req.Build {
			return nil, ErrReserveBadRequest
		}
		explicit, err = types.ParseTemplateID(*req.TemplateID)
		if err != nil {
			return nil, fmt.Errorf("%w: templateID: %v", ErrReserveBadRequest, err)
		}
	}
	excluded := placementExclusions{}
	for _, id := range req.ExcludeNodeIDs {
		excluded.add(id)
	}
	r.mu.Lock()
	capacity, zoneMax := r.placementCandidates, r.placementZoneMax
	r.mu.Unlock()
	if capacity < 1 {
		capacity = 2
	}
	sample := make([]groupCandidate, 0, capacity)
	eligible, authenticated := 0, false
	for cursor := metadata.Cursor(""); ; {
		page, err := r.stores.groups.ListGroupSupport(ctx, req.Group, cursor, metadata.MaxGroupSupportBatchRows)
		if err != nil {
			return nil, err
		}
		// The cache is per page, not an independent credential authority.
		verified := make(map[string]bool)
		for _, candidate := range page.Items {
			var chosen *metadata.GroupSupportEntry
			for _, entry := range candidate.Support.Value.Entries {
				if req.PinnedAuthRef != "" && entry.AuthRef != req.PinnedAuthRef {
					continue
				}
				ok, seen := verified[entry.AuthRef]
				if !seen {
					auth, err := r.stores.groups.ReadGroupAuth(ctx, req.Group, entry.AuthRef)
					if err != nil {
						return nil, err
					}
					if auth.Exists {
						raw, decodeErr := hex.DecodeString(auth.Value.APISecret)
						ok = decodeErr == nil && len(raw) == 32 && apikey.Verify(parsedKey, raw)
						clear(raw)
					}
					verified[entry.AuthRef] = ok
				}
				if ok {
					copy := entry
					chosen = &copy
					break
				}
			}
			if chosen == nil {
				continue
			}
			authenticated = true
			node := candidate.Node.Value
			if !candidate.Node.Exists || candidate.Node.Reaping || node.Draining || node.State == metadata.NodeDead || node.State == metadata.NodeDrained || excluded.has(node.NodeID) || (req.RequiredNodeID != "" && req.RequiredNodeID != node.NodeID) || !req.Constraints.Matches(node.Labels) || placementZoneRank(node.Zone) > placementZoneRank(zoneMax) {
				continue
			}
			// Keep the established restore/migration runtime checksum condition.
			if req.TargetRuntimeDigest != "" && node.RuntimeDigest != "" && req.TargetRuntimeDigest != node.RuntimeDigest {
				continue
			}
			selected := Placement{NodeID: node.NodeID, APISecretFingerprint: chosen.AuthRef, TargetPort: chosen.Routing.TargetPort}
			if req.PinnedTemplateSource == "default" && req.PinnedNodeID == node.NodeID && candidate.Support.Value.GroupVersion != req.PinnedGroupVersion {
				continue
			}
			if req.Build {
				if !hasRegistrationHeadroom(nodeRecord(candidate.Node), req.BuildResources) {
					continue
				}
			} else if !req.Migration {
				template := explicit
				selected.TemplateSource = "explicit"
				if req.TemplateID == nil {
					if candidate.Support.Value.DefaultContext == nil {
						continue
					}
					template, err = types.ParseTemplateID(candidate.Support.Value.DefaultContext.TemplateID)
					if err != nil {
						return nil, fmt.Errorf("invalid published Group template: %w", err)
					}
					selected.TemplateSource = "default"
				}
				if !requestplacement.RuntimeMatches(template, node.Labels) {
					continue
				}
				selected.TemplateRef = template.String()
				if req.PinnedTemplateID != "" && selected.TemplateRef != req.PinnedTemplateID {
					continue
				}
			}
			selected.GroupAdmission = &metadata.GroupAdmission{Group: req.Group, GroupVersion: candidate.Support.Value.GroupVersion, AuthRef: chosen.AuthRef, Registration: candidate.Node.Registration, SupportVersion: candidate.Support.Version, PublicationVersion: candidate.Publication.Version}
			selected.GroupAdmission.Placement = req.Constraints.Clone()
			selected.GroupAdmission.ZoneAdmitMax = zoneMax
			selected.GroupAdmission.TargetRuntimeDigest = req.TargetRuntimeDigest
			if req.BuildResources != nil {
				copy := *req.BuildResources
				selected.GroupAdmission.BuildResources = &copy
			}
			item := groupCandidate{placement: selected, load: placementNodeLoad(node)}
			eligible++
			if len(sample) < capacity {
				sample = append(sample, item)
			} else if i := rand.IntN(eligible); i < capacity {
				sample[i] = item
			}
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	if !authenticated {
		return nil, ErrReserveForbidden
	}
	if len(sample) == 0 {
		return nil, ErrNoNode
	}
	// Reservoir sampling supplies distinct candidates; randomize equal-load ties.
	best := rand.IntN(len(sample))
	for i := range sample {
		if sample[i].load < sample[best].load {
			best = i
		}
	}
	return &sample[best].placement, nil
}

func placementZoneRank(zone string) int {
	return metadata.ZoneRank(zone)
}
func placementNodeLoad(node metadata.NodeProfileRecord) float64 {
	if node.Pool > 0 {
		return float64(node.Allocated) / float64(node.Pool)
	}
	if node.Capacity > 0 {
		return float64(node.Counts) / float64(node.Capacity)
	}
	return float64(node.Counts)
}
