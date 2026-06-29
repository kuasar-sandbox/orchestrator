package registry

import (
	"context"
	"fmt"
	"time"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// BuildReserveResult is the registry-assigned identity + placement for a build
// (the router returns these to the e2b client + routes its follow-ups by build_id).
type BuildReserveResult struct {
	BuildID      string `json:"build_id"`
	TemplateID   string `json:"template_id"`
	NodeID       string `json:"node_id"`
	DataEndpoint string `json:"data_endpoint"`
}

// BuildReserveReq is the router's build-register ask: the group + the (optional)
// declared build resources + the template config metadata (cpu/memory/headers).
type BuildReserveReq struct {
	Group     string                    `json:"group"`
	Resources *routesync.BuildResources `json:"resources,omitempty"`
	Metadata  map[string]string         `json:"metadata,omitempty"`
}

// defaultBuildResources is one build's resource footprint when the request omits
// it (cluster.md §7.5 "不指定则使用默认"); a coarse single-build slot.
var defaultBuildResources = &routesync.BuildResources{CPU: 1000, Mem: 1 << 30}

const buildRegisterAckTimeout = 5 * time.Second

// ReserveBuild places a build on a resource-eligible node and pre-provisions it
// (cluster.md §7.5): the registry assigns the build/template ids, resource-aware
// PlaceBuild picks a node, the BuildStore commit RESERVES that node's build pool
// IMMEDIATELY (so concurrent builds don't oversubscribe before heartbeats catch
// up), and a build_register command hands the node the ids + image-pull creds.
// Build state flows back as build events (releasing the reservation on terminal).
func (r *Registry) ReserveBuild(ctx context.Context, req BuildReserveReq) (*BuildReserveResult, error) {
	if req.Group == "" {
		return nil, fmt.Errorf("registry: group is required")
	}
	resources := req.Resources
	if resources == nil {
		resources = defaultBuildResources
	}
	buildID := "bld-" + newID()
	templateID := "transient-" + newID()

	// Place + commit, re-asking once if the suggested node lacks headroom for THIS
	// build given the already-RESERVED builds (the registry is authoritative, §7.5).
	var nodeID string
	for attempt := 0; attempt < 2; attempt++ {
		placement, err := r.placer.Place(ctx, PlaceRequest{Group: req.Group, RouteKey: "build", Build: true, Config: req.Metadata})
		if err != nil {
			return nil, err
		}
		if placement == nil || placement.NodeID == "" {
			return nil, ErrNoNode
		}
		id := placement.NodeID
		if !r.admitBuild(ctx, id, buildID, resources) {
			if attempt == 0 {
				continue // node owner budget rejected; re-ask the placer
			}
			return nil, ErrNoNode
		}
		// Commit the group build record after node-owner admission succeeds.
		rec := &BuildRecord{Group: req.Group, BuildID: buildID, NodeID: id, Resources: resources, State: BuildRegistered}
		if err := r.stores.PutBuild(ctx, rec); err != nil {
			r.releaseBuildAdmission(buildID)
			return nil, err
		}
		_ = r.stores.AddNodeBuildRef(ctx, id, clusterstate.NodeBuildRef{Group: req.Group, BuildID: buildID})
		nodeID = id
		cmd := &routesync.Command{
			CmdID: newID(), Kind: routesync.CmdBuildRegister, Group: req.Group,
			BuildID: buildID, TemplateRef: templateID, BuildResources: resources, Config: req.Metadata,
			KeyFingerprint: placement.KeyFingerprint, ImageRepo: placement.ImageRepo, RegistryAuth: placement.RegistryAuth,
		}
		if r.nodeOwner == nil {
			_ = r.stores.DeleteBuild(ctx, req.Group, buildID)
			_ = r.stores.RemoveNodeBuildRef(ctx, id, req.Group, buildID)
			r.releaseBuildAdmission(buildID)
			return nil, ErrNodeGone
		}
		ack, err := r.nodeOwner.SendCommandAndWait(ctx, id, cmd, buildRegisterAckTimeout)
		if err != nil || ack == nil || ack.Status != routesync.AckAccepted {
			_ = r.stores.DeleteBuild(ctx, req.Group, buildID) // roll back the reservation
			_ = r.stores.RemoveNodeBuildRef(ctx, id, req.Group, buildID)
			r.releaseBuildAdmission(buildID)
			if attempt == 0 {
				continue
			}
			if err != nil {
				return nil, err
			}
			reason := ""
			if ack != nil {
				reason = ack.Reason
			}
			return nil, fmt.Errorf("registry: build_register rejected: %s", reason)
		}
		break
	}
	if nodeID == "" {
		return nil, ErrNoNode
	}
	return &BuildReserveResult{BuildID: buildID, TemplateID: templateID, NodeID: nodeID, DataEndpoint: r.nodeDataEndpoint(ctx, nodeID)}, nil
}

func (r *Registry) admitBuild(ctx context.Context, nodeID, buildID string, want *routesync.BuildResources) bool {
	if r.nodeOwner == nil {
		return false
	}
	return r.nodeOwner.AdmitBuild(ctx, nodeID, buildID, want)
}

func (r *Registry) releaseBuildAdmission(buildID string) {
	if r.nodeOwner != nil {
		r.nodeOwner.ReleaseBuild(context.Background(), buildID)
	}
}

// applyBuildEvent converges a build's state from a node's build event (§5.1): it
// updates the BuildStore (releasing the reservation on a terminal state, since the
// headroom sum counts only registered/building builds).
func (r *Registry) applyBuildEvent(ctx context.Context, nodeID string, e *routesync.BuildEvent) {
	if e.Group == "" || e.BuildID == "" {
		return
	}
	rec, found, err := r.stores.GetBuildInGroup(ctx, e.Group, e.BuildID)
	if err != nil || !found {
		return
	}
	if rec.NodeID != "" && nodeID != "" && rec.NodeID != nodeID {
		return
	}
	rec.State = BuildState(e.State)
	if e.TemplateID != "" {
		rec.TemplateID = e.TemplateID
	}
	rec.Reason = e.Reason
	_ = r.stores.PutBuild(ctx, rec)
	if !rec.occupies() {
		r.releaseBuildAdmission(rec.BuildID)
		_ = r.stores.RemoveNodeBuildRef(ctx, rec.NodeID, rec.Group, rec.BuildID)
	}
}

// ResolveBuild maps a group's build_id to its node (router restart recovery: the
// router's in-memory build map is lost, but route_link replicated build state is
// still group-sharded).
func (r *Registry) ResolveBuild(ctx context.Context, group, buildID string) (*BuildReserveResult, bool) {
	b, found, err := r.stores.GetBuildInGroup(ctx, group, buildID)
	if err != nil || !found {
		return nil, false
	}
	return &BuildReserveResult{BuildID: b.BuildID, TemplateID: b.TemplateID, NodeID: b.NodeID, DataEndpoint: r.nodeDataEndpoint(ctx, b.NodeID)}, true
}
