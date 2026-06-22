package registry

import (
	"context"

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

// ReserveBuild places a build on a resource-eligible node and pre-provisions it
// (cluster.md §7.5): the registry assigns the build/template ids, resource-aware
// PlaceBuild picks a node, the BuildStore commit RESERVES that node's build pool
// IMMEDIATELY (so concurrent builds don't oversubscribe before heartbeats catch
// up), and a build_register command hands the node the ids + image-pull creds.
// Build state flows back as build events (releasing the reservation on terminal).
func (r *Registry) ReserveBuild(ctx context.Context, req BuildReserveReq) (*BuildReserveResult, error) {
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
		id, err := r.placer.Place(ctx, PlaceRequest{Group: req.Group, RouteKey: "build", Build: true})
		if err != nil {
			return nil, err
		}
		conn, live := r.node(id)
		if !live {
			continue
		}
		if !r.buildHeadroom(ctx, id, resources) {
			if attempt == 0 {
				continue // oversubscribed (commit-time check); re-ask the placer
			}
			return nil, ErrNoNode
		}
		// Commit the reservation BEFORE sending the command — RESERVED occupies now.
		rec := &BuildRecord{Group: req.Group, BuildID: buildID, NodeID: id, Resources: resources, State: BuildRegistered}
		if err := r.stores.PutBuild(ctx, rec); err != nil {
			return nil, err
		}
		nodeID = id
		cmd := &routesync.Command{
			CmdID: newID(), Kind: routesync.CmdBuildRegister, Group: req.Group,
			BuildID: buildID, TemplateRef: templateID, BuildResources: resources, Config: req.Metadata,
		}
		if k, ok, _ := r.resolver.Key.Key(ctx, req.Group); ok && k.ManifestKey != "" {
			cmd.KeyFingerprint = keyFingerprint(k.ManifestKey)
		}
		if ip, ok, _ := r.resolver.ImagePull.ImagePull(ctx, req.Group); ok {
			cmd.ImageRepo, cmd.RegistryAuth = ip.ImageRepo, ip.RegistryAuth // delivered with the build task; node uses transiently
		}
		if err := conn.send(cmd); err != nil {
			_ = r.stores.DeleteBuild(ctx, req.Group, buildID) // roll back the reservation
			return nil, err
		}
		break
	}
	if nodeID == "" {
		return nil, ErrNoNode
	}
	return &BuildReserveResult{BuildID: buildID, TemplateID: templateID, NodeID: nodeID, DataEndpoint: r.nodeDataEndpoint(ctx, nodeID)}, nil
}

// buildHeadroom reports whether nodeID can fit want given its declared build pool
// (NodeRecord.BuildCapacity) minus the sum of RESERVED/building builds on it (the
// registry-authoritative occupancy, §7.5). A node with no declared pool fits.
func (r *Registry) buildHeadroom(ctx context.Context, nodeID string, want *routesync.BuildResources) bool {
	node, found, err := r.stores.GetNode(ctx, nodeID)
	if err != nil || !found || node.BuildCapacity == nil {
		return found && err == nil // no declared pool → unconstrained
	}
	var usedCPU int
	var usedMem, usedStor int64
	_ = r.stores.RangeBuilds(ctx, func(b *BuildRecord) error {
		if b.NodeID == nodeID && b.occupies() && b.Resources != nil {
			usedCPU += b.Resources.CPU
			usedMem += b.Resources.Mem
			usedStor += b.Resources.Storage
		}
		return nil
	})
	cap := node.BuildCapacity
	return usedCPU+want.CPU <= cap.CPU && usedMem+want.Mem <= cap.Mem && usedStor+want.Storage <= cap.Storage
}

// applyBuildEvent converges a build's state from a node's build event (§5.1): it
// updates the BuildStore (releasing the reservation on a terminal state, since the
// headroom sum counts only registered/building builds).
func (r *Registry) applyBuildEvent(ctx context.Context, e *routesync.BuildEvent) {
	if e.Group == "" || e.BuildID == "" {
		return
	}
	rec, found, err := r.stores.GetBuildInGroup(ctx, e.Group, e.BuildID)
	if err != nil || !found {
		return
	}
	rec.State = BuildState(e.State)
	if e.TemplateID != "" {
		rec.TemplateID = e.TemplateID
	}
	rec.Reason = e.Reason
	_ = r.stores.PutBuild(ctx, rec)
}

// SetGroupTemplate updates a group's template_ref in-process (the registry is the
// sole sqlite writer, so a build that just produced a template can point its
// group at it without a second writer racing the revision counter).
func (r *Registry) SetGroupTemplate(ctx context.Context, group, templateRef string) error {
	g, found, err := r.stores.GetGroupByID(ctx, group)
	if err != nil || !found || g == nil {
		return err
	}
	g.TemplateRef = templateRef
	return r.stores.PutGroup(ctx, g)
}

// ResolveBuild maps a build_id to its node (router restart recovery: the router's
// in-memory build map is lost, but the BuildStore persists). Scans builds (small).
func (r *Registry) ResolveBuild(ctx context.Context, buildID string) (*BuildReserveResult, bool) {
	var res *BuildReserveResult
	_ = r.stores.RangeBuilds(ctx, func(b *BuildRecord) error {
		if b.BuildID == buildID {
			res = &BuildReserveResult{BuildID: b.BuildID, TemplateID: b.TemplateID, NodeID: b.NodeID, DataEndpoint: r.nodeDataEndpoint(ctx, b.NodeID)}
		}
		return nil
	})
	return res, res != nil
}
