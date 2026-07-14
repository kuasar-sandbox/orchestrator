package registry

import (
	"context"
	"errors"
	"fmt"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
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
	Group      string                    `json:"group"`
	BuildID    string                    `json:"build_id,omitempty"`
	TemplateID string                    `json:"template_id,omitempty"`
	Resources  *routesync.BuildResources `json:"resources,omitempty"`
	Metadata   map[string]string         `json:"metadata,omitempty"`
}

// defaultBuildResources is one build's resource footprint when the request omits
// it (cluster.md "不指定则使用默认"); a coarse single-build slot.
var defaultBuildResources = &routesync.BuildResources{CPU: 1000, Mem: 1 << 30}

const buildRegisterAckTimeout = 5 * time.Second

const (
	terminalBuildStoreAttempts   = 5
	terminalBuildStoreRetryDelay = 20 * time.Millisecond
)

var errNodeBuildIDConflict = errors.New("registry: build id is already owned by another group on this node")

// ReserveBuild places a build on a resource-eligible node and pre-provisions it
// (cluster.md): the registry assigns the build/template ids, resource-aware
// PlaceBuild picks a node, the BuildStore commit RESERVES that node's build pool
// IMMEDIATELY (so concurrent builds don't oversubscribe before heartbeats catch
// up), and a build_register command hands the node the ids + image-pull creds.
// Build state flows back as build events (releasing the reservation on terminal).
func (r *Registry) ReserveBuild(ctx context.Context, req BuildReserveReq) (*BuildReserveResult, error) {
	if req.Group == "" {
		return nil, fmt.Errorf("registry: group is required")
	}
	metadata, err := clusterstate.WithObjectLocation(req.Metadata, clusterstate.ObjectLocation{Group: req.Group})
	if err != nil {
		return nil, err
	}
	req.Metadata = metadata
	resources := req.Resources
	if resources == nil {
		resources = defaultBuildResources
	}
	buildID := req.BuildID
	if buildID == "" {
		buildID = "bld-" + newID()
	}
	ref, err := clusterstate.NodeBuildRefFromMetadata(buildID, metadata)
	if err != nil {
		return nil, err
	}
	templateID := req.TemplateID
	if templateID == "" {
		templateID = "transient-" + newID()
	}
	if req.BuildID != "" {
		if rec, found, err := r.stores.GetBuildInGroup(ctx, req.Group, req.BuildID); err != nil {
			return nil, err
		} else if found {
			return r.buildReserveResult(ctx, rec), nil
		}
	}

	// Place + commit. node_list supplies candidates; the node owner validates the
	// live connection and authoritative build budget before the build is committed.
	excluded := placementExclusions{}
	var lastFailure error
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		placement, err := r.placer.Place(ctx, PlaceRequest{
			Group: req.Group, RouteKey: "build", Build: true, Config: req.Metadata,
			ExcludeNodeIDs: excluded.values(),
		})
		if err != nil {
			if errors.Is(err, ErrNoNode) && lastFailure != nil {
				return nil, lastFailure
			}
			return nil, err
		}
		if placement == nil || placement.NodeID == "" {
			return nil, ErrNoNode
		}
		id := placement.NodeID
		if excluded.has(id) {
			if lastFailure != nil {
				return nil, lastFailure
			}
			return nil, ErrNoNode
		}
		if err := r.nodeRuntimeLive(ctx, id); err != nil {
			if !errors.Is(err, ErrNodeGone) {
				return nil, err
			}
			lastFailure = err
			excluded.add(id)
			continue
		}
		if !r.admitBuild(ctx, id, req.Group, buildID, resources) {
			excluded.add(id)
			continue
		}
		// Commit the group build record after node-owner admission succeeds.
		rec := &BuildRecord{Group: req.Group, BuildID: buildID, NodeID: id, Resources: resources, State: BuildRegistered, TemplateID: templateID}
		if err := r.stores.PutBuild(ctx, rec); err != nil {
			r.releaseBuildAdmission(id, req.Group, buildID)
			return nil, err
		}
		if err := r.stores.AddNodeBuildRef(ctx, id, ref); err != nil {
			_ = r.stores.DeleteBuild(ctx, req.Group, buildID)
			r.releaseBuildAdmission(id, req.Group, buildID)
			if errors.Is(err, errNodeBuildIDConflict) {
				lastFailure = err
				excluded.add(id)
				continue
			}
			return nil, err
		}
		cmd := &routesync.Command{
			CmdID: newID(), Kind: routesync.CmdBuildRegister,
			BuildID: buildID, TemplateRef: templateID, BuildResources: resources, Config: req.Metadata,
			KeyFingerprint: placement.KeyFingerprint, ImageRepo: placement.ImageRepo, RegistryAuth: placement.RegistryAuth,
		}
		if r.nodeOwner == nil {
			_ = r.stores.DeleteBuild(ctx, req.Group, buildID)
			_ = r.stores.RemoveNodeBuildRef(ctx, id, buildID)
			r.releaseBuildAdmission(id, req.Group, buildID)
			lastFailure = ErrNodeGone
			excluded.add(id)
			continue
		}
		ack, err := r.nodeOwner.SendCommandAndWait(ctx, id, cmd, buildRegisterAckTimeout)
		if commandAckTimedOut(ctx, err) {
			return r.buildReserveResult(ctx, rec), nil
		}
		if err != nil {
			if !errors.Is(err, ErrNodeGone) {
				return r.buildReserveResult(ctx, rec), nil
			}
			_ = r.stores.DeleteBuild(ctx, req.Group, buildID) // command definitively did not reach the node
			_ = r.stores.RemoveNodeBuildRef(ctx, id, buildID)
			r.releaseBuildAdmission(id, req.Group, buildID)
			lastFailure = err
			excluded.add(id)
			continue
		}
		if ack == nil || ack.Status != routesync.AckAccepted {
			_ = r.stores.DeleteBuild(ctx, req.Group, buildID) // roll back the reservation
			_ = r.stores.RemoveNodeBuildRef(ctx, id, buildID)
			r.releaseBuildAdmission(id, req.Group, buildID)
			reason := ""
			if ack != nil {
				reason = ack.Reason
			}
			lastFailure = fmt.Errorf("registry: build_register rejected: %s", reason)
			excluded.add(id)
			continue
		}
		return r.buildReserveResult(ctx, rec), nil
	}
}

func (r *Registry) buildReserveResult(ctx context.Context, rec *BuildRecord) *BuildReserveResult {
	if rec == nil {
		return nil
	}
	return &BuildReserveResult{
		BuildID:      rec.BuildID,
		TemplateID:   rec.TemplateID,
		NodeID:       rec.NodeID,
		DataEndpoint: r.nodeDataEndpoint(ctx, rec.NodeID),
	}
}

func (r *Registry) admitBuild(ctx context.Context, nodeID, group, buildID string, want *routesync.BuildResources) bool {
	if r.nodeOwner == nil {
		return false
	}
	return r.nodeOwner.AdmitBuild(ctx, nodeID, buildAdmissionID(group, buildID), want)
}

func (r *Registry) releaseBuildAdmission(nodeID, group, buildID string) {
	if r.nodeOwner != nil {
		r.nodeOwner.ReleaseBuild(context.Background(), nodeID, buildAdmissionID(group, buildID))
	}
}

func buildAdmissionID(group, buildID string) string { return group + "\x00" + buildID }

// applyBuildEvent converges a build's state from a node's build event (§5.1): it
// updates the BuildStore (releasing the reservation on a terminal state, since the
// headroom sum counts only registered/building builds).
func (r *Registry) applyBuildEvent(ctx context.Context, nodeID string, e *routesync.BuildEvent) {
	if e == nil || e.BuildID == "" {
		return
	}
	ref, found, err := r.lookupNodeBuildRef(ctx, nodeID, e.BuildID)
	if err != nil || !found {
		return
	}
	rec, found, err := r.stores.GetBuildInGroup(ctx, ref.Group, e.BuildID)
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
	terminal := !rec.occupies()
	if terminal {
		// Build events are transition-only. Free the volatile capacity lease even
		// when route_link persistence is temporarily unavailable.
		r.releaseBuildAdmission(rec.NodeID, rec.Group, rec.BuildID)
	}
	put := func(writeCtx context.Context) error { return r.stores.PutBuild(writeCtx, rec) }
	var writeErr error
	if terminal {
		writeErr = retryTerminalBuildStore(ctx, put)
	} else {
		writeErr = put(ctx)
	}
	if writeErr != nil {
		r.log.Warn("registry: persist build event", "node", nodeID, "build", rec.BuildID, "state", rec.State, "err", writeErr)
		return
	}
}

func retryTerminalBuildStore(ctx context.Context, operation func(context.Context) error) error {
	retryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lifecycleAckTimeout)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt < terminalBuildStoreAttempts; attempt++ {
		if lastErr = operation(retryCtx); lastErr == nil {
			return nil
		}
		if attempt+1 == terminalBuildStoreAttempts {
			break
		}
		timer := time.NewTimer(time.Duration(attempt+1) * terminalBuildStoreRetryDelay)
		select {
		case <-retryCtx.Done():
			timer.Stop()
			return retryCtx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func (r *Registry) lookupNodeBuildRef(ctx context.Context, nodeID, buildID string) (clusterstate.NodeBuildRef, bool, error) {
	var ref clusterstate.NodeBuildRef
	var found bool
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		ref, found, err = r.stores.GetNodeBuildRef(ctx, nodeID, buildID)
		if err == nil || !transientRouteRead(err) {
			return ref, found, err
		}
		select {
		case <-ctx.Done():
			return clusterstate.NodeBuildRef{}, false, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		}
	}
	return ref, found, err
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
