package registry

import (
	"context"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// ReserveBuild places a template build on a node (cluster.md §11): it picks a
// build-capable node via the placer and predistributes the group's manifest key
// (so the node's build API authorizes the router-forwarded request), then
// returns the node's data endpoint. The router forwards the build control calls
// (register/trigger/status/files) to that node and routes follow-ups by build id.
//
// Unlike a sandbox, a build is not keyed by (group, route_key) — it is transient
// and the router tracks build_id -> node; durable build tracking in a BuildStore
// is a later refinement.
func (r *Registry) ReserveBuild(ctx context.Context, group string) (*ReserveResult, error) {
	nodeID, err := r.placer.PlaceSandbox(ctx, group, "build")
	if err != nil {
		return nil, err
	}
	conn, ok := r.node(nodeID)
	if !ok {
		return nil, ErrNoNode
	}
	if g, gok, _ := r.stores.GetGroupByID(ctx, group); gok && g.ManifestKey != "" {
		fp := keyFingerprint(g.ManifestKey)
		// Gate on the key being installed: the build register arrives over the data
		// plane (HTTP) and races this channel command, so wait for the node's ack
		// before handing the node back to the router (cluster.md §5.1, §7.6).
		ack, err := r.sendAndWait(ctx, conn, &routesync.Command{
			CmdID: newID(), Kind: routesync.CmdKeyPut, KeyFingerprint: fp,
			ManifestKey: g.ManifestKey, ExpiresUnix: time.Now().Add(3 * time.Hour).Unix(),
		}, 10*time.Second)
		if err != nil {
			return nil, fmt.Errorf("registry: build key predistribution: %w", err)
		}
		if ack.Status != routesync.AckAccepted {
			return nil, fmt.Errorf("registry: build node rejected key: %s", ack.Reason)
		}
	}
	return &ReserveResult{NodeID: nodeID, DataEndpoint: r.nodeDataEndpoint(ctx, nodeID)}, nil
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
