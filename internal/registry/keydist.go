package registry

import (
	"context"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// Key distribution (cluster.md §7.6): the registry predistributes each group's
// manifest key to its allocation set (the nodes matching the group's
// nodeSelectors) on a TTL lease, ahead of placement, renews held leases, and
// drops the key from nodes that leave the set. Placement itself does no key_put —
// a node missing the key fails create/build (a rejected ack / 4xx), which is the
// intended signal that predistribution must precede placement.

const (
	keyLeaseTTL   = 3 * time.Hour // lease lifetime pushed to nodes
	keyRenewEvery = time.Hour     // reconcile / renew cadence (>=2 renews of margin)
)

// RunKeyDistributor reconciles key leases on start, then every renew interval,
// until ctx is cancelled; cluster-ctl registry runs it in the background.
func (r *Registry) RunKeyDistributor(ctx context.Context, renew time.Duration) {
	if renew <= 0 {
		renew = keyRenewEvery
	}
	r.reconcileKeys(ctx)
	t := time.NewTicker(renew)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.reconcileKeys(ctx)
		case <-r.reconcileTrigger:
			r.reconcileKeys(ctx)
		}
	}
}

// onNodeConnected coalesces a key-reconcile request when a node connects: a
// reconnect storm collapses into one reconcile instead of one goroutine per
// node. The distributor loop drains the trigger; if it isn't running, the
// non-blocking send is simply dropped (tests reconcile directly).
func (r *Registry) onNodeConnected() {
	select {
	case r.reconcileTrigger <- struct{}{}:
	default:
	}
}

// reconcileKeys pushes each keyed group's manifest key to its connected
// allocation set (install / renew) and drops it from nodes that left the set.
// The key-lease map is updated under keyMu, but the channel sends happen AFTER
// the lock is released — so one wedged node can't stall key distribution for
// every other group/node behind a blocked send.
func (r *Registry) reconcileKeys(ctx context.Context) {
	expires := time.Now().Add(keyLeaseTTL).Unix()
	type keyOp struct {
		nodeID, fp, manifestKey string
		drop                    bool
	}
	var ops []keyOp
	_ = r.stores.RangeGroups(ctx, func(g *GroupConfig) error {
		if g.ManifestKey == "" {
			return nil
		}
		fp := keyFingerprint(g.ManifestKey)
		want := r.allocationSet(ctx, g)
		r.keyMu.Lock()
		have := r.keyLeased[g.Group]
		for nodeID := range want {
			ops = append(ops, keyOp{nodeID: nodeID, fp: fp, manifestKey: g.ManifestKey})
		}
		for nodeID := range have {
			if !want[nodeID] {
				ops = append(ops, keyOp{nodeID: nodeID, fp: fp, drop: true})
			}
		}
		r.keyLeased[g.Group] = want
		r.keyMu.Unlock()
		return nil
	})
	for _, o := range ops {
		conn, live := r.node(o.nodeID)
		if !live {
			continue
		}
		if o.drop {
			_ = conn.send(&routesync.Command{CmdID: newID(), Kind: routesync.CmdKeyDrop, KeyFingerprint: o.fp})
		} else {
			_ = conn.send(&routesync.Command{CmdID: newID(), Kind: routesync.CmdKeyPut, KeyFingerprint: o.fp, ManifestKey: o.manifestKey, ExpiresUnix: expires})
		}
	}
}

// allocationSet is the connected nodes matching a group's nodeSelectors (the key
// recipients); empty selectors match every node.
func (r *Registry) allocationSet(ctx context.Context, g *GroupConfig) map[string]bool {
	set := map[string]bool{}
	_ = r.stores.RangeNodes(ctx, func(n *NodeRecord) error {
		if _, live := r.node(n.NodeID); live && matchAnySelector(n.Labels, g.NodeSelectors) {
			set[n.NodeID] = true
		}
		return nil
	})
	return set
}

// matchAnySelector reports whether labels satisfy ANY selector (OR over
// selectors, AND within one); empty selectors match.
func matchAnySelector(labels map[string]string, selectors []map[string]string) bool {
	if len(selectors) == 0 {
		return true
	}
	for _, sel := range selectors {
		ok := true
		for k, v := range sel {
			if labels[k] != v {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}
