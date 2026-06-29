package registry

import (
	"context"
	"time"
)

// Key distribution (cluster.md §12): the registry projects each group's manifest
// key allocation into per-node node_link key caches. Heartbeat maintenance sends
// periodic key_put renewals from that cache; placement itself does no key_put.

const (
	keyLeaseTTL   = 3 * time.Hour // lease lifetime pushed to nodes
	keyRenewEvery = time.Hour     // reconcile / renew cadence (>=2 renews of margin)
)

type keyLeaseState struct {
	fp    string
	nodes map[string]bool
}

type keyOp struct {
	nodeID, fp, keyType, keyValue string
	expiresUnix                   int64
}

type keyAllocationState struct {
	fp       string
	keyType  string
	keyValue string
	nodes    map[string]bool
}

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

// reconcileKeys projects scaler-owned group allocations into each node's
// node_link manifest-key cache. Actual key_put refreshes are driven by node
// heartbeat maintenance; this path must not synchronously push node commands.
func (r *Registry) reconcileKeys(ctx context.Context) {
	expires := time.Now().Add(keyLeaseTTL).Unix()
	allocs := r.keyAllocations()
	prev := r.keyLeaseSnapshot()
	next := make(map[string]keyLeaseState, len(allocs))
	desired := map[string]map[string]keyOp{}
	for group, alloc := range allocs {
		if alloc.fp == "" || alloc.keyValue == "" {
			continue
		}
		want := r.liveAllocationSet(alloc.nodes)
		for nodeID := range want {
			if desired[nodeID] == nil {
				desired[nodeID] = map[string]keyOp{}
			}
			desired[nodeID][alloc.fp] = keyOp{nodeID: nodeID, fp: alloc.fp, keyType: alloc.keyType, keyValue: alloc.keyValue, expiresUnix: expires}
		}
		next[group] = keyLeaseState{fp: alloc.fp, nodes: want}
	}
	for _, keys := range desired {
		for _, o := range keys {
			if r.nodeOwner == nil {
				continue
			}
			_ = r.nodeOwner.PutManifestKey(ctx, o.nodeID, o.fp, o.keyType, o.keyValue, o.expiresUnix)
		}
	}
	for _, have := range previousNodeKeySet(prev) {
		if r.nodeOwner == nil {
			continue
		}
		if desired[have.nodeID] != nil && desired[have.nodeID][have.fp].fp != "" {
			continue
		}
		_ = r.nodeOwner.DropManifestKey(ctx, have.nodeID, have.fp)
	}
	r.replaceKeyLeases(next)
}

func (r *Registry) keyLeaseSnapshot() map[string]keyLeaseState {
	r.keyMu.Lock()
	defer r.keyMu.Unlock()
	out := make(map[string]keyLeaseState, len(r.keyLeased))
	for group, lease := range r.keyLeased {
		nodes := make(map[string]bool, len(lease.nodes))
		for nodeID := range lease.nodes {
			nodes[nodeID] = true
		}
		out[group] = keyLeaseState{fp: lease.fp, nodes: nodes}
	}
	return out
}

func (r *Registry) replaceKeyLeases(next map[string]keyLeaseState) {
	r.keyMu.Lock()
	r.keyLeased = next
	r.keyMu.Unlock()
}

func previousNodeKeySet(prev map[string]keyLeaseState) []keyOp {
	seen := map[string]bool{}
	var out []keyOp
	for _, lease := range prev {
		if lease.fp == "" {
			continue
		}
		for nodeID := range lease.nodes {
			key := nodeID + "\x00" + lease.fp
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, keyOp{nodeID: nodeID, fp: lease.fp})
		}
	}
	return out
}

func (r *Registry) liveAllocationSet(nodes map[string]bool) map[string]bool {
	set := map[string]bool{}
	for nodeID := range nodes {
		if r.nodeOwner == nil {
			continue
		}
		if _, found, err := r.nodeOwner.Runtime(context.Background(), nodeID); err == nil && found {
			set[nodeID] = true
		}
	}
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
