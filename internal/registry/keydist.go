package registry

import (
	"context"
	"time"
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

type keyLeaseState struct {
	fp    string
	nodes map[string]bool
}

type keyOp struct {
	nodeID, fp, keyType, keyValue string
	drop                          bool
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

// reconcileKeys pushes each keyed group's manifest key to its connected
// allocation set (install / renew) and drops it from nodes that left the set.
// The key-lease map is updated under keyMu, but the channel sends happen AFTER
// the lock is released — so one wedged node can't stall key distribution for
// every other group/node behind a blocked send.
func (r *Registry) reconcileKeys(ctx context.Context) {
	expires := time.Now().Add(keyLeaseTTL).Unix()
	var ops []keyOp
	allocs := r.keyAllocations()
	for group, alloc := range allocs {
		if alloc.fp == "" || alloc.keyValue == "" {
			r.dropGroupKeyLeases(group, &ops)
			continue
		}
		want := r.liveAllocationSet(alloc.nodes)
		r.keyMu.Lock()
		have := r.keyLeased[group]
		if have.fp != "" && have.fp != alloc.fp {
			for nodeID := range have.nodes {
				ops = append(ops, keyOp{nodeID: nodeID, fp: have.fp, drop: true})
			}
		}
		for nodeID := range want {
			ops = append(ops, keyOp{nodeID: nodeID, fp: alloc.fp, keyType: alloc.keyType, keyValue: alloc.keyValue})
		}
		for nodeID := range have.nodes {
			if have.fp == alloc.fp && !want[nodeID] {
				ops = append(ops, keyOp{nodeID: nodeID, fp: have.fp, drop: true})
			}
		}
		r.keyLeased[group] = keyLeaseState{fp: alloc.fp, nodes: want}
		r.keyMu.Unlock()
	}
	for _, o := range ops {
		if r.nodeOwner == nil {
			continue
		}
		if o.drop {
			_ = r.nodeOwner.DropManifestKey(ctx, o.nodeID, o.fp)
		} else {
			_ = r.nodeOwner.PutManifestKey(ctx, o.nodeID, o.fp, o.keyType, o.keyValue, expires)
		}
	}
}

func (r *Registry) dropGroupKeyLeases(group string, ops *[]keyOp) {
	r.keyMu.Lock()
	defer r.keyMu.Unlock()
	if have, ok := r.keyLeased[group]; ok {
		for nodeID := range have.nodes {
			*ops = append(*ops, keyOp{nodeID: nodeID, fp: have.fp, drop: true})
		}
		delete(r.keyLeased, group)
	}
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
