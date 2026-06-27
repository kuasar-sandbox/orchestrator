package registry

import (
	"context"
	"fmt"
	"time"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
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
	nodeID, fp, manifestKey string
	drop                    bool
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
	seen := map[string]bool{}
	importErr := r.rangeImportedGroups(ctx, func(group string) error {
		seen[group] = true
		manifestKey, found, err := r.inlineManifestKey(ctx, group)
		if err != nil {
			r.log.Warn("registry: manifest key unavailable for distribution", "group", group, "err", err)
			r.dropGroupKeyLeases(group, &ops)
			return nil
		}
		if !found || manifestKey == "" {
			r.dropGroupKeyLeases(group, &ops)
			return nil
		}
		fp := keyFingerprint(manifestKey)
		want := r.allocationSet(ctx, group)
		r.keyMu.Lock()
		have := r.keyLeased[group]
		if have.fp != "" && have.fp != fp {
			for nodeID := range have.nodes {
				ops = append(ops, keyOp{nodeID: nodeID, fp: have.fp, drop: true})
			}
		}
		for nodeID := range want {
			ops = append(ops, keyOp{nodeID: nodeID, fp: fp, manifestKey: manifestKey})
		}
		for nodeID := range have.nodes {
			if have.fp == fp && !want[nodeID] {
				ops = append(ops, keyOp{nodeID: nodeID, fp: have.fp, drop: true})
			}
		}
		r.keyLeased[group] = keyLeaseState{fp: fp, nodes: want}
		r.keyMu.Unlock()
		return nil
	})
	if importErr != nil {
		r.log.Warn("registry: group import failed during key reconcile", "err", importErr)
	} else {
		r.keyMu.Lock()
		for group, have := range r.keyLeased {
			if seen[group] {
				continue
			}
			for nodeID := range have.nodes {
				ops = append(ops, keyOp{nodeID: nodeID, fp: have.fp, drop: true})
			}
			delete(r.keyLeased, group)
		}
		r.keyMu.Unlock()
	}
	for _, o := range ops {
		if r.nodeOwner == nil {
			continue
		}
		if o.drop {
			_ = r.nodeOwner.DropManifestKey(ctx, o.nodeID, o.fp)
		} else {
			_ = r.nodeOwner.PutManifestKey(ctx, o.nodeID, o.fp, o.manifestKey, expires)
		}
	}
}

func (r *Registry) rangeImportedGroups(ctx context.Context, fn func(string) error) error {
	if r.groupImporter == nil {
		return fmt.Errorf("registry: sandbox group importer is not configured")
	}
	cursor := ""
	for {
		page, err := r.groupImporter.Range(ctx, cursor, 1024)
		if err != nil {
			return err
		}
		for _, group := range page.Groups {
			if group == "" {
				continue
			}
			if err := fn(group); err != nil {
				return err
			}
		}
		if page.NextCursor == "" {
			return nil
		}
		if page.NextCursor == cursor {
			return fmt.Errorf("registry: sandbox group importer did not advance cursor %q", cursor)
		}
		cursor = page.NextCursor
	}
}

func (r *Registry) inlineManifestKey(ctx context.Context, group string) (string, bool, error) {
	k, found, err := r.groupProvider.GetKey(ctx, group)
	if err != nil || !found || k.Value == "" {
		return "", found, err
	}
	if k.Type != "" && k.Type != clusterstate.SecretInline {
		return "", true, fmt.Errorf("typed manifest_key %q requires out-of-band node delivery", k.Type)
	}
	return k.Value, true, nil
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

// allocationSet returns the manifest-key recipients. The scaler-owned explicit
// allocation is authoritative once present; local selector matching is used only
// for size-1/bootstrap operation before the scaler pushes allocation.
func (r *Registry) allocationSet(ctx context.Context, group string) map[string]bool {
	if alloc, ok := r.keyAllocation(group); ok {
		set := map[string]bool{}
		for nodeID := range alloc {
			if _, live := r.node(nodeID); live {
				set[nodeID] = true
			}
		}
		return set
	}
	var selectors []map[string]string
	if hint, ok, err := r.groupProvider.GetPlacementHint(ctx, group); err != nil {
		r.log.Warn("registry: placement hint unavailable during key reconcile", "group", group, "err", err)
	} else if ok {
		selectors = hint.NodeSelectors
	}
	if eff, ok := r.effectiveSelectors(group); ok {
		selectors = eff
	}
	set := map[string]bool{}
	_ = r.stores.RangeNodes(ctx, func(n *NodeRecord) error {
		if _, live := r.node(n.NodeID); live && matchAnySelector(n.Labels, selectors) {
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
