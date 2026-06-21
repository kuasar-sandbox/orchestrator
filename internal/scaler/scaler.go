// Package scaler is the cluster's placement scheduler (cluster-scaler.md): it
// suggests a node for a new sandbox using nodeSelectors (blast radius) +
// shuffle-sharding (maglev, from sandbox-accelerator/pkg/maglev) + P2C
// least-load, over the registry's node + group state. It implements
// registry.Placer; the registry commits the suggestion by CAS (cluster.md §4.3).
//
// Phase 4 runs the scheduler co-located in the registry process (a Placer the
// registry calls in-process); the standalone cluster-ctl scaler over the op
// channel (the design's separate process for scale) is a Phase 7 deployment of
// the same algorithm.
package scaler

import (
	"context"
	"math/rand"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/maglev"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/groupcfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
)

// Placer schedules sandboxes onto nodes.
type Placer struct {
	stores    *registry.Stores
	placement groupcfg.PlacementProvider // group selectors (store by default; external §6.2)
	cfg       clustercfg.ScalerConfig
}

// New builds a Placer over the registry's stores. placement supplies a group's
// nodeSelectors (nil → the store-backed provider); pass the resolver's placement
// provider to honor an external placement interface.
func New(stores *registry.Stores, placement groupcfg.PlacementProvider, cfg clustercfg.ScalerConfig) *Placer {
	if cfg.PlaceCandidates <= 0 {
		cfg.PlaceCandidates = 2
	}
	if placement == nil {
		placement = registry.StorePlacement(stores)
	}
	return &Placer{stores: stores, placement: placement, cfg: cfg}
}

// PlaceSandbox suggests the least-loaded eligible node (cluster-scaler.md §4.2):
// matchSelectors(group's nodeSelectors) ∧ not draining ∧ within the group's
// shuffle-sharding slots, then P2C by sandbox count.
func (p *Placer) PlaceSandbox(ctx context.Context, group, routeKey string) (string, error) {
	var selectors []map[string]string
	if pl, found, _ := p.placement.Placement(ctx, group); found {
		selectors = pl.NodeSelectors
	}
	// Scan the node table once (RangeNodes deserializes every row — one pass), then
	// run the shared placement algorithm over the slice.
	var nodes []*registry.NodeRecord
	if err := p.stores.RangeNodes(ctx, func(n *registry.NodeRecord) error {
		nodes = append(nodes, n)
		return nil
	}); err != nil {
		return "", err
	}
	return placeOver(group, nodes, selectors, p.cfg.ShuffleSharding, p.cfg.PlaceCandidates)
}

// placeOver runs the placement algorithm over a node set + a group's selectors
// (cluster-scaler.md §4.2): matchSelectors ∧ ¬draining ∧ shuffle slot, then P2C by
// sandbox count. Shared by the in-process Placer and the standalone scaler's
// view-backed handler.
func placeOver(group string, nodes []*registry.NodeRecord, selectors []map[string]string, rules []clustercfg.ShuffleRule, candidates int) (string, error) {
	slotSet, shardBy := shuffleSlots(group, nodes, rules)
	var eligible []*registry.NodeRecord
	for _, n := range nodes {
		if n.Draining || !matchSelectors(n.Labels, selectors) {
			continue
		}
		if slotSet != nil && !slotSet[n.Labels[shardBy]] {
			continue // shuffle-sharding: the group isn't pinned to this node's slot
		}
		eligible = append(eligible, n)
	}
	if len(eligible) == 0 {
		return "", registry.ErrNoNode
	}
	return p2c(eligible, candidates).NodeID, nil
}

// shuffleSlots returns the deterministic set of shard_by label values this group
// is pinned to (cluster-scaler.md §4.4), or (nil,"") when no shuffle rule
// applies. It buckets the given nodes by each rule's shard_by label and uses
// maglev.LocateN to pick the group's n slots from that set.
func shuffleSlots(group string, nodes []*registry.NodeRecord, rules []clustercfg.ShuffleRule) (map[string]bool, string) {
	for _, rule := range rules {
		if rule.ShardBy == "" || rule.N <= 0 {
			continue
		}
		// distinct shard_by values among nodes carrying the rule's selector.
		seen := map[string]bool{}
		var values []string
		for _, n := range nodes {
			if !labelsContain(n.Labels, rule.Selector) {
				continue
			}
			if v := n.Labels[rule.ShardBy]; v != "" && !seen[v] {
				seen[v] = true
				values = append(values, v)
			}
		}
		if len(values) == 0 {
			continue
		}
		n := rule.N
		if n > len(values) {
			n = len(values)
		}
		shards, err := maglev.LocateN([]byte(group), values, n)
		if err != nil {
			continue
		}
		set := make(map[string]bool, len(shards))
		for _, s := range shards {
			set[s] = true
		}
		return set, rule.ShardBy
	}
	return nil, ""
}

// matchSelectors reports whether labels satisfy ANY selector in the list (OR over
// selectors, AND within one). An empty list matches every node.
func matchSelectors(labels map[string]string, selectors []map[string]string) bool {
	if len(selectors) == 0 {
		return true
	}
	for _, sel := range selectors {
		if labelsContain(labels, sel) {
			return true
		}
	}
	return false
}

// labelsContain reports whether labels include every key=value in want.
func labelsContain(labels, want map[string]string) bool {
	for k, v := range want {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// p2c picks the least-loaded of k DISTINCT random candidates (power-of-two-
// choices when k=2): decorrelated, herd-resistant (cluster-scaler.md §4.2).
// Sampling is WITHOUT replacement — with replacement, k=2 over a 2-node set
// returns the more-loaded node ~25% of the time, which is exactly the eligible-
// set size shuffle-sharding produces. Fisher-Yates also randomizes tie order.
// load = sandbox count (the headroom fallback when no resource heartbeat).
func p2c(nodes []*registry.NodeRecord, k int) *registry.NodeRecord {
	if k > len(nodes) {
		k = len(nodes)
	}
	if k < 1 {
		k = 1
	}
	idx := make([]int, len(nodes))
	for i := range idx {
		idx[i] = i
	}
	best := nodes[0]
	for i := 0; i < k; i++ {
		j := i + rand.Intn(len(idx)-i)
		idx[i], idx[j] = idx[j], idx[i]
		if c := nodes[idx[i]]; i == 0 || c.Counts < best.Counts {
			best = c
		}
	}
	return best
}
