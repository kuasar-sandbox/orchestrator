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
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
)

// Placer schedules sandboxes onto nodes.
type Placer struct {
	stores *registry.Stores
	cfg    clustercfg.ScalerConfig
}

// New builds a Placer over the registry's stores with the scaler config.
func New(stores *registry.Stores, cfg clustercfg.ScalerConfig) *Placer {
	if cfg.PlaceCandidates <= 0 {
		cfg.PlaceCandidates = 2
	}
	return &Placer{stores: stores, cfg: cfg}
}

// PlaceSandbox suggests the least-loaded eligible node (cluster-scaler.md §4.2):
// matchSelectors(group's nodeSelectors) ∧ not draining ∧ within the group's
// shuffle-sharding slots, then P2C by sandbox count.
func (p *Placer) PlaceSandbox(ctx context.Context, group, routeKey string) (string, error) {
	var selectors []map[string]string
	if g, found, _ := p.stores.GetGroupByID(ctx, group); found && g != nil {
		selectors = g.NodeSelectors
	}
	slotSet, shardBy := p.shuffleSlots(ctx, group)

	var eligible []*registry.NodeRecord
	if err := p.stores.RangeNodes(ctx, func(n *registry.NodeRecord) error {
		if n.Draining || !matchSelectors(n.Labels, selectors) {
			return nil
		}
		if slotSet != nil && !slotSet[n.Labels[shardBy]] {
			return nil // shuffle-sharding: the group isn't pinned to this node's slot
		}
		eligible = append(eligible, n)
		return nil
	}); err != nil {
		return "", err
	}
	if len(eligible) == 0 {
		return "", registry.ErrNoNode
	}
	return p2c(eligible, p.cfg.PlaceCandidates).NodeID, nil
}

// shuffleSlots returns the deterministic set of shard_by label values this group
// is pinned to (cluster-scaler.md §4.4), or (nil,"") when no shuffle rule
// applies. It buckets the live nodes by each rule's shard_by label and uses
// maglev.LocateN to pick the group's n slots from that set.
func (p *Placer) shuffleSlots(ctx context.Context, group string) (map[string]bool, string) {
	for _, rule := range p.cfg.ShuffleSharding {
		if rule.ShardBy == "" || rule.N <= 0 {
			continue
		}
		// distinct shard_by values among nodes carrying the rule's selector.
		seen := map[string]bool{}
		var values []string
		_ = p.stores.RangeNodes(ctx, func(n *registry.NodeRecord) error {
			if !labelsContain(n.Labels, rule.Selector) {
				return nil
			}
			if v := n.Labels[rule.ShardBy]; v != "" && !seen[v] {
				seen[v] = true
				values = append(values, v)
			}
			return nil
		})
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

// p2c picks the least-loaded of k random candidates (power-of-two-choices when
// k=2): O(1), decorrelated, herd-resistant (cluster-scaler.md §4.2). load =
// sandbox count (the headroom fallback when no resource_listen heartbeat).
func p2c(nodes []*registry.NodeRecord, k int) *registry.NodeRecord {
	if k < 1 {
		k = 1
	}
	best := nodes[rand.Intn(len(nodes))]
	for i := 1; i < k; i++ {
		c := nodes[rand.Intn(len(nodes))]
		if c.Counts < best.Counts {
			best = c
		}
	}
	return best
}
