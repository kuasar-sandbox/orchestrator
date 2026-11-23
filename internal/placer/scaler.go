// Package placer imports Node-owned Groups using static selectors and shuffle sharding.
package placer

import (
	"github.com/kuasar-sandbox/accelerator/pkg/maglev"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"sort"
)

// shuffleSlots returns the deterministic set of shard_by label values this group
// is pinned to (docs/cluster.md#placer-placement), or (nil,"") when no shuffle rule
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
			if n == nil {
				continue
			}
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
		sort.Strings(values)
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

// effectiveSelectors narrows a group's static nodeSelectors to its shuffle slots
// (docs/cluster.md#placer-placement): the cross-product of the static selectors with
// {shard_by ∈ pinned slots}. Returns (sel, true) when a shuffle rule applies, so
// the registry's key distribution (docs/cluster.md#placer-key-distribution) predistributes only to the pinned nodes;
// (nil, false) when no shuffle rule narrows (key dist uses the static selectors).
func effectiveSelectors(group string, nodes []*registry.NodeRecord, selectors []map[string]string, rules []clustercfg.ShuffleRule) ([]map[string]string, bool) {
	slotSet, shardBy := shuffleSlots(group, nodes, rules)
	if slotSet == nil {
		return nil, false
	}
	base := selectors
	if len(base) == 0 {
		base = []map[string]string{{}} // match-all → a single empty base selector
	}
	var eff []map[string]string
	for slot := range slotSet {
		for _, sel := range base {
			if value, exists := sel[shardBy]; exists && value != slot {
				continue
			}
			m := map[string]string{shardBy: slot}
			for k, v := range sel {
				m[k] = v
			}
			eff = append(eff, m)
		}
	}
	return eff, true
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
