// Package placer is the cluster placement scheduler. Placement combines
// nodeSelectors, shuffle-sharding, zone/water-level admission, runtime matching,
// and P2C least-load over the placer's local node_list/group view; registry
// route/node owners commit the suggested node.
package placer

import (
	"math/rand"

	"github.com/kuasar-sandbox/accelerator/pkg/maglev"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// placeParams is one placement evaluation over the placer's local view.
type placeParams struct {
	group               string
	nodes               []*registry.NodeRecord
	selectors           []map[string]string
	rules               []clustercfg.ShuffleRule
	candidates          int
	zoneAdmitMax        string // exclude nodes hotter than this (green<yellow<red<critical); "" = no zone filter
	targetRuntimeDigest string // when set, require node.RuntimeDigest == it (runtime match, §4.2)
	excludedNodeIDs     map[string]struct{}
	buildResources      *routesync.BuildResources
}

// placeSandbox runs the sandbox placement algorithm (cluster-placer.md):
// matchSelectors ∧ ¬draining ∧ zone≤max ∧ runtime-match ∧ shuffle
// slot, then P2C by water level. (Build placement is resource-aware — placeBuild.)
func placeSandbox(p placeParams) (string, error) {
	slotSet, shardBy := shuffleSlots(p.group, p.nodes, p.rules)
	maxZone := zoneRank(p.zoneAdmitMax)
	var eligible []*registry.NodeRecord
	for _, n := range p.nodes {
		if !eligibleNode(n, p, maxZone) {
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
	return p2c(eligible, p.candidates, sandboxLoad).NodeID, nil
}

// placeBuild runs resource-aware build placement (cluster-placer.md). The
// low-frequency node_list deliberately carries configured capacity but not
// heartbeat usage, so this layer only rejects a request that can never fit a
// node. Registry rechecks current registration headroom at the Holder boundary,
// and the selected node remains the final transactional admission authority.
func placeBuild(p placeParams) (string, error) {
	maxZone := zoneRank(p.zoneAdmitMax)
	var eligible []*registry.NodeRecord
	for _, n := range p.nodes {
		if !eligibleNode(n, p, maxZone) || !canRegisterBuild(n, p.buildResources) {
			continue
		}
		eligible = append(eligible, n)
	}
	if len(eligible) == 0 {
		return "", registry.ErrNoNode
	}
	return p2c(eligible, p.candidates, sandboxLoad).NodeID, nil
}

// canRegisterBuild is true when the request fits every configured registration
// dimension in isolation. Current usage is intentionally unavailable in the
// Placer view and is checked by Registry immediately before dispatch.
func canRegisterBuild(n *registry.NodeRecord, want *routesync.BuildResources) bool {
	limit := n.BuildRegistrationCapacity
	if limit == nil {
		return true
	}
	requested := routesync.BuildResources{}
	if want != nil {
		requested = *want
	}
	return dimensionFits(0, 1, limit.MaxBuilds) &&
		dimensionFits(0, requested.CPU, resourceCPU(limit.Resources)) &&
		dimensionFits(0, requested.Memory, resourceMemory(limit.Resources)) &&
		dimensionFits(0, requested.Storage, resourceStorage(limit.Resources))
}

func dimensionFits(used, requested, limit int64) bool {
	if used < 0 || requested < 0 || limit < 0 {
		return false
	}
	if limit == 0 {
		return true
	}
	if requested > limit {
		return false
	}
	return used <= limit-requested
}

func resourceCPU(resources *routesync.BuildResources) int64 {
	if resources == nil {
		return 0
	}
	return resources.CPU
}
func resourceMemory(resources *routesync.BuildResources) int64 {
	if resources == nil {
		return 0
	}
	return resources.Memory
}
func resourceStorage(resources *routesync.BuildResources) int64 {
	if resources == nil {
		return 0
	}
	return resources.Storage
}

// eligibleNode is the shared eligibility predicate (sans shuffle slot): not
// draining, not excluded, zone within admit, runtime-compatible, selector match.
func eligibleNode(n *registry.NodeRecord, p placeParams, maxZone int) bool {
	if n.Draining || !matchSelectors(n.Labels, p.selectors) {
		return false
	}
	if _, excluded := p.excludedNodeIDs[n.NodeID]; excluded {
		return false
	}
	if p.zoneAdmitMax != "" && zoneRank(n.Zone) > maxZone {
		return false // hotter than admit (red/critical excluded)
	}
	if p.targetRuntimeDigest != "" && n.RuntimeDigest != "" && n.RuntimeDigest != p.targetRuntimeDigest {
		return false // snapshot/template runtime mismatch would fail restore (§4.2)
	}
	return true
}

// zoneRank orders water-level zones; an empty zone (no resource controller) is
// treated as green (coolest).
func zoneRank(z string) int {
	switch z {
	case "yellow":
		return 1
	case "red":
		return 2
	case "critical":
		return 3
	default:
		return 0 // green / empty
	}
}

// sandboxLoad is a node's sandbox load for ranking (cluster-placer.md):
// preferred = allocated/pool water level; fallback = sandbox count / capacity
// headroom; last resort = raw count.
func sandboxLoad(n *registry.NodeRecord) float64 {
	if n.Pool > 0 {
		return float64(n.Allocated) / float64(n.Pool)
	}
	if n.Capacity > 0 {
		return float64(n.Counts) / float64(n.Capacity)
	}
	return float64(n.Counts)
}

// shuffleSlots returns the deterministic set of shard_by label values this group
// is pinned to (cluster-placer.md), or (nil,"") when no shuffle rule
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

// effectiveSelectors narrows a group's static nodeSelectors to its shuffle slots
// (cluster-placer.md): the cross-product of the static selectors with
// {shard_by ∈ pinned slots}. Returns (sel, true) when a shuffle rule applies, so
// the registry's key distribution (§7.6) predistributes only to the pinned nodes;
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

// p2c picks the least-loaded (by load) of k DISTINCT random candidates
// (power-of-two-choices when k=2): decorrelated, herd-resistant (cluster-placer.md
// §4.2). Sampling is WITHOUT replacement — with replacement, k=2 over a 2-node set
// returns the more-loaded node ~25% of the time, exactly the eligible-set size
// shuffle-sharding produces. Fisher-Yates also randomizes tie order.
func p2c(nodes []*registry.NodeRecord, k int, load func(*registry.NodeRecord) float64) *registry.NodeRecord {
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
	var best *registry.NodeRecord
	var bestLoad float64
	for i := 0; i < k; i++ {
		j := i + rand.Intn(len(idx)-i)
		idx[i], idx[j] = idx[j], idx[i]
		c := nodes[idx[i]]
		if l := load(c); best == nil || l < bestLoad {
			best, bestLoad = c, l
		}
	}
	return best
}
