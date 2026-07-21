package placement

import (
	cryptorand "crypto/rand"
	"errors"
	"io"
	"math/big"
	"sort"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

const DefaultCandidateCount = cluster.MaxPlacementCandidates

type CatalogNode struct {
	NodeID        string
	Labels        map[string]string
	FailureDomain string
	RuntimeDigest string
	Capabilities  map[string]bool
	Draining      bool

	SandboxSlotCapacity  uint64
	BuildSlotCapacity    uint64
	BuildCPUCapacity     uint64
	BuildMemoryCapacity  uint64
	BuildStorageCapacity uint64
}

type StaticPolicy struct {
	Selectors             []map[string]string
	AllowedNodeIDs        map[string]struct{}
	ExcludedNodeIDs       map[string]struct{}
	AllowedFailureDomains map[string]struct{}
	TargetRuntimeDigest   string
	RequiredCapabilities  []string
}

type IndexSource interface {
	IntN(int) (int, error)
}

type cryptoIndexSource struct{ reader io.Reader }

func (s cryptoIndexSource) IntN(n int) (int, error) {
	if n <= 0 {
		return 0, errors.New("placement: random bound must be positive")
	}
	value, err := cryptorand.Int(s.reader, big.NewInt(int64(n)))
	if err != nil {
		return 0, err
	}
	return int(value.Int64()), nil
}

func PlaceSandboxN(nodes []CatalogNode, demand SandboxDemand, policy StaticPolicy, n int, source IndexSource) ([]cluster.PlacementCandidate, error) {
	if demand.SlotUnits == 0 {
		return nil, errors.New("placement: sandbox slot demand is required")
	}
	eligible := filterCatalog(nodes, policy, func(node CatalogNode) bool {
		return node.SandboxSlotCapacity > 0 && demand.SlotUnits <= node.SandboxSlotCapacity
	})
	return randomN(eligible, n, source)
}

func PlaceBuildN(nodes []CatalogNode, demand BuildDemand, policy StaticPolicy, n int, source IndexSource) ([]cluster.PlacementCandidate, error) {
	if demand.Slots == 0 {
		return nil, errors.New("placement: build slot demand is required")
	}
	eligible := filterCatalog(nodes, policy, func(node CatalogNode) bool {
		return demand.Slots <= node.BuildSlotCapacity &&
			(demand.CPU == 0 || demand.CPU <= node.BuildCPUCapacity) &&
			(demand.Memory == 0 || demand.Memory <= node.BuildMemoryCapacity) &&
			(demand.Storage == 0 || demand.Storage <= node.BuildStorageCapacity)
	})
	return randomN(eligible, n, source)
}

func filterCatalog(nodes []CatalogNode, policy StaticPolicy, capacity func(CatalogNode) bool) []CatalogNode {
	eligible := make([]CatalogNode, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if node.NodeID == "" || node.Draining || !capacity(node) || !matchesSelectors(node.Labels, policy.Selectors) {
			continue
		}
		if _, duplicate := seen[node.NodeID]; duplicate {
			continue
		}
		if len(policy.AllowedNodeIDs) > 0 {
			if _, allowed := policy.AllowedNodeIDs[node.NodeID]; !allowed {
				continue
			}
		}
		if _, excluded := policy.ExcludedNodeIDs[node.NodeID]; excluded {
			continue
		}
		if len(policy.AllowedFailureDomains) > 0 {
			if _, allowed := policy.AllowedFailureDomains[node.FailureDomain]; !allowed {
				continue
			}
		}
		if policy.TargetRuntimeDigest != "" && node.RuntimeDigest != policy.TargetRuntimeDigest {
			continue
		}
		compatible := true
		for _, capability := range policy.RequiredCapabilities {
			if capability == "" || !node.Capabilities[capability] {
				compatible = false
				break
			}
		}
		if !compatible {
			continue
		}
		seen[node.NodeID] = struct{}{}
		if policy.TargetRuntimeDigest == "" {
			node.RuntimeDigest = ""
		}
		eligible = append(eligible, node)
	}
	return eligible
}

func randomN(nodes []CatalogNode, n int, source IndexSource) ([]cluster.PlacementCandidate, error) {
	if n <= 0 {
		return nil, errors.New("placement: candidate count must be positive")
	}
	if len(nodes) == 0 {
		return nil, errors.New("placement: no statically eligible node")
	}
	if source == nil {
		source = cryptoIndexSource{reader: cryptorand.Reader}
	}
	if n > len(nodes) {
		n = len(nodes)
	}
	pool := append([]CatalogNode(nil), nodes...)
	result := make([]cluster.PlacementCandidate, 0, n)
	for i := 0; i < n; i++ {
		offset, err := source.IntN(len(pool) - i)
		if err != nil {
			return nil, err
		}
		if offset < 0 || offset >= len(pool)-i {
			return nil, errors.New("placement: random source returned an out-of-range index")
		}
		selected := i + offset
		pool[i], pool[selected] = pool[selected], pool[i]
		node := pool[i]
		result = append(result, cluster.PlacementCandidate{
			NodeID: node.NodeID, FailureDomain: node.FailureDomain, RuntimeDigest: node.RuntimeDigest,
		})
	}
	return result, nil
}

func matchesSelectors(labels map[string]string, selectors []map[string]string) bool {
	if len(selectors) == 0 {
		return true
	}
	for _, selector := range selectors {
		matches := true
		keys := make([]string, 0, len(selector))
		for key := range selector {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value, present := labels[key]
			if !present || value != selector[key] {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}
