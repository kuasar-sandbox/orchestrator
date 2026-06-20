package registry

import "context"

// builtinPlacer is the registry's default placer for a single-node-cluster: it
// picks the live, non-draining node with the lowest sandbox count (a crude
// least-load). cluster-ctl scaler replaces it with shuffle-sharding +
// nodeSelectors over the op channel in Phase 4; group affinity / selectors are
// ignored here.
type builtinPlacer struct{ stores *Stores }

func (p *builtinPlacer) PlaceSandbox(ctx context.Context, group, routeKey string) (string, error) {
	var best *NodeRecord
	if err := p.stores.RangeNodes(ctx, func(n *NodeRecord) error {
		if n.Draining {
			return nil
		}
		if best == nil || n.Counts < best.Counts {
			best = n
		}
		return nil
	}); err != nil {
		return "", err
	}
	if best == nil {
		return "", ErrNoNode
	}
	return best.NodeID, nil
}
