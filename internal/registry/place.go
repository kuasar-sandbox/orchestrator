package registry

import "context"

// builtinPlacer is the single-member placer (tests / a registry with no scaler attached):
// it picks the live, non-draining node with the lowest sandbox count (a crude
// least-load), ignoring selectors / shuffle / zone. Production placement is the
// standalone scaler over scale_link (channelPlacer); cluster.md §4.1/§5.2.
type builtinPlacer struct{ stores *Stores }

func (p *builtinPlacer) Place(ctx context.Context, req PlaceRequest) (*Placement, error) {
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
		return nil, err
	}
	if best == nil {
		return nil, ErrNoNode
	}
	return &Placement{NodeID: best.NodeID, Config: cloneStringMap(req.Config)}, nil
}
