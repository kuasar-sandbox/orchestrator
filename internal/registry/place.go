package registry

import (
	"context"
	"errors"
	"sort"
)

type noPlacer struct{}

func (noPlacer) Place(context.Context, PlaceRequest) (*Placement, error) {
	return nil, ErrNoNode
}

type placementExclusions map[string]struct{}

func (e placementExclusions) has(nodeID string) bool {
	_, ok := e[nodeID]
	return ok
}

func (e placementExclusions) add(nodeID string) {
	if nodeID != "" {
		e[nodeID] = struct{}{}
	}
}

func (e placementExclusions) values() []string {
	if len(e) == 0 {
		return nil
	}
	ids := make([]string, 0, len(e))
	for id := range e {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (r *Registry) nodeRuntimeLive(ctx context.Context, nodeID string) error {
	if r.nodeOwner == nil {
		return ErrNodeGone
	}
	node, found, err := r.nodeOwner.Runtime(ctx, nodeID)
	if err != nil {
		return err
	}
	if !found || node == nil {
		return ErrNodeGone
	}
	return nil
}

func sameSandboxGeneration(a, b *SandboxRecord) bool {
	return a != nil && b != nil && a.SID == b.SID && a.NodeID == b.NodeID
}

func commandAckTimedOut(ctx context.Context, err error) bool {
	return err != nil && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded)
}
