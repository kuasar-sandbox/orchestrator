package cluster

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/kuasar-sandbox/accelerator/pkg/maglev"
)

// ConnectionOwners locates the Registry processes that may own a Node stream.
// Its immutable lookup tables are independent of FoundationDB replication.
type ConnectionOwners struct {
	localID string
	views   atomic.Pointer[connectionViews]
}

type connectionView struct {
	table *maglev.Table
	count int
}
type connectionViews struct{ views []connectionView }

func NewConnectionOwners(localID string, views []MemberView, count int) (*ConnectionOwners, error) {
	if localID == "" {
		return nil, fmt.Errorf("cluster: local connection owner ID is required")
	}
	o := &ConnectionOwners{localID: localID}
	if err := o.Update(views, count); err != nil {
		return nil, err
	}
	return o, nil
}

func (o *ConnectionOwners) LocalID() string { return o.localID }

// Update prepares every table before atomically installing the new view. It
// rejects invalid membership without changing the current routing decision.
func (o *ConnectionOwners) Update(views []MemberView, count int) error {
	if count <= 0 || len(views) == 0 {
		return fmt.Errorf("cluster: connection owner views and positive count are required")
	}
	ordered := append([]MemberView(nil), views...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Version < ordered[j].Version })
	next := &connectionViews{views: make([]connectionView, 0, len(views))}
	versions := make(map[int64]bool, len(views))
	for _, view := range ordered {
		if view.Version <= 0 || versions[view.Version] {
			return fmt.Errorf("cluster: invalid or duplicate connection view version %d", view.Version)
		}
		versions[view.Version] = true
		members := make(map[string]bool, len(view.Members))
		for _, id := range view.Members {
			if id == "" || members[id] {
				return fmt.Errorf("cluster: empty or duplicate connection owner in view %d", view.Version)
			}
			members[id] = true
		}
		if len(members) == 0 {
			return fmt.Errorf("cluster: empty connection view %d", view.Version)
		}
		n := min(count, len(members))
		next.views = append(next.views, connectionView{table: maglev.New(view.Members), count: n})
	}
	o.views.Store(next)
	return nil
}

func (o *ConnectionOwners) Candidates(ctx context.Context, nodeID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if nodeID == "" {
		return nil, fmt.Errorf("cluster: node ID is required")
	}
	current := o.views.Load()
	if current == nil {
		return nil, fmt.Errorf("cluster: connection owner views are not initialized")
	}
	var out []string
	seen := make(map[string]bool)
	for _, view := range current.views {
		members, err := view.table.LocateN([]byte(nodeID), view.count)
		if err != nil {
			return nil, err
		}
		for _, id := range members {
			if !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
	}
	return out, nil
}
