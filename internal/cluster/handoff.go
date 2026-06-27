package cluster

import "context"

type RouteReplicaMap map[string]RouteReplica

// SyncRouteHandoff pre-syncs moved route_link keys to their added owners. It is
// the in-memory equivalent of learner catch-up before a membership flip.
func SyncRouteHandoff(ctx context.Context, plan HandoffPlan, replicas RouteReplicaMap) error {
	for _, move := range plan.Moves {
		if err := ctx.Err(); err != nil {
			return err
		}
		best, found, err := highestRouteForKey(ctx, move.Key, move.From, replicas)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		for _, owner := range move.Added {
			r := replicas[owner]
			if r == nil {
				continue
			}
			if err := r.Repair(ctx, move.Key, best); err != nil {
				return err
			}
		}
	}
	return nil
}

func highestRouteForKey(ctx context.Context, key string, owners []string, replicas RouteReplicaMap) (RouteRecord, bool, error) {
	var reads []routeRead
	for _, owner := range owners {
		r := replicas[owner]
		if r == nil {
			continue
		}
		rec, found, err := r.Read(ctx, key)
		if err != nil {
			return RouteRecord{}, false, err
		}
		reads = append(reads, routeRead{rec: rec, found: found})
	}
	best, found := highestRoute(reads)
	return best, found, nil
}
