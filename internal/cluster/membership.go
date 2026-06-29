package cluster

import "context"

type KeyMove struct {
	Key      string   `json:"key"`
	From     []string `json:"from"`
	To       []string `json:"to"`
	Added    []string `json:"added,omitempty"`
	Removed  []string `json:"removed,omitempty"`
	Stable   []string `json:"stable,omitempty"`
	Affected bool     `json:"affected"`
}

type HandoffPlan struct {
	FromVersion int64     `json:"from_version"`
	ToVersion   int64     `json:"to_version"`
	OwnerCount  int       `json:"owner_count"`
	Moves       []KeyMove `json:"moves"`
}

func PlanHandoff(ctx context.Context, from, to MemberView, ownerCount int, keys []string) (HandoffPlan, error) {
	plan := HandoffPlan{FromVersion: from.Version, ToVersion: to.Version, OwnerCount: ownerCount}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return plan, err
		}
		oldOwners, err := from.Owners(key, ownerCount)
		if err != nil {
			return plan, err
		}
		newOwners, err := to.Owners(key, ownerCount)
		if err != nil {
			return plan, err
		}
		move := KeyMove{Key: key, From: oldOwners, To: newOwners}
		move.Stable, move.Added, move.Removed = diffOwners(oldOwners, newOwners)
		move.Affected = len(move.Added) != 0 || len(move.Removed) != 0
		if move.Affected {
			plan.Moves = append(plan.Moves, move)
		}
	}
	return plan, nil
}

func diffOwners(oldOwners, newOwners []string) (stable, added, removed []string) {
	oldSet := make(map[string]bool, len(oldOwners))
	newSet := make(map[string]bool, len(newOwners))
	for _, o := range oldOwners {
		oldSet[o] = true
	}
	for _, n := range newOwners {
		newSet[n] = true
		if oldSet[n] {
			stable = append(stable, n)
		} else {
			added = append(added, n)
		}
	}
	for _, o := range oldOwners {
		if !newSet[o] {
			removed = append(removed, o)
		}
	}
	return stable, added, removed
}
