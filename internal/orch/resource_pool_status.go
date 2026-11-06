package orch

import "github.com/kuasar-sandbox/orchestrator/internal/nodectl"

func (o *Orchestrator) runnerPoolStatus() ([]nodectl.RunnerPoolStatus, int) {
	o.runs.mu.Lock()
	defer o.runs.mu.Unlock()
	var out []nodectl.RunnerPoolStatus
	for _, set := range []*runPools{o.runnerPool, o.builderRunPool} {
		for _, p := range set.pools {
			view := nodectl.RunnerPoolStatus{Kind: p.kind, Name: p.name, ResourcePool: p.resourcePool, Unit: p.unitName(""), IdleTarget: p.size, Priority: p.priority, States: map[string]int{}}
			if p.capacity != nil {
				n := *p.capacity
				view.Capacity = &n
			}
			waiting := 0
			for id, owner := range o.runs.owners {
				if owner == p {
					view.Total++
					state := o.runs.states[id]
					if state == "" {
						state = "restored"
					}
					view.States[state]++
					if o.runs.waiting[id] == p {
						waiting++
					}
				}
			}
			view.Reserved = max(0, p.reserved-waiting)
			out = append(out, view)
		}
	}
	unknown := 0
	for id := range o.runs.units {
		if o.runs.owners[id] == nil {
			unknown++
		}
	}
	return out, unknown
}
