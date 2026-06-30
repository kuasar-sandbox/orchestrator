package shardkv

import (
	"errors"
	"fmt"
	"sort"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/maglev"
)

type ClusterView struct {
	Version int64      `json:"version"`
	Label   string     `json:"label"`
	Members []MemberID `json:"members"`
}

type MaglevResolverConfig struct {
	Local  MemberID
	Views  []ClusterView
	Layout Layout
}

type MaglevResolver struct {
	local  MemberID
	views  []ClusterView
	layout Layout
}

func NewMaglevResolver(cfg MaglevResolverConfig) (*MaglevResolver, error) {
	if cfg.Local == "" {
		return nil, errors.New("shardkv: local member is required")
	}
	if len(cfg.Views) == 0 {
		return nil, errors.New("shardkv: at least one cluster view is required")
	}
	for ns, spec := range cfg.Layout.Namespaces {
		if ns == "" {
			return nil, errors.New("shardkv: empty namespace")
		}
		if spec.ShardMemberCount <= 0 {
			return nil, fmt.Errorf("shardkv: namespace %s member count must be positive", ns)
		}
	}
	views := append([]ClusterView(nil), cfg.Views...)
	sort.Slice(views, func(i, j int) bool { return views[i].Version < views[j].Version })
	for i := range views {
		if len(views[i].Members) == 0 {
			return nil, fmt.Errorf("shardkv: view %d has no members", views[i].Version)
		}
		if views[i].Label == "" {
			views[i].Label = fmt.Sprintf("membership.%d", views[i].Version)
		}
		views[i].Members = normalizeMembers(views[i].Members)
	}
	return &MaglevResolver{local: cfg.Local, views: views, layout: cfg.Layout}, nil
}

func (r *MaglevResolver) ResolveShard(ns Namespace, shard ShardKey) (ShardView, error) {
	if r == nil {
		return ShardView{}, ErrInvalidView
	}
	spec, ok := r.layout.Namespaces[ns]
	if !ok {
		return ShardView{}, ErrUnknownNamespace
	}
	out := ShardView{Namespace: ns, Shard: shard, Local: r.local}
	for _, view := range r.views {
		owners, err := locateMembers(shard, view.Members, spec.ShardMemberCount)
		if err != nil {
			return ShardView{}, err
		}
		set := ShardMemberSet{
			Version: view.Version,
			Label:   view.Label,
			Members: owners,
			Quorum:  len(owners)/2 + 1,
		}
		out.Sets = append(out.Sets, set)
		out.Label = view.Label
	}
	return out, validateView(out)
}

func validateView(view ShardView) error {
	if view.Namespace == "" || view.Shard == "" || view.Local == "" || len(view.Sets) == 0 {
		return ErrInvalidView
	}
	for _, set := range view.Sets {
		if len(set.Members) == 0 || set.Quorum <= 0 || set.Quorum > len(set.Members) {
			return ErrInvalidView
		}
	}
	return nil
}

func locateMembers(shard ShardKey, members []MemberID, n int) ([]MemberID, error) {
	if n <= 0 {
		return nil, errors.New("shardkv: member count must be positive")
	}
	values := make([]string, 0, len(members))
	for _, member := range members {
		if member != "" {
			values = append(values, string(member))
		}
	}
	sort.Strings(values)
	if len(values) == 0 {
		return nil, errors.New("shardkv: empty member view")
	}
	if n > len(values) {
		n = len(values)
	}
	owners, err := maglev.LocateN([]byte(shard), values, n)
	if err != nil {
		return nil, fmt.Errorf("shardkv: locate shard members: %w", err)
	}
	out := make([]MemberID, len(owners))
	for i, owner := range owners {
		out[i] = MemberID(owner)
	}
	return out, nil
}

func normalizeMembers(in []MemberID) []MemberID {
	seen := map[MemberID]bool{}
	out := make([]MemberID, 0, len(in))
	for _, member := range in {
		if member == "" || seen[member] {
			continue
		}
		seen[member] = true
		out = append(out, member)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func shardMembers(view ShardView) []MemberID {
	seen := map[MemberID]bool{}
	var out []MemberID
	for _, set := range view.Sets {
		for _, member := range set.Members {
			if member == "" || seen[member] {
				continue
			}
			seen[member] = true
			out = append(out, member)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func quorumSatisfied(ok map[MemberID]bool, sets []ShardMemberSet) bool {
	for _, set := range sets {
		count := 0
		for _, member := range set.Members {
			if ok[member] {
				count++
			}
		}
		if count < set.Quorum {
			return false
		}
	}
	return true
}
