package controlplane

import (
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

type RegistryLayoutReconnectRouter struct {
	self         string
	members      []session.Member
	endpoints    map[string]string
	availability func(string) bool
}

func NewRegistryLayoutReconnectRouter(
	self string,
	registryLayout raftstore.RegistryLayout,
	availability func(string) bool,
) (*RegistryLayoutReconnectRouter, error) {
	if self == "" || availability == nil {
		return nil, errors.New("controlplane: local reconnect member and availability source are required")
	}
	members := make([]session.Member, 0, len(registryLayout.Members))
	endpoints := make(map[string]string, len(registryLayout.Members))
	selfFound := false
	for _, member := range registryLayout.Members {
		if err := member.Validate(); err != nil {
			return nil, err
		}
		members = append(members, session.Member{MemberID: member.MemberID, Weight: 1, Available: true})
		endpoints[member.MemberID] = member.InternalEndpoint
		selfFound = selfFound || member.MemberID == self
	}
	if !selfFound {
		return nil, errors.New("controlplane: local reconnect member is absent from RegistryLayout")
	}
	return &RegistryLayoutReconnectRouter{
		self: self, members: members, endpoints: endpoints, availability: availability,
	}, nil
}

func (r *RegistryLayoutReconnectRouter) RedirectTargets(nodeID string) ([]routesync.NodeLinkTarget, error) {
	members := append([]session.Member(nil), r.members...)
	for index := range members {
		members[index].Available = members[index].MemberID == r.self || r.availability(members[index].MemberID)
	}
	ranked, err := session.RankReconnectTargets(nodeID, members)
	if err != nil {
		return nil, err
	}
	if ranked[0] == r.self {
		return nil, nil
	}
	targets := make([]routesync.NodeLinkTarget, 0, len(ranked)-1)
	for _, memberID := range ranked {
		if memberID == r.self {
			continue
		}
		targets = append(targets, routesync.NodeLinkTarget{MemberID: memberID, Endpoint: r.endpoints[memberID]})
	}
	return targets, nil
}
