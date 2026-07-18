package raftstore

import (
	"context"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

func (r *Runtime) ApplySystem(ctx context.Context, command SystemCommand) (SystemApplyResult, error) {
	if command.Type == SystemBootstrap || command.Type == SystemRefreshPermit {
		return SystemApplyResult{}, errors.New("raftstore: use the dedicated bootstrap/permit operation")
	}
	if command.Type == SystemBeginTransition {
		if command.Transition == nil || command.Transition.Version != r.manifest.ManifestVersion ||
			command.Transition.Digest != r.manifestDigest {
			return SystemApplyResult{}, errors.New("raftstore: transition does not name the verified next manifest")
		}
	}
	return r.proposeSystem(ctx, command)
}

func (r *Runtime) ReadData(ctx context.Context, query DataLookup) (DataLookupResult, error) {
	if err := query.Validate(); err != nil {
		return DataLookupResult{}, err
	}
	identity, logicalShardID, strong := lookupIdentity(query)
	if err := r.permitCache.Authorize(identity.PermitIdentity, PermitRegistryRead); err != nil {
		return DataLookupResult{}, err
	}
	var (
		value any
		err   error
	)
	if strong {
		value, err = r.nodeHost.SyncRead(ctx, DataRaftShardID(logicalShardID), query)
	} else {
		value, err = r.nodeHost.StaleRead(DataRaftShardID(logicalShardID), query)
	}
	if err != nil {
		return DataLookupResult{}, err
	}
	result, ok := value.(DataLookupResult)
	if !ok {
		return DataLookupResult{}, errors.New("raftstore: Dragonboat returned an invalid data lookup result")
	}
	if !strong {
		r.attachLeaderHint(logicalShardID, &result)
	}
	return result, nil
}

func lookupIdentity(query DataLookup) (ShardRequestIdentity, uint32, bool) {
	switch {
	case query.Route != nil:
		return shardIdentityFromRoute(query.Route.RequestIdentity), query.Route.ShardID, query.Route.Strong
	case query.Build != nil:
		return shardIdentityFromRoute(query.Build.RequestIdentity), query.Build.ShardID, query.Build.Strong
	default:
		return query.Pending.Identity, query.Pending.Identity.ShardID, true
	}
}

func (r *Runtime) attachLeaderHint(shardID uint32, result *DataLookupResult) {
	if result == nil {
		return
	}
	needsHint := result.Route != nil &&
		(result.Route.Outcome == routeapi.ReadNeedLeader || result.Route.Outcome == routeapi.ReadReplicaBehind) ||
		result.Build != nil &&
			(result.Build.Outcome == routeapi.ReadNeedLeader || result.Build.Outcome == routeapi.ReadReplicaBehind)
	if !needsHint {
		return
	}
	leaderID, term, valid, err := r.nodeHost.GetLeaderID(DataRaftShardID(shardID))
	if err != nil || !valid || term == 0 {
		return
	}
	placement := r.manifest.DataShards[shardID]
	for _, replica := range placement.Replicas {
		if replica.ReplicaID != leaderID {
			continue
		}
		member, found := manifestMember(r.manifest, replica.MemberID)
		if !found {
			return
		}
		hint := &routeapi.LeaderHint{MemberID: member.MemberID, Endpoint: member.InternalEndpoint, Term: term}
		if result.Route != nil {
			result.Route.LeaderHint = hint
		} else {
			result.Build.LeaderHint = hint
		}
		return
	}
}
