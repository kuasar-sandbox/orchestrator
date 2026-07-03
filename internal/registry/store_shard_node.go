package registry

import (
	"context"
	"errors"
	"sort"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
)

func (s *Stores) putNodeProfileShard(ctx context.Context, n *NodeRecord) error {
	_, err := s.putNodeProfileShardReturn(ctx, n)
	return err
}

func (s *Stores) putNodeProfileShardReturn(ctx context.Context, n *NodeRecord) (uint64, error) {
	if n == nil || n.NodeID == "" {
		return 0, nil
	}
	sh, err := s.nodeLinkRecordSet(n.NodeID, clusterstate.RecordSetNodeProfile)
	if err != nil {
		return 0, err
	}
	value, err := clusterstate.EncodeShardValue(nodeProfileFromRegistry(n))
	if err != nil {
		return 0, err
	}
	profile, err := shardUpsertReturn(ctx, sh, clusterstate.NodeLinkProfileRecord, value)
	if err != nil {
		return 0, err
	}
	n.Meta = clusterRecordMeta(profile.Meta)
	return profile.Meta.Rev, nil
}

func (s *Stores) putNodeShard(ctx context.Context, n *NodeRecord) (uint64, error) {
	rev, err := s.putNodeProfileShardReturn(ctx, n)
	if err != nil {
		return 0, err
	}
	if n == nil || n.NodeID == "" {
		return rev, nil
	}
	for _, ref := range n.Sandboxes {
		if err := s.addNodeSandboxRefShard(ctx, n.NodeID, ref); err != nil {
			return 0, err
		}
	}
	for _, ref := range n.Builds {
		if err := s.addNodeBuildRefShard(ctx, n.NodeID, ref); err != nil {
			return 0, err
		}
	}
	for _, key := range n.ManifestKeys {
		if err := s.upsertNodeManifestKeyShard(ctx, n.NodeID, key); err != nil {
			return 0, err
		}
	}
	return rev, nil
}

func (s *Stores) addNodeSandboxRefShard(ctx context.Context, nodeID string, ref clusterstate.NodeSandboxRef) error {
	if nodeID == "" || ref.Group == "" || ref.RouteKey == "" {
		return nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeSandbox)
	if err != nil {
		return err
	}
	value, err := clusterstate.EncodeShardValue(ref)
	if err != nil {
		return err
	}
	return shardUpsert(ctx, sh, clusterstate.NodeSandboxRecordKey(ref.Group, ref.RouteKey), value)
}

func (s *Stores) removeNodeSandboxRefShard(ctx context.Context, nodeID, group, routeKey string) error {
	if nodeID == "" || group == "" || routeKey == "" {
		return nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeSandbox)
	if err != nil {
		return err
	}
	return shardDeleteIfFound(ctx, sh, clusterstate.NodeSandboxRecordKey(group, routeKey))
}

func (s *Stores) addNodeBuildRefShard(ctx context.Context, nodeID string, ref clusterstate.NodeBuildRef) error {
	if nodeID == "" || ref.Group == "" || ref.BuildID == "" {
		return nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeBuild)
	if err != nil {
		return err
	}
	value, err := clusterstate.EncodeShardValue(ref)
	if err != nil {
		return err
	}
	return shardUpsert(ctx, sh, clusterstate.NodeBuildRecordKey(ref.Group, ref.BuildID), value)
}

func (s *Stores) removeNodeBuildRefShard(ctx context.Context, nodeID, group, buildID string) error {
	if nodeID == "" || group == "" || buildID == "" {
		return nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeBuild)
	if err != nil {
		return err
	}
	return shardDeleteIfFound(ctx, sh, clusterstate.NodeBuildRecordKey(group, buildID))
}

func (s *Stores) upsertNodeManifestKeyShard(ctx context.Context, nodeID string, key clusterstate.NodeManifestKey) error {
	if nodeID == "" || key.Fingerprint == "" {
		return nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeManifestKey)
	if err != nil {
		return err
	}
	value, err := clusterstate.EncodeShardValue(key)
	if err != nil {
		return err
	}
	return shardUpsert(ctx, sh, clusterstate.NodeManifestKeyRecordKey(key.Fingerprint), value)
}

func (s *Stores) dropNodeManifestKeyShard(ctx context.Context, nodeID, fingerprint string) error {
	if nodeID == "" || fingerprint == "" {
		return nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeManifestKey)
	if err != nil {
		return err
	}
	return shardDeleteIfFound(ctx, sh, clusterstate.NodeManifestKeyRecordKey(fingerprint))
}

func (s *Stores) getNodeManifestKeyShard(ctx context.Context, nodeID, fingerprint string) (clusterstate.NodeManifestKey, uint64, bool, error) {
	if nodeID == "" || fingerprint == "" {
		return clusterstate.NodeManifestKey{}, 0, false, nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeManifestKey)
	if err != nil {
		return clusterstate.NodeManifestKey{}, 0, false, err
	}
	rec, found, err := sh.Get(ctx, clusterstate.NodeManifestKeyRecordKey(fingerprint))
	if err != nil || !found {
		return clusterstate.NodeManifestKey{}, 0, found, err
	}
	key, err := clusterstate.DecodeShardValue[clusterstate.NodeManifestKey](rec.Value)
	if err != nil {
		return clusterstate.NodeManifestKey{}, 0, false, err
	}
	return key, rec.Meta.Rev, true, nil
}

func (s *Stores) deleteNodeShard(ctx context.Context, nodeID string) error {
	if nodeID == "" {
		return nil
	}
	for _, recordSet := range []shardkv.RecordSetName{
		clusterstate.RecordSetNodeProfile,
		clusterstate.RecordSetNodeSandbox,
		clusterstate.RecordSetNodeBuild,
		clusterstate.RecordSetNodeManifestKey,
	} {
		sh, err := s.nodeLinkRecordSet(nodeID, recordSet)
		if err != nil {
			return err
		}
		snap, err := sh.Snapshot(ctx)
		if err != nil {
			return err
		}
		for _, rec := range snap.Records {
			if _, ok, err := sh.Delete(ctx, rec.Key, rec.Meta.Rev); err != nil {
				return err
			} else if !ok {
				return shardkv.ErrConflict
			}
		}
	}
	return nil
}

func (s *Stores) getNodeShard(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	if nodeID == "" {
		return nil, false, nil
	}
	out, found, err := s.getNodeProfileShard(ctx, nodeID)
	if err != nil || !found {
		return nil, found, err
	}
	sandboxSet, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeSandbox)
	if err != nil {
		return nil, false, err
	}
	snap, err := sandboxSet.Snapshot(ctx)
	if err != nil {
		return nil, false, err
	}
	for _, rec := range snap.Records {
		ref, err := clusterstate.DecodeShardValue[clusterstate.NodeSandboxRef](rec.Value)
		if err != nil {
			return nil, false, err
		}
		out.Sandboxes = append(out.Sandboxes, ref)
	}
	buildSet, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeBuild)
	if err != nil {
		return nil, false, err
	}
	buildSnap, err := buildSet.Snapshot(ctx)
	if err != nil {
		return nil, false, err
	}
	for _, rec := range buildSnap.Records {
		ref, err := clusterstate.DecodeShardValue[clusterstate.NodeBuildRef](rec.Value)
		if err != nil {
			return nil, false, err
		}
		out.Builds = append(out.Builds, ref)
	}
	keySet, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeManifestKey)
	if err != nil {
		return nil, false, err
	}
	keySnap, err := keySet.Snapshot(ctx)
	if err != nil {
		return nil, false, err
	}
	for _, rec := range keySnap.Records {
		key, err := clusterstate.DecodeShardValue[clusterstate.NodeManifestKey](rec.Value)
		if err != nil {
			return nil, false, err
		}
		out.ManifestKeys = append(out.ManifestKeys, key)
	}
	sort.Slice(out.Sandboxes, func(i, j int) bool {
		if out.Sandboxes[i].Group != out.Sandboxes[j].Group {
			return out.Sandboxes[i].Group < out.Sandboxes[j].Group
		}
		return out.Sandboxes[i].RouteKey < out.Sandboxes[j].RouteKey
	})
	sort.Slice(out.Builds, func(i, j int) bool {
		if out.Builds[i].Group != out.Builds[j].Group {
			return out.Builds[i].Group < out.Builds[j].Group
		}
		return out.Builds[i].BuildID < out.Builds[j].BuildID
	})
	sortNodeManifestKeys(out.ManifestKeys)
	return out, true, nil
}

func (s *Stores) getNodeProfileShard(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	if nodeID == "" {
		return nil, false, nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeProfile)
	if err != nil {
		return nil, false, err
	}
	profileRec, found, err := sh.Get(ctx, clusterstate.NodeLinkProfileRecord)
	if err != nil || !found {
		return nil, found, err
	}
	profile, err := clusterstate.DecodeShardValue[clusterstate.NodeProfileRecord](profileRec.Value)
	if err != nil {
		return nil, false, err
	}
	return nodeRecordFromProfile(profile, profileRec.Meta), true, nil
}

func (s *Stores) nodeLinkShard(nodeID string) (*shardkv.Shard, error) {
	store := s.ShardStore()
	if store == nil {
		return nil, errors.New("registry: shard store is not initialized")
	}
	return store.Shard(shardkv.Namespace(clusterstate.NamespaceNodeLink), clusterstate.NodeLinkShard(nodeID))
}

func (s *Stores) nodeLinkRecordSet(nodeID string, recordSet shardkv.RecordSetName) (*shardkv.RecordSet, error) {
	sh, err := s.nodeLinkShard(nodeID)
	if err != nil {
		return nil, err
	}
	return sh.RecordSet(recordSet)
}

func shardUpsert(ctx context.Context, sh *shardkv.RecordSet, key shardkv.RecordKey, value []byte) error {
	for attempt := 0; attempt < 5; attempt++ {
		cur, found, err := sh.Get(ctx, key)
		if err != nil {
			return err
		}
		expect := uint64(0)
		if found {
			expect = cur.Meta.Rev
		}
		_, ok, err := sh.CAS(ctx, key, expect, value)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return shardkv.ErrConflict
}

func shardDeleteIfFound(ctx context.Context, sh *shardkv.RecordSet, key shardkv.RecordKey) error {
	for attempt := 0; attempt < 5; attempt++ {
		cur, found, err := sh.Get(ctx, key)
		if err != nil || !found {
			return err
		}
		_, ok, err := sh.Delete(ctx, key, cur.Meta.Rev)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return shardkv.ErrConflict
}

func nodeProfileFromRegistry(n *NodeRecord) clusterstate.NodeProfileRecord {
	return clusterstate.NodeProfileRecord{
		NodeID: n.NodeID, Labels: cloneStringMap(n.Labels), Capacity: n.Capacity,
		BuildCapacity: cloneBuildResources(n.BuildCapacity), DataEndpoint: n.DataEndpoint,
		RuntimeDigest: n.RuntimeDigest, Zone: n.Zone, Allocated: n.Allocated, Pool: n.Pool,
		BuildAlloc: cloneBuildResources(n.BuildAlloc), Counts: n.Counts, Draining: n.Draining,
		LastHeartbeatUnix: n.LastHeartbeatUnix, ResumeToken: n.ResumeToken, LinkOwner: n.LinkOwner,
	}
}

func nodeRecordFromProfile(p clusterstate.NodeProfileRecord, meta shardkv.RecordMeta) *NodeRecord {
	return &NodeRecord{
		Meta:   clusterRecordMeta(meta),
		NodeID: p.NodeID, Labels: cloneStringMap(p.Labels), Capacity: p.Capacity,
		BuildCapacity: cloneBuildResources(p.BuildCapacity), DataEndpoint: p.DataEndpoint,
		RuntimeDigest: p.RuntimeDigest, Zone: p.Zone, Allocated: p.Allocated, Pool: p.Pool,
		BuildAlloc: cloneBuildResources(p.BuildAlloc), Counts: p.Counts, Draining: p.Draining,
		LastHeartbeatUnix: p.LastHeartbeatUnix, ResumeToken: p.ResumeToken, LinkOwner: p.LinkOwner,
	}
}

func clusterRecordMeta(meta shardkv.RecordMeta) clusterstate.RecordMeta {
	return clusterstate.RecordMeta{
		Ballot:    clusterstate.Ballot{Round: meta.Ballot.Round, Writer: string(meta.Ballot.Writer)},
		Rev:       meta.Rev,
		UpdatedAt: meta.UpdatedAt,
	}
}

func isNodeSandboxRecord(key shardkv.RecordKey) bool {
	_, _, ok := clusterstate.ParseNodeSandboxRecordKey(key)
	return ok
}

func isNodeBuildRecord(key shardkv.RecordKey) bool {
	_, _, ok := clusterstate.ParseNodeBuildRecordKey(key)
	return ok
}

func isNodeManifestKeyRecord(key shardkv.RecordKey) bool {
	_, ok := clusterstate.ParseNodeManifestKeyRecordKey(key)
	return ok
}
