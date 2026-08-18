package registry

import (
	"context"
	"errors"
	"sort"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

var errNodeSandboxIDConflict = errors.New("registry: sandbox id is already owned by another route on this node")

type nodeReapSandboxRef struct {
	Ref      clusterstate.NodeSandboxRef
	Revision uint64
}

type nodeReapBuildRef struct {
	Ref      clusterstate.NodeBuildRef
	Revision uint64
}

type nodeReapKeyPair struct {
	APISecretFingerprint string
	Revision             uint64
}

type nodeReapSnapshot struct {
	Sandboxes []nodeReapSandboxRef
	Builds    []nodeReapBuildRef
	KeyPairs  []nodeReapKeyPair
}

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

func (s *Stores) casNodeProfileShard(ctx context.Context, n *NodeRecord, expectRev uint64) (uint64, bool, error) {
	if n == nil || n.NodeID == "" {
		return 0, false, nil
	}
	sh, err := s.nodeLinkRecordSet(n.NodeID, clusterstate.RecordSetNodeProfile)
	if err != nil {
		return 0, false, err
	}
	value, err := clusterstate.EncodeShardValue(nodeProfileFromRegistry(n))
	if err != nil {
		return 0, false, err
	}
	profile, ok, err := sh.CAS(ctx, clusterstate.NodeLinkProfileRecord, expectRev, value)
	if err != nil || !ok {
		return 0, ok, err
	}
	n.Meta = clusterRecordMeta(profile.Meta)
	return profile.Meta.Rev, true, nil
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
	for _, pair := range n.KeyPairs {
		if err := s.UpsertNodeKeyPair(ctx, n.NodeID, pair); err != nil {
			return 0, err
		}
	}
	return rev, nil
}

func (s *Stores) addNodeSandboxRefShard(ctx context.Context, nodeID string, ref clusterstate.NodeSandboxRef) error {
	if nodeID == "" || ref.Group == "" || ref.RouteKey == "" ||
		!validNodeSandboxIdentity(ref.SandboxID, ref.NodeSandboxID, ref.SandboxGeneration) ||
		!validFullFingerprint(ref.APISecretFingerprint) || !types.Profile(ref.Profile).Valid() {
		return errors.New("registry: invalid node sandbox ref")
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeSandbox)
	if err != nil {
		return err
	}
	value, err := clusterstate.EncodeShardValue(ref)
	if err != nil {
		return err
	}
	key := clusterstate.NodeSandboxRecordKey(ref.NodeSandboxID)
	for attempt := 0; attempt < 5; attempt++ {
		cur, found, err := sh.Get(ctx, key)
		if err != nil {
			return err
		}
		if found {
			existing, err := clusterstate.DecodeShardValue[clusterstate.NodeSandboxRef](cur.Value)
			if err != nil {
				return err
			}
			if existing.Group == "" || existing.RouteKey == "" ||
				!validNodeSandboxIdentity(existing.SandboxID, existing.NodeSandboxID, existing.SandboxGeneration) ||
				!validFullFingerprint(existing.APISecretFingerprint) || !types.Profile(existing.Profile).Valid() {
				return errors.New("registry: invalid node sandbox ref")
			}
			if existing.Group != ref.Group || existing.RouteKey != ref.RouteKey ||
				existing.SandboxID != ref.SandboxID || existing.SandboxGeneration != ref.SandboxGeneration ||
				existing.NodeSandboxID != ref.NodeSandboxID ||
				existing.Profile != ref.Profile || existing.APISecretFingerprint != ref.APISecretFingerprint {
				return errNodeSandboxIDConflict
			}
			if _, ok, err := sh.CAS(ctx, key, cur.Meta.Rev, value); err != nil {
				return err
			} else if ok {
				return nil
			}
			continue
		}
		if _, ok, err := sh.CAS(ctx, key, 0, value); err != nil {
			return err
		} else if ok {
			return nil
		}
	}
	return shardkv.ErrConflict
}

func (s *Stores) getNodeSandboxRefShard(ctx context.Context, nodeID, nodeSandboxID string) (clusterstate.NodeSandboxRef, bool, error) {
	if nodeID == "" || nodeSandboxID == "" {
		return clusterstate.NodeSandboxRef{}, false, nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeSandbox)
	if err != nil {
		return clusterstate.NodeSandboxRef{}, false, err
	}
	rec, found, err := sh.Get(ctx, clusterstate.NodeSandboxRecordKey(nodeSandboxID))
	if err != nil || !found {
		return clusterstate.NodeSandboxRef{}, found, err
	}
	ref, err := clusterstate.DecodeShardValue[clusterstate.NodeSandboxRef](rec.Value)
	if err != nil {
		return clusterstate.NodeSandboxRef{}, false, err
	}
	if ref.NodeSandboxID != nodeSandboxID || ref.Group == "" || ref.RouteKey == "" ||
		!validNodeSandboxIdentity(ref.SandboxID, ref.NodeSandboxID, ref.SandboxGeneration) ||
		!validFullFingerprint(ref.APISecretFingerprint) || !types.Profile(ref.Profile).Valid() {
		return clusterstate.NodeSandboxRef{}, false, errors.New("registry: invalid node sandbox ref")
	}
	return ref, true, nil
}

func (s *Stores) removeNodeSandboxRefShard(ctx context.Context, nodeID, nodeSandboxID string) error {
	if nodeID == "" || nodeSandboxID == "" {
		return nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeSandbox)
	if err != nil {
		return err
	}
	return shardDeleteIfFound(ctx, sh, clusterstate.NodeSandboxRecordKey(nodeSandboxID))
}

func (s *Stores) removeNodeSandboxRefShardAtRevision(ctx context.Context, nodeID, nodeSandboxID string, expectRev uint64) (bool, error) {
	if nodeID == "" || nodeSandboxID == "" || expectRev == 0 {
		return false, nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeSandbox)
	if err != nil {
		return false, err
	}
	_, ok, err := sh.Delete(ctx, clusterstate.NodeSandboxRecordKey(nodeSandboxID), expectRev)
	return ok, err
}

func (s *Stores) addNodeBuildRefShard(ctx context.Context, nodeID string, ref clusterstate.NodeBuildRef) error {
	if nodeID == "" || ref.Group == "" || ref.BuildID == "" {
		return errors.New("registry: invalid node build ref")
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeBuild)
	if err != nil {
		return err
	}
	value, err := clusterstate.EncodeShardValue(ref)
	if err != nil {
		return err
	}
	key := clusterstate.NodeBuildRecordKey(ref.BuildID)
	for attempt := 0; attempt < 5; attempt++ {
		cur, found, err := sh.Get(ctx, key)
		if err != nil {
			return err
		}
		if found {
			existing, err := clusterstate.DecodeShardValue[clusterstate.NodeBuildRef](cur.Value)
			if err != nil {
				return err
			}
			if existing.BuildID != ref.BuildID || existing.Group == "" {
				return errors.New("registry: invalid node build ref")
			}
			if existing.Group != ref.Group {
				return errNodeBuildIDConflict
			}
			if _, ok, err := sh.CAS(ctx, key, cur.Meta.Rev, value); err != nil {
				return err
			} else if ok {
				return nil
			}
			continue
		}
		if _, ok, err := sh.CAS(ctx, key, 0, value); err != nil {
			return err
		} else if ok {
			return nil
		}
	}
	return shardkv.ErrConflict
}

func (s *Stores) getNodeBuildRefShard(ctx context.Context, nodeID, buildID string) (clusterstate.NodeBuildRef, bool, error) {
	if nodeID == "" || buildID == "" {
		return clusterstate.NodeBuildRef{}, false, nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeBuild)
	if err != nil {
		return clusterstate.NodeBuildRef{}, false, err
	}
	rec, found, err := sh.Get(ctx, clusterstate.NodeBuildRecordKey(buildID))
	if err != nil || !found {
		return clusterstate.NodeBuildRef{}, found, err
	}
	ref, err := clusterstate.DecodeShardValue[clusterstate.NodeBuildRef](rec.Value)
	if err != nil {
		return clusterstate.NodeBuildRef{}, false, err
	}
	if ref.BuildID != buildID || ref.Group == "" {
		return clusterstate.NodeBuildRef{}, false, errors.New("registry: invalid node build ref")
	}
	return ref, true, nil
}

func (s *Stores) removeNodeBuildRefShard(ctx context.Context, nodeID, buildID string) error {
	if nodeID == "" || buildID == "" {
		return nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeBuild)
	if err != nil {
		return err
	}
	return shardDeleteIfFound(ctx, sh, clusterstate.NodeBuildRecordKey(buildID))
}

func (s *Stores) removeNodeBuildRefShardAtRevision(ctx context.Context, nodeID, buildID string, expectRev uint64) (bool, error) {
	if nodeID == "" || buildID == "" || expectRev == 0 {
		return false, nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeBuild)
	if err != nil {
		return false, err
	}
	_, ok, err := sh.Delete(ctx, clusterstate.NodeBuildRecordKey(buildID), expectRev)
	return ok, err
}

func (s *Stores) upsertNodeKeyPairShard(ctx context.Context, nodeID string, pair clusterstate.NodeKeyPair) error {
	if nodeID == "" || pair.APISecretFingerprint == "" {
		return nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeKeyPair)
	if err != nil {
		return err
	}
	key := clusterstate.NodeKeyPairRecordKey(pair.APISecretFingerprint)
	for attempt := 0; attempt < 5; attempt++ {
		currentPair := pair
		current, found, err := sh.Get(ctx, key)
		if err != nil {
			return err
		}
		expect := uint64(0)
		if found {
			existing, err := clusterstate.DecodeShardValue[clusterstate.NodeKeyPair](current.Value)
			if err != nil {
				return err
			}
			if !sameNodeKeyPairMaterial(existing, pair) {
				return errors.New("registry: API secret fingerprint is already bound to a different key pair")
			}
			if currentPair.AckedExpiresUnix == 0 {
				currentPair.AckedExpiresUnix = existing.AckedExpiresUnix
			}
			expect = current.Meta.Rev
		}
		value, err := clusterstate.EncodeShardValue(currentPair)
		if err != nil {
			return err
		}
		if _, ok, err := sh.CAS(ctx, key, expect, value); err != nil {
			return err
		} else if ok {
			return nil
		}
	}
	return shardkv.ErrConflict
}

func (s *Stores) dropNodeKeyPairShard(ctx context.Context, nodeID, apiSecretFingerprint string) error {
	if nodeID == "" || apiSecretFingerprint == "" {
		return nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeKeyPair)
	if err != nil {
		return err
	}
	return shardDeleteIfFound(ctx, sh, clusterstate.NodeKeyPairRecordKey(apiSecretFingerprint))
}

func (s *Stores) dropNodeKeyPairShardAtRevision(ctx context.Context, nodeID, apiSecretFingerprint string, expectRev uint64) (bool, error) {
	if nodeID == "" || apiSecretFingerprint == "" || expectRev == 0 {
		return false, nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeKeyPair)
	if err != nil {
		return false, err
	}
	_, ok, err := sh.Delete(ctx, clusterstate.NodeKeyPairRecordKey(apiSecretFingerprint), expectRev)
	return ok, err
}

func (s *Stores) getNodeKeyPairShard(ctx context.Context, nodeID, apiSecretFingerprint string) (clusterstate.NodeKeyPair, uint64, bool, error) {
	if nodeID == "" || apiSecretFingerprint == "" {
		return clusterstate.NodeKeyPair{}, 0, false, nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeKeyPair)
	if err != nil {
		return clusterstate.NodeKeyPair{}, 0, false, err
	}
	rec, found, err := sh.Get(ctx, clusterstate.NodeKeyPairRecordKey(apiSecretFingerprint))
	if err != nil || !found {
		return clusterstate.NodeKeyPair{}, 0, found, err
	}
	pair, err := clusterstate.DecodeShardValue[clusterstate.NodeKeyPair](rec.Value)
	if err != nil {
		return clusterstate.NodeKeyPair{}, 0, false, err
	}
	return pair, rec.Meta.Rev, true, nil
}

func (s *Stores) markNodeKeyPairAckedShard(ctx context.Context, nodeID string, expected clusterstate.NodeKeyPair) (bool, error) {
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeKeyPair)
	if err != nil {
		return false, err
	}
	recordKey := clusterstate.NodeKeyPairRecordKey(expected.APISecretFingerprint)
	for attempt := 0; attempt < 5; attempt++ {
		rec, found, err := sh.Get(ctx, recordKey)
		if err != nil || !found {
			return false, err
		}
		current, err := clusterstate.DecodeShardValue[clusterstate.NodeKeyPair](rec.Value)
		if err != nil {
			return false, err
		}
		if current.ExpiresUnix != expected.ExpiresUnix || !sameNodeKeyPairMaterial(current, expected) {
			return false, nil
		}
		if current.AckedExpiresUnix >= expected.ExpiresUnix {
			return true, nil
		}
		current.AckedExpiresUnix = expected.ExpiresUnix
		value, err := clusterstate.EncodeShardValue(current)
		if err != nil {
			return false, err
		}
		if _, ok, err := sh.CAS(ctx, recordKey, rec.Meta.Rev, value); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}
	return false, shardkv.ErrConflict
}

func (s *Stores) snapshotNodeReapShard(ctx context.Context, nodeID string) (*nodeReapSnapshot, error) {
	if nodeID == "" {
		return nil, nil
	}
	out := &nodeReapSnapshot{}
	sandboxSet, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeSandbox)
	if err != nil {
		return nil, err
	}
	sandboxSnap, err := sandboxSet.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	for _, rec := range sandboxSnap.Records {
		ref, err := clusterstate.DecodeShardValue[clusterstate.NodeSandboxRef](rec.Value)
		if err != nil {
			return nil, err
		}
		nodeSandboxID, ok := clusterstate.ParseNodeSandboxRecordKey(rec.Key)
		if !ok || ref.NodeSandboxID != nodeSandboxID || ref.Group == "" || ref.RouteKey == "" ||
			!validNodeSandboxIdentity(ref.SandboxID, ref.NodeSandboxID, ref.SandboxGeneration) ||
			!validFullFingerprint(ref.APISecretFingerprint) || !types.Profile(ref.Profile).Valid() {
			return nil, errors.New("registry: invalid node sandbox ref")
		}
		out.Sandboxes = append(out.Sandboxes, nodeReapSandboxRef{Ref: ref, Revision: rec.Meta.Rev})
	}
	buildSet, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeBuild)
	if err != nil {
		return nil, err
	}
	buildSnap, err := buildSet.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	for _, rec := range buildSnap.Records {
		ref, err := clusterstate.DecodeShardValue[clusterstate.NodeBuildRef](rec.Value)
		if err != nil {
			return nil, err
		}
		buildID, ok := clusterstate.ParseNodeBuildRecordKey(rec.Key)
		if !ok || ref.BuildID != buildID || ref.Group == "" {
			return nil, errors.New("registry: invalid node build ref")
		}
		out.Builds = append(out.Builds, nodeReapBuildRef{Ref: ref, Revision: rec.Meta.Rev})
	}
	keySet, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeKeyPair)
	if err != nil {
		return nil, err
	}
	keySnap, err := keySet.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	for _, rec := range keySnap.Records {
		pair, err := clusterstate.DecodeShardValue[clusterstate.NodeKeyPair](rec.Value)
		if err != nil {
			return nil, err
		}
		fingerprint, ok := clusterstate.ParseNodeKeyPairRecordKey(rec.Key)
		if !ok || pair.APISecretFingerprint != fingerprint {
			return nil, errors.New("registry: invalid node key pair")
		}
		out.KeyPairs = append(out.KeyPairs, nodeReapKeyPair{APISecretFingerprint: fingerprint, Revision: rec.Meta.Rev})
	}
	sort.Slice(out.Sandboxes, func(i, j int) bool {
		return out.Sandboxes[i].Ref.NodeSandboxID < out.Sandboxes[j].Ref.NodeSandboxID
	})
	sort.Slice(out.Builds, func(i, j int) bool { return out.Builds[i].Ref.BuildID < out.Builds[j].Ref.BuildID })
	sort.Slice(out.KeyPairs, func(i, j int) bool {
		return out.KeyPairs[i].APISecretFingerprint < out.KeyPairs[j].APISecretFingerprint
	})
	return out, nil
}

func (s *Stores) claimNodeProfileReapShard(ctx context.Context, nodeID string, expectRev uint64) (bool, error) {
	if nodeID == "" || expectRev == 0 {
		return false, nil
	}
	sh, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeProfile)
	if err != nil {
		return false, err
	}
	_, ok, err := sh.Delete(ctx, clusterstate.NodeLinkProfileRecord, expectRev)
	return ok, err
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
		nodeSandboxID, ok := clusterstate.ParseNodeSandboxRecordKey(rec.Key)
		if !ok || ref.NodeSandboxID != nodeSandboxID || ref.Group == "" || ref.RouteKey == "" ||
			!validNodeSandboxIdentity(ref.SandboxID, ref.NodeSandboxID, ref.SandboxGeneration) ||
			!validFullFingerprint(ref.APISecretFingerprint) || !types.Profile(ref.Profile).Valid() {
			return nil, false, errors.New("registry: invalid node sandbox ref")
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
		buildID, ok := clusterstate.ParseNodeBuildRecordKey(rec.Key)
		if !ok || ref.BuildID != buildID || ref.Group == "" {
			return nil, false, errors.New("registry: invalid node build ref")
		}
		out.Builds = append(out.Builds, ref)
	}
	keySet, err := s.nodeLinkRecordSet(nodeID, clusterstate.RecordSetNodeKeyPair)
	if err != nil {
		return nil, false, err
	}
	keySnap, err := keySet.Snapshot(ctx)
	if err != nil {
		return nil, false, err
	}
	for _, rec := range keySnap.Records {
		pair, err := clusterstate.DecodeShardValue[clusterstate.NodeKeyPair](rec.Value)
		if err != nil {
			return nil, false, err
		}
		out.KeyPairs = append(out.KeyPairs, pair)
	}
	sort.Slice(out.Sandboxes, func(i, j int) bool { return out.Sandboxes[i].NodeSandboxID < out.Sandboxes[j].NodeSandboxID })
	sort.Slice(out.Builds, func(i, j int) bool {
		return out.Builds[i].BuildID < out.Builds[j].BuildID
	})
	sortNodeKeyPairs(out.KeyPairs)
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
		BuildRegistrationCapacity: cloneBuildAdmissionLimit(n.BuildRegistrationCapacity), BuildExecutionCapacity: cloneBuildAdmissionLimit(n.BuildExecutionCapacity), DataEndpoint: n.DataEndpoint,
		RuntimeDigest: n.RuntimeDigest, Zone: n.Zone, Allocated: n.Allocated, Pool: n.Pool,
		BuildRegistrationUsage: cloneBuildAdmissionUsage(n.BuildRegistrationUsage), BuildExecutionUsage: cloneBuildAdmissionUsage(n.BuildExecutionUsage), Counts: n.Counts, Draining: n.Draining,
		LastHeartbeatUnix: n.LastHeartbeatUnix, ResumeToken: n.ResumeToken, LinkOwner: n.LinkOwner,
	}
}

func nodeRecordFromProfile(p clusterstate.NodeProfileRecord, meta shardkv.RecordMeta) *NodeRecord {
	return &NodeRecord{
		Meta:   clusterRecordMeta(meta),
		NodeID: p.NodeID, Labels: cloneStringMap(p.Labels), Capacity: p.Capacity,
		BuildRegistrationCapacity: cloneBuildAdmissionLimit(p.BuildRegistrationCapacity), BuildExecutionCapacity: cloneBuildAdmissionLimit(p.BuildExecutionCapacity), DataEndpoint: p.DataEndpoint,
		RuntimeDigest: p.RuntimeDigest, Zone: p.Zone, Allocated: p.Allocated, Pool: p.Pool,
		BuildRegistrationUsage: cloneBuildAdmissionUsage(p.BuildRegistrationUsage), BuildExecutionUsage: cloneBuildAdmissionUsage(p.BuildExecutionUsage), Counts: p.Counts, Draining: p.Draining,
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
	_, ok := clusterstate.ParseNodeSandboxRecordKey(key)
	return ok
}

func isNodeBuildRecord(key shardkv.RecordKey) bool {
	_, ok := clusterstate.ParseNodeBuildRecordKey(key)
	return ok
}

func isNodeKeyPairRecord(key shardkv.RecordKey) bool {
	_, ok := clusterstate.ParseNodeKeyPairRecordKey(key)
	return ok
}
