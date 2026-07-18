package raftstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"golang.org/x/sync/errgroup"
)

const fenceRetentionProofDomain = "kuasar-fence-retention-proof-v1\x00"

type FenceOutboxAckEvidence struct {
	AckedWatermark uint64 `json:"acked_watermark"`
	ProofDigest    string `json:"proof_digest"`
}

func (e FenceOutboxAckEvidence) validates(fence clusterstate.ExecutionFence) bool {
	return fence.FinalOutboxWatermark >= fence.LastEventSeq &&
		e.AckedWatermark == fence.FinalOutboxWatermark &&
		isSHA256(e.ProofDigest)
}

// CompactExecutionFence waits the configured retention period from a fresh
// strong read, proves every exact manifest voter has applied the fence, and
// then submits the only authorized compaction command path.
func (r *Runtime) CompactExecutionFence(
	ctx context.Context,
	identity ShardRequestIdentity,
	group string,
	routeKey string,
	sandboxID string,
	outboxAck FenceOutboxAckEvidence,
) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if group == "" || routeKey == "" || sandboxID == "" {
		return errors.New("raftstore: execution-fence compaction identity is incomplete")
	}
	_, shardID, err := clusterstate.RouteShardFor(
		group, routeKey, r.manifest.RouteBucketCount, r.manifest.VirtualShardCount,
	)
	if err != nil || shardID != identity.ShardID {
		return errors.New("raftstore: execution-fence compaction targets another shard")
	}
	system, err := r.requireStableActiveManifest(ctx, identity.PermitIdentity)
	if err != nil {
		return err
	}
	query := FenceLookup{
		Identity: identity, Group: group, RouteKey: routeKey, SandboxID: sandboxID,
	}
	fence, err := r.readFenceStrong(ctx, query)
	if err != nil {
		return err
	}
	if fence == nil {
		return errors.New("raftstore: execution fence is missing")
	}
	permanentlyFenced := fence.Proof.Kind == clusterstate.ProofNewerNodeEpoch ||
		fence.Proof.Kind == clusterstate.ProofExternalFence
	if !permanentlyFenced && !outboxAck.validates(*fence) {
		return errors.New("raftstore: execution fence lacks a durable final outbox ACK")
	}

	wait := time.Duration(r.config.Tuning.FenceRetentionMillis) * time.Millisecond
	started := time.Now()
	if err := waitMonotonic(ctx, started, wait); err != nil {
		return err
	}
	grant, err := r.RefreshPermit(ctx)
	if err != nil {
		return err
	}
	if grant.PermitIdentity != identity.PermitIdentity {
		return errors.New("raftstore: active permit identity changed during fence retention")
	}
	currentSystem, err := r.requireStableActiveManifest(ctx, identity.PermitIdentity)
	if err != nil {
		return err
	}
	if currentSystem.SystemEpoch != system.SystemEpoch ||
		currentSystem.ActiveManifestDigest != system.ActiveManifestDigest {
		return errors.New("raftstore: System identity changed during fence retention")
	}
	currentFence, err := r.readFenceStrong(ctx, query)
	if err != nil {
		return err
	}
	if currentFence == nil {
		return nil
	}
	if *currentFence != *fence {
		return errors.New("raftstore: execution fence changed during retention")
	}
	proofs, err := r.proveFenceAppliedEverywhere(ctx, identity, *fence)
	if err != nil {
		return err
	}
	finalSystem, err := r.requireStableActiveManifest(ctx, identity.PermitIdentity)
	if err != nil {
		return err
	}
	if finalSystem.SystemEpoch != currentSystem.SystemEpoch ||
		finalSystem.ActiveManifestDigest != currentSystem.ActiveManifestDigest {
		return errors.New("raftstore: System identity changed during fence proof collection")
	}
	retentionDigest, err := fenceRetentionProofDigest(
		*fence, r.config.Tuning.FenceRetentionMillis, outboxAck,
	)
	if err != nil {
		return err
	}
	authorization := FenceCompactionAuthorization{
		Group: group, RouteKey: routeKey, SandboxID: sandboxID,
		FenceRevision: fence.Revision.LogIndex, TerminalProofDigest: fence.Proof.ProofDigest,
		FinalOutboxWatermarkAcked:  !permanentlyFenced,
		NodeEpochPermanentlyFenced: permanentlyFenced,
		ReplicaApplied:             proofs, RetentionProofDigest: retentionDigest,
	}
	result, proposeErr := r.proposeDataRaw(ctx, DataCommand{
		Type: DataCompactFence, Identity: identity, Compaction: &authorization,
	})
	remaining, readErr := r.readFenceStrong(ctx, query)
	if readErr == nil && remaining == nil {
		return nil
	}
	if proposeErr != nil {
		return proposeErr
	}
	if result.Conflict || !result.Applied {
		return errors.New(result.Reason)
	}
	if readErr != nil {
		return readErr
	}
	return errors.New("raftstore: compacted execution fence remains visible")
}

func (r *Runtime) requireStableActiveManifest(
	ctx context.Context,
	identity PermitIdentity,
) (SystemState, error) {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if err := r.authorizeManifestState(state); err != nil {
		return SystemState{}, err
	}
	if state.Retired || state.Recovery != nil || state.Transition != nil ||
		state.ActiveManifestVersion != r.manifest.ManifestVersion ||
		state.ActiveManifestDigest != r.manifestDigest || state.Identity() != identity {
		return SystemState{}, errors.New("raftstore: execution fences compact only under a stable active manifest")
	}
	if err := r.permitCache.Authorize(identity, PermitRegistryWrite); err != nil {
		return SystemState{}, err
	}
	return state, nil
}

func (r *Runtime) readFenceStrong(ctx context.Context, query FenceLookup) (*clusterstate.ExecutionFence, error) {
	result, err := r.ReadData(ctx, DataLookup{Fence: &query})
	if err != nil {
		return nil, err
	}
	if result.Fence == nil {
		return nil, errors.New("raftstore: Dragonboat returned an invalid execution-fence lookup")
	}
	return result.Fence.Fence, nil
}

func (r *Runtime) proveFenceAppliedEverywhere(
	ctx context.Context,
	identity ShardRequestIdentity,
	fence clusterstate.ExecutionFence,
) ([]ReplicaAppliedProof, error) {
	raftShardID := DataRaftShardID(identity.ShardID)
	state, err := r.readDataStateStrong(ctx, raftShardID)
	if err != nil {
		return nil, err
	}
	desired := replicaIDsForPlacement(r.manifest.DataShards[identity.ShardID])
	if len(state.ServingEpochs) != 1 || state.ServingEpochs[0] != identity.PermitIdentity ||
		!slices.Equal(state.ReplicaIDs, desired) || len(state.PreparedReplicaIDs) != 0 {
		return nil, errors.New("raftstore: data shard is not on the exact stable manifest replica set")
	}
	placements := r.manifest.DataShards[identity.ShardID].Replicas
	proofs := make([]ReplicaAppliedProof, len(placements))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(len(placements))
	for index, placement := range placements {
		index, placement := index, placement
		group.Go(func() error {
			request := ReplicaAppliedRequest{
				ShardID: raftShardID, ReplicaID: placement.ReplicaID, MemberID: placement.MemberID,
				ManifestDigest: r.manifestDigest, MinimumAppliedIndex: fence.Revision.LogIndex,
			}
			proof, err := r.probeReplicaApplied(groupCtx, request)
			if err != nil {
				return err
			}
			proofs[index] = proof
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	sort.Slice(proofs, func(i, j int) bool { return proofs[i].ReplicaID < proofs[j].ReplicaID })
	return proofs, nil
}

func fenceRetentionProofDigest(
	fence clusterstate.ExecutionFence,
	retentionMillis uint64,
	outboxAck FenceOutboxAckEvidence,
) (string, error) {
	value := struct {
		StorageGeneration string                 `json:"storage_generation"`
		ShardID           uint32                 `json:"shard_id"`
		FenceRevision     uint64                 `json:"fence_revision"`
		TerminalDigest    string                 `json:"terminal_digest"`
		RetentionMillis   uint64                 `json:"retention_millis"`
		OutboxAck         FenceOutboxAckEvidence `json:"outbox_ack"`
	}{
		StorageGeneration: fence.StorageGeneration, ShardID: fence.Revision.ShardID,
		FenceRevision: fence.Revision.LogIndex, TerminalDigest: fence.Proof.ProofDigest,
		RetentionMillis: retentionMillis, OutboxAck: outboxAck,
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte(fenceRetentionProofDomain), raw...))
	return hex.EncodeToString(digest[:]), nil
}
