package raftstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sort"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"golang.org/x/sync/errgroup"
)

const (
	fenceRetentionProofDomain = "kuasar-fence-retention-proof-v1\x00"
	nodeEpochFenceProofDomain = "kuasar-node-epoch-fence-proof-v1\x00"
)

type FenceOutboxAckEvidence struct {
	AckedWatermark uint64 `json:"acked_watermark"`
	ProofDigest    string `json:"proof_digest"`
}

type FenceOutboxAckRequest struct {
	Group                string
	RouteKey             string
	SandboxID            string
	NodeID               string
	NodeEpoch            uint64
	RegistryGeneration   string
	BindingDigest        string
	FinalOutboxWatermark uint64
}

type FenceOutboxAckVerifier interface {
	VerifyFenceOutboxAck(context.Context, FenceOutboxAckRequest) (FenceOutboxAckEvidence, error)
}

type FenceOutboxAckVerifierFunc func(context.Context, FenceOutboxAckRequest) (FenceOutboxAckEvidence, error)

func (f FenceOutboxAckVerifierFunc) VerifyFenceOutboxAck(
	ctx context.Context,
	request FenceOutboxAckRequest,
) (FenceOutboxAckEvidence, error) {
	return f(ctx, request)
}

func (r *Runtime) SetOutboxAckVerifier(verifier FenceOutboxAckVerifier) error {
	if r == nil || verifier == nil {
		return errors.New("raftstore: trusted final outbox ACK verifier is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.outboxAckVerifier != nil {
		return errors.New("raftstore: trusted final outbox ACK verifier is already configured")
	}
	r.outboxAckVerifier = verifier
	return nil
}

func (r *Runtime) trustedOutboxAckVerifier() FenceOutboxAckVerifier {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.outboxAckVerifier
}

func (e FenceOutboxAckEvidence) validates(fence clusterstate.ExecutionFence) bool {
	return fence.FinalOutboxWatermark >= fence.LastEventSeq &&
		e.AckedWatermark == fence.FinalOutboxWatermark &&
		isSHA256(e.ProofDigest)
}

// CompactExecutionFence waits the configured retention period from a fresh
// strong read, proves every exact registryLayout voter has applied the fence, and
// then submits the only authorized compaction command path.
func (r *Runtime) CompactExecutionFence(
	ctx context.Context,
	identity ShardRequestIdentity,
	group string,
	routeKey string,
	sandboxID string,
) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if group == "" || routeKey == "" || sandboxID == "" {
		return errors.New("raftstore: execution-fence compaction identity is incomplete")
	}
	_, shardID, err := clusterstate.RouteShardFor(
		group, routeKey, r.registryLayout.RouteBucketCount, r.registryLayout.VirtualShardCount,
	)
	if err != nil || shardID != identity.ShardID {
		return errors.New("raftstore: execution-fence compaction targets another shard")
	}
	system, err := r.requireStableActiveRegistryLayout(ctx, identity.PermitIdentity)
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
	placementFailure := fence.PlacementFailure != nil
	proofPermanentlyFenced := fence.Proof.Kind == clusterstate.ProofNewerNodeEpoch ||
		fence.Proof.Kind == clusterstate.ProofExternalFence
	var outboxAck FenceOutboxAckEvidence
	if !placementFailure && !proofPermanentlyFenced {
		verifier := r.trustedOutboxAckVerifier()
		if verifier == nil {
			return errors.New("raftstore: trusted final outbox ACK verifier is unavailable")
		}
		outboxAck, err = verifier.VerifyFenceOutboxAck(ctx, FenceOutboxAckRequest{
			Group: fence.Group, RouteKey: fence.RouteKey, SandboxID: fence.SandboxID,
			NodeID: fence.NodeID, NodeEpoch: fence.NodeEpoch, RegistryGeneration: fence.RegistryGeneration,
			BindingDigest: fence.BindingDigest, FinalOutboxWatermark: fence.FinalOutboxWatermark,
		})
		if err != nil {
			return err
		}
		if !outboxAck.validates(*fence) {
			return errors.New("raftstore: trusted verifier did not prove the exact final outbox ACK")
		}
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
	currentSystem, err := r.requireStableActiveRegistryLayout(ctx, identity.PermitIdentity)
	if err != nil {
		return err
	}
	if currentSystem.SystemEpoch != system.SystemEpoch ||
		currentSystem.ActiveRegistryLayoutDigest != system.ActiveRegistryLayoutDigest {
		return errors.New("raftstore: System identity changed during fence retention")
	}
	currentFence, err := r.readFenceStrong(ctx, query)
	if err != nil {
		return err
	}
	if currentFence == nil {
		return nil
	}
	if !reflect.DeepEqual(*currentFence, *fence) {
		return errors.New("raftstore: execution fence changed during retention")
	}
	proofs, err := r.proveFenceAppliedEverywhere(ctx, identity, *fence)
	if err != nil {
		return err
	}
	finalSystem, err := r.requireStableActiveRegistryLayout(ctx, identity.PermitIdentity)
	if err != nil {
		return err
	}
	if finalSystem.SystemEpoch != currentSystem.SystemEpoch ||
		finalSystem.ActiveRegistryLayoutDigest != currentSystem.ActiveRegistryLayoutDigest {
		return errors.New("raftstore: System identity changed during fence proof collection")
	}
	retentionDigest, err := fenceRetentionProofDigest(
		*fence, r.config.Tuning.FenceRetentionMillis, outboxAck, nil,
	)
	if err != nil {
		return err
	}
	authorization := FenceCompactionAuthorization{
		Group: group, RouteKey: routeKey, SandboxID: sandboxID,
		FenceRevision: fence.Revision.LogIndex, TerminalProofDigest: fence.ProofDigest(),
		FinalOutboxWatermarkAcked: placementFailure ||
			!proofPermanentlyFenced && outboxAck.validates(*fence),
		ReplicaApplied: proofs, RetentionProofDigest: retentionDigest,
	}
	result, submitted, proposeErr := r.proposeDataRaw(ctx, DataCommand{
		Type: DataCompactFence, Identity: identity, Compaction: &authorization,
	})
	if proposeErr != nil && !submitted {
		return proposeErr
	}
	resolveContext, cancelResolve := ambiguityResolutionContext(ctx)
	defer cancelResolve()
	remaining, readErr := r.readFenceStrong(resolveContext, query)
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

func (r *Runtime) requireStableActiveRegistryLayout(
	ctx context.Context,
	identity PermitIdentity,
) (SystemState, error) {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if err := r.authorizeRegistryLayoutState(state); err != nil {
		return SystemState{}, err
	}
	if state.Retired || state.Recovery != nil || state.Transition != nil ||
		state.ActiveRegistryLayoutVersion != r.registryLayout.RegistryLayoutVersion ||
		state.ActiveRegistryLayoutDigest != r.registryLayoutDigest || state.Identity() != identity {
		return SystemState{}, errors.New("raftstore: execution fences compact only under a stable active registryLayout")
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
	desired := replicaIDsForPlacement(r.registryLayout.DataShards[identity.ShardID])
	if len(state.ServingEpochs) != 1 || state.ServingEpochs[0] != identity.PermitIdentity ||
		!slices.Equal(state.ReplicaIDs, desired) || len(state.PreparedReplicaIDs) != 0 {
		return nil, errors.New("raftstore: data shard is not on the exact stable registryLayout replica set")
	}
	placements := r.registryLayout.DataShards[identity.ShardID].Replicas
	proofs := make([]ReplicaAppliedProof, len(placements))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(len(placements))
	for index, placement := range placements {
		index, placement := index, placement
		group.Go(func() error {
			request := ReplicaAppliedRequest{
				ShardID: raftShardID, ReplicaID: placement.ReplicaID, MemberID: placement.MemberID,
				RegistryLayoutDigest: r.registryLayoutDigest, MinimumAppliedIndex: fence.Revision.LogIndex,
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
	nodeEpochFence *NodeEpochFenceEvidence,
) (string, error) {
	value := struct {
		RegistryGeneration string                  `json:"registry_generation"`
		ShardID            uint32                  `json:"shard_id"`
		FenceRevision      uint64                  `json:"fence_revision"`
		TerminalDigest     string                  `json:"terminal_digest"`
		RetentionMillis    uint64                  `json:"retention_millis"`
		OutboxAck          FenceOutboxAckEvidence  `json:"outbox_ack"`
		NodeEpochFence     *NodeEpochFenceEvidence `json:"node_epoch_fence,omitempty"`
	}{
		RegistryGeneration: fence.RegistryGeneration, ShardID: fence.Revision.ShardID,
		FenceRevision: fence.Revision.LogIndex, TerminalDigest: fence.ProofDigest(),
		RetentionMillis: retentionMillis, OutboxAck: outboxAck, NodeEpochFence: nodeEpochFence,
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte(fenceRetentionProofDomain), raw...))
	return hex.EncodeToString(digest[:]), nil
}

func (e NodeEpochFenceEvidence) validates(identity PermitIdentity, fence clusterstate.ExecutionFence) bool {
	if e.PermitIdentity != identity || e.SystemCommitIndex == 0 || e.EnrollmentID == "" ||
		e.EnrollmentCommitIndex == 0 || e.EnrollmentCommitIndex > e.SystemCommitIndex ||
		e.NodeID != fence.NodeID || e.FencedNodeEpoch != fence.NodeEpoch ||
		e.ObservedNodeEpoch <= e.FencedNodeEpoch || e.ObservedDataEndpoint == "" || !isSHA256(e.ProofDigest) {
		return false
	}
	digest, err := nodeEpochFenceEvidenceDigest(e)
	return err == nil && digest == e.ProofDigest
}

func nodeEpochFenceEvidenceDigest(evidence NodeEpochFenceEvidence) (string, error) {
	evidence.ProofDigest = ""
	raw, err := json.Marshal(evidence)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte(nodeEpochFenceProofDomain), raw...))
	return hex.EncodeToString(digest[:]), nil
}
