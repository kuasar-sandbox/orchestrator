package session

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// InstallKeyLease sends one complete Provider-owned key bundle through the
// exact current node session. sent=true with an error is ambiguous and callers
// must retry this same lease rather than authorizing dispatch or choosing a
// different key bundle.
func (h *Holder) InstallKeyLease(
	ctx context.Context,
	identity ServeIdentity,
	nodeID string,
	nodeEpoch uint64,
	dataEndpoint string,
	lease routesync.NodeKeyLeaseV1,
) (routesync.NodeKeyLeaseRefV1, bool, error) {
	ref := keyLeaseRef(lease)
	if err := lease.Validate(); err != nil {
		return ref, false, err
	}
	if lease.ExpiresUnix <= h.clock().Unix() {
		return ref, false, errors.New("session: cannot install an expired key lease")
	}
	if err := h.CheckServe(identity, PermitDispatch); err != nil {
		return ref, false, err
	}
	held, endpoint, tuple, operation, err := h.lockKeyLeaseOperation(nodeID, nodeEpoch, dataEndpoint, ref)
	if err != nil {
		return ref, false, err
	}
	defer operation.Unlock()
	if err := h.authorizeNodeSession(identity, held.registration); err != nil {
		return ref, false, err
	}
	command := &routesync.Command{
		Kind: routesync.CmdKeyPut, NodeEpoch: nodeEpoch, SessionSeq: tuple.SessionSeq,
		RegistryGeneration: identity.RegistryGeneration,
		AuthKeyFingerprint: lease.AuthKey.Fingerprint, ManifestKeyFingerprint: lease.ManifestKey.Fingerprint,
		KeyLease: &lease,
	}
	ack, sent, err := endpoint.SendNodeCommand(ctx, command)
	if err != nil {
		return ref, sent, err
	}
	if !sent {
		return ref, false, ErrSessionUnavailable
	}
	if ack.Status != routesync.AckAccepted {
		return ref, true, fmt.Errorf("session: node rejected key lease: %s", ack.Reason)
	}
	if ack.KeyLeaseRef == nil || ack.KeyLeaseRef.Validate() != nil || *ack.KeyLeaseRef != ref {
		return ref, true, errors.New("session: node ACK did not prove the exact key lease")
	}
	h.mu.Lock()
	current := h.active[nodeID]
	if current != held || current.registration.Tuple.Compare(tuple) != 0 || current.registration.DataEndpoint != dataEndpoint {
		h.mu.Unlock()
		return ref, true, ErrSessionUnavailable
	}
	current.keyLeases[keyLeaseRefID(ref)] = lease.ExpiresUnix
	h.mu.Unlock()
	return ref, true, nil
}

// DropKeyLease is an eager cleanup hint. Failure leaves the local ACK unusable,
// while the node's lease TTL remains the authoritative cleanup mechanism.
func (h *Holder) DropKeyLease(
	ctx context.Context,
	identity ServeIdentity,
	nodeID string,
	nodeEpoch uint64,
	dataEndpoint string,
	ref routesync.NodeKeyLeaseRefV1,
) (bool, error) {
	if err := ref.Validate(); err != nil {
		return false, err
	}
	if err := h.CheckServe(identity, PermitDispatch); err != nil {
		return false, err
	}
	held, endpoint, tuple, operation, err := h.lockKeyLeaseOperation(nodeID, nodeEpoch, dataEndpoint, ref)
	if err != nil {
		return false, err
	}
	defer operation.Unlock()
	h.mu.Lock()
	if h.active[nodeID] != held || held.registration.Tuple.Compare(tuple) != 0 ||
		held.registration.DataEndpoint != dataEndpoint {
		h.mu.Unlock()
		return false, ErrSessionUnavailable
	}
	delete(held.keyLeases, keyLeaseRefID(ref))
	h.mu.Unlock()
	if err := h.authorizeNodeSession(identity, held.registration); err != nil {
		return false, err
	}
	command := &routesync.Command{
		Kind: routesync.CmdKeyDrop, NodeEpoch: nodeEpoch, SessionSeq: tuple.SessionSeq,
		RegistryGeneration: identity.RegistryGeneration, KeyLeaseRef: &ref,
		AuthKeyFingerprint: ref.AuthKeyFingerprint, ManifestKeyFingerprint: ref.ManifestKeyFingerprint,
	}
	ack, sent, err := endpoint.SendNodeCommand(ctx, command)
	if err != nil || !sent {
		return sent, err
	}
	if ack.Status != routesync.AckAccepted || ack.KeyLeaseRef == nil || *ack.KeyLeaseRef != ref {
		return true, errors.New("session: node did not acknowledge the exact dropped key lease")
	}
	return true, nil
}

func (h *Holder) HasKeyLease(nodeID string, nodeEpoch uint64, ref routesync.NodeKeyLeaseRefV1) bool {
	if ref.Validate() != nil {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	held := h.active[nodeID]
	return held != nil && held.registration.NodeEpoch == nodeEpoch &&
		held.keyLeases[keyLeaseRefID(ref)] > h.clock().Unix()
}

func dispatchKeyLeaseRef(command DispatchCommand) (routesync.NodeKeyLeaseRefV1, error) {
	ref := routesync.NodeKeyLeaseRefV1{Version: routesync.NodeKeyLeaseVersionV1, Group: command.Group}
	switch command.Kind {
	case cluster.ExecutionKindSandbox:
		spec, err := cluster.ParseSandboxDispatchSpec(command.Intent.DispatchSpec)
		if err != nil {
			return ref, err
		}
		ref.AuthKeyFingerprint = spec.AuthKeyFingerprint
		ref.ManifestKeyFingerprint = spec.ManifestKeyFingerprint
	case cluster.ExecutionKindBuild:
		spec, err := cluster.ParseBuildDispatchSpec(command.Intent.DispatchSpec)
		if err != nil {
			return ref, err
		}
		ref.AuthKeyFingerprint = spec.AuthKeyFingerprint
		ref.ManifestKeyFingerprint = spec.ManifestKeyFingerprint
	default:
		return ref, errors.New("session: unsupported dispatch execution kind")
	}
	return ref, ref.Validate()
}

func keyLeaseRef(lease routesync.NodeKeyLeaseV1) routesync.NodeKeyLeaseRefV1 {
	return routesync.NodeKeyLeaseRefV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: lease.Group,
		AuthKeyFingerprint: lease.AuthKey.Fingerprint, ManifestKeyFingerprint: lease.ManifestKey.Fingerprint,
	}
}

func keyLeaseRefID(ref routesync.NodeKeyLeaseRefV1) string {
	return ref.Group + "\x00" + ref.AuthKeyFingerprint + "\x00" + ref.ManifestKeyFingerprint
}

func (h *Holder) lockKeyLeaseOperation(
	nodeID string,
	nodeEpoch uint64,
	dataEndpoint string,
	ref routesync.NodeKeyLeaseRefV1,
) (*heldSession, commandEndpoint, Tuple, *sync.Mutex, error) {
	operation := h.keyLeaseOperation(nodeID, ref)
	operation.Lock()
	h.mu.RLock()
	held := h.active[nodeID]
	if held == nil || held.registration.NodeEpoch != nodeEpoch || held.registration.DataEndpoint != dataEndpoint {
		h.mu.RUnlock()
		operation.Unlock()
		return nil, nil, Tuple{}, nil, ErrSessionUnavailable
	}
	endpoint, ok := held.endpoint.(commandEndpoint)
	if !ok {
		h.mu.RUnlock()
		operation.Unlock()
		return nil, nil, Tuple{}, nil, errors.New("session: node-link endpoint cannot mutate key leases")
	}
	tuple := held.registration.Tuple
	h.mu.RUnlock()
	return held, endpoint, tuple, operation, nil
}

func (h *Holder) keyLeaseOperation(nodeID string, ref routesync.NodeKeyLeaseRefV1) *sync.Mutex {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(nodeID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(keyLeaseRefID(ref)))
	return &h.keyOps[hash.Sum32()%uint32(len(h.keyOps))]
}
