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

type commandEndpoint interface {
	SendNodeCommand(context.Context, *routesync.Command) (routesync.CmdAck, bool, error)
}

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
	ref, err := lease.Ref()
	if err != nil {
		return ref, false, err
	}
	if lease.ExpiresUnix <= h.clock().Unix() {
		return ref, false, errors.Join(ErrDispatchNotSent, ErrKeyLeaseUnavailable,
			errors.New("session: cannot install an expired key lease"))
	}
	if err := h.CheckServe(identity); err != nil {
		return ref, false, err
	}
	operation, err := h.beginKeyLeaseMutation(nodeID, nodeEpoch, dataEndpoint, ref, lease.ExpiresUnix)
	if err != nil {
		return ref, false, err
	}
	defer operation.unlock()
	if !operation.current() {
		return ref, false, errors.Join(ErrDispatchNotSent, ErrKeyLeaseSuperseded)
	}
	if lease.ExpiresUnix <= h.clock().Unix() {
		return ref, false, errors.Join(ErrDispatchNotSent, ErrKeyLeaseUnavailable,
			errors.New("session: key lease expired while waiting for the command fence"))
	}
	operation.held.leaseMu.RLock()
	installed := operation.held.keyLeases[operation.key]
	operation.held.leaseMu.RUnlock()
	if installed.Ref.KeyRevision > ref.KeyRevision ||
		(installed.Ref.KeyRevision == ref.KeyRevision && installed.Ref.RegistryAuthDigest != "" &&
			installed.Ref.RegistryAuthDigest != ref.RegistryAuthDigest) {
		return ref, false, errors.Join(ErrDispatchNotSent, ErrKeyLeaseSuperseded)
	}
	if installed.Ref == ref && installed.ExpiresUnix > lease.ExpiresUnix {
		return ref, false, errors.Join(ErrDispatchNotSent, ErrKeyLeaseSuperseded)
	}
	if err := h.CheckServe(identity); err != nil {
		return ref, false, errors.Join(ErrDispatchNotSent, err)
	}
	command := &routesync.Command{
		Kind: routesync.CmdKeyPut, NodeEpoch: nodeEpoch, SessionSeq: operation.tuple.SessionSeq,
		RegistryGeneration: identity.RegistryGeneration,
		AuthKeyFingerprint: lease.AuthKey.Fingerprint, ManifestKeyFingerprint: lease.ManifestKey.Fingerprint,
		KeyLease: &lease,
	}
	ack, sent, err := operation.endpoint.SendNodeCommand(ctx, command)
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
	operation.held.leaseMu.Lock()
	if operation.held.keyLeaseSeq[operation.key] != operation.sequence {
		operation.held.leaseMu.Unlock()
		return ref, true, ErrKeyLeaseSuperseded
	}
	operation.held.keyLeases[operation.key] = acknowledgedKeyLease{Ref: ref, ExpiresUnix: lease.ExpiresUnix}
	operation.held.leaseMu.Unlock()
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
	if err := h.CheckServe(identity); err != nil {
		return false, err
	}
	operation, err := h.beginKeyLeaseMutation(nodeID, nodeEpoch, dataEndpoint, ref, 0)
	if err != nil {
		return false, err
	}
	defer operation.unlock()
	operation.held.leaseMu.Lock()
	if operation.held.keyLeaseSeq[operation.key] != operation.sequence {
		operation.held.leaseMu.Unlock()
		return false, ErrKeyLeaseSuperseded
	}
	if installed, found := operation.held.keyLeases[operation.key]; found && installed.Ref != ref {
		operation.held.leaseMu.Unlock()
		return false, ErrKeyLeaseSuperseded
	}
	delete(operation.held.keyLeases, operation.key)
	operation.held.leaseMu.Unlock()
	if err := h.CheckServe(identity); err != nil {
		return false, errors.Join(ErrDispatchNotSent, err)
	}
	command := &routesync.Command{
		Kind: routesync.CmdKeyDrop, NodeEpoch: nodeEpoch, SessionSeq: operation.tuple.SessionSeq,
		RegistryGeneration: identity.RegistryGeneration, KeyLeaseRef: &ref,
		AuthKeyFingerprint: ref.AuthKeyFingerprint, ManifestKeyFingerprint: ref.ManifestKeyFingerprint,
	}
	ack, sent, err := operation.endpoint.SendNodeCommand(ctx, command)
	if err != nil {
		return sent, err
	}
	if !sent {
		return false, ErrSessionUnavailable
	}
	if ack.Status != routesync.AckAccepted || ack.KeyLeaseRef == nil || *ack.KeyLeaseRef != ref {
		return true, errors.New("session: node did not acknowledge the exact dropped key lease")
	}
	if !operation.current() {
		return true, ErrKeyLeaseSuperseded
	}
	return true, nil
}

func (h *Holder) HasKeyLease(nodeID string, nodeEpoch uint64, ref routesync.NodeKeyLeaseRefV1) bool {
	if ref.Validate() != nil {
		return false
	}
	h.mu.RLock()
	held := h.active[nodeID]
	if held == nil || held.registration.NodeEpoch != nodeEpoch {
		h.mu.RUnlock()
		return false
	}
	held.leaseMu.RLock()
	h.mu.RUnlock()
	installed := held.keyLeases[keyLeaseRefID(ref)]
	available := installed.Ref == ref && installed.ExpiresUnix > h.clock().Unix()
	held.leaseMu.RUnlock()
	return available
}

func dispatchKeyLeaseID(command DispatchCommand) (string, error) {
	authFingerprint := ""
	manifestFingerprint := ""
	switch command.Kind {
	case cluster.ExecutionKindSandbox:
		spec, err := cluster.ParseSandboxDispatchSpec(command.Intent.DispatchSpec)
		if err != nil {
			return "", err
		}
		authFingerprint = spec.AuthKeyFingerprint
		manifestFingerprint = spec.ManifestKeyFingerprint
	case cluster.ExecutionKindBuild:
		spec, err := cluster.ParseBuildDispatchSpec(command.Intent.DispatchSpec)
		if err != nil {
			return "", err
		}
		authFingerprint = spec.AuthKeyFingerprint
		manifestFingerprint = spec.ManifestKeyFingerprint
	default:
		return "", errors.New("session: unsupported dispatch execution kind")
	}
	if err := command.KeyLeaseRef.Validate(); err != nil ||
		command.KeyLeaseRef.Group != command.Group ||
		command.KeyLeaseRef.AuthKeyFingerprint != authFingerprint ||
		command.KeyLeaseRef.ManifestKeyFingerprint != manifestFingerprint {
		return "", errors.New("session: dispatch does not carry its exact acknowledged key lease")
	}
	return keyLeaseRefID(command.KeyLeaseRef), nil
}

func keyLeaseRef(lease routesync.NodeKeyLeaseV1) routesync.NodeKeyLeaseRefV1 {
	ref, err := lease.Ref()
	if err != nil {
		return routesync.NodeKeyLeaseRefV1{}
	}
	return ref
}

func keyLeaseRefID(ref routesync.NodeKeyLeaseRefV1) string {
	return keyLeaseID(ref.Group, ref.AuthKeyFingerprint, ref.ManifestKeyFingerprint)
}

func keyLeaseID(group, authFingerprint, manifestFingerprint string) string {
	return group + "\x00" + authFingerprint + "\x00" + manifestFingerprint
}

type acknowledgedKeyLease struct {
	Ref         routesync.NodeKeyLeaseRefV1
	ExpiresUnix int64
}

type keyLeaseMutation struct {
	held      *heldSession
	endpoint  commandEndpoint
	tuple     Tuple
	key       string
	sequence  uint64
	operation *sync.Mutex
}

func (m *keyLeaseMutation) current() bool {
	m.held.leaseMu.RLock()
	defer m.held.leaseMu.RUnlock()
	return m.held.keyLeaseSeq[m.key] == m.sequence
}

func (m *keyLeaseMutation) unlock() {
	m.held.commandMu.Unlock()
	m.operation.Unlock()
}

func (h *Holder) beginKeyLeaseMutation(
	nodeID string,
	nodeEpoch uint64,
	dataEndpoint string,
	ref routesync.NodeKeyLeaseRefV1,
	desiredExpiry int64,
) (*keyLeaseMutation, error) {
	h.mu.RLock()
	held := h.active[nodeID]
	if held == nil || held.registration.NodeEpoch != nodeEpoch || held.registration.DataEndpoint != dataEndpoint {
		h.mu.RUnlock()
		return nil, ErrSessionUnavailable
	}
	endpoint, ok := held.endpoint.(commandEndpoint)
	if !ok {
		h.mu.RUnlock()
		return nil, errors.New("session: node-link endpoint cannot mutate key leases")
	}
	tuple := held.registration.Tuple
	key := keyLeaseRefID(ref)
	held.leaseMu.Lock()
	high := held.keyLeaseHigh[key]
	if desiredExpiry > 0 {
		if high.Ref.KeyRevision > ref.KeyRevision {
			held.leaseMu.Unlock()
			h.mu.RUnlock()
			return nil, errors.Join(ErrDispatchNotSent, ErrKeyLeaseSuperseded)
		}
		if high.Ref.KeyRevision == ref.KeyRevision && high.Ref.RegistryAuthDigest != "" &&
			high.Ref.RegistryAuthDigest != ref.RegistryAuthDigest {
			held.leaseMu.Unlock()
			h.mu.RUnlock()
			return nil, errors.Join(ErrDispatchNotSent, ErrKeyLeaseConflict)
		}
		if high.Ref == ref && high.ExpiresUnix > desiredExpiry {
			held.leaseMu.Unlock()
			h.mu.RUnlock()
			return nil, errors.Join(ErrDispatchNotSent, ErrKeyLeaseSuperseded)
		}
		held.keyLeaseHigh[key] = acknowledgedKeyLease{Ref: ref, ExpiresUnix: desiredExpiry}
	} else if high.Ref.KeyRevision > ref.KeyRevision ||
		(high.Ref.KeyRevision == ref.KeyRevision && high.Ref.RegistryAuthDigest != "" && high.Ref.RegistryAuthDigest != ref.RegistryAuthDigest) {
		held.leaseMu.Unlock()
		h.mu.RUnlock()
		return nil, errors.Join(ErrDispatchNotSent, ErrKeyLeaseSuperseded)
	} else {
		held.keyLeaseHigh[key] = acknowledgedKeyLease{Ref: ref, ExpiresUnix: high.ExpiresUnix}
	}
	held.keyLeaseSeq[key]++
	sequence := held.keyLeaseSeq[key]
	held.leaseMu.Unlock()
	h.mu.RUnlock()

	operation := h.keyLeaseOperation(nodeID, ref)
	operation.Lock()
	current, err := h.lockCommandSession(nodeID, nodeEpoch, dataEndpoint, held)
	if err != nil {
		operation.Unlock()
		return nil, err
	}
	return &keyLeaseMutation{
		held: current, endpoint: endpoint, tuple: tuple, key: key, sequence: sequence, operation: operation,
	}, nil
}

func (h *Holder) keyLeaseOperation(nodeID string, ref routesync.NodeKeyLeaseRefV1) *sync.Mutex {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(nodeID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(keyLeaseRefID(ref)))
	return &h.keyOps[hash.Sum32()%uint32(len(h.keyOps))]
}
