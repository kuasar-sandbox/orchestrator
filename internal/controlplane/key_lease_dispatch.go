package controlplane

import (
	"context"
	"errors"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/coordinator"
	"github.com/kuasar-sandbox/orchestrator/internal/placer"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

type keyLeaseDispatcher struct {
	planner   PlacementPlanner
	installer NodeCommandSender
	next      coordinator.Dispatcher
}

func (d keyLeaseDispatcher) AdmitAndDispatch(
	ctx context.Context,
	command session.DispatchCommand,
) (session.DispatchReply, error) {
	expected, err := dispatchLeaseRequest(command)
	if err != nil {
		return session.DispatchReply{}, err
	}
	lease, err := d.planner.ResolveKeyLease(ctx, expected)
	if err != nil {
		return session.DispatchReply{}, err
	}
	if err := lease.Validate(); err != nil || lease.Group != expected.Group ||
		lease.AuthKey.Fingerprint != expected.AuthKeyFingerprint ||
		lease.ManifestKey.Fingerprint != expected.ManifestKeyFingerprint {
		return session.DispatchReply{}, errors.New("controlplane: Provider returned a mismatched key lease")
	}
	if lease.ExpiresUnix <= time.Now().Unix() {
		return session.DispatchReply{}, errors.New("controlplane: Provider returned an expired key lease")
	}
	want, err := lease.Ref()
	if err != nil {
		return session.DispatchReply{}, err
	}
	ack, sent, err := d.installer.InstallKeyLease(
		ctx, command.ServeIdentity, command.NodeID, command.NodeEpoch, command.DataEndpoint, lease,
	)
	if err != nil {
		return session.DispatchReply{}, err
	}
	if !sent || ack != want {
		return session.DispatchReply{}, errors.New("controlplane: selected node did not durably acknowledge the exact key lease")
	}
	command.KeyLeaseRef = want
	return d.next.AdmitAndDispatch(ctx, command)
}

func dispatchLeaseRequest(command session.DispatchCommand) (placer.KeyLeaseRequest, error) {
	request := placer.KeyLeaseRequest{Group: command.Group}
	switch command.Kind {
	case clusterstate.ExecutionKindSandbox:
		spec, err := clusterstate.ParseSandboxDispatchSpec(command.Intent.DispatchSpec)
		if err != nil {
			return request, err
		}
		request.AuthKeyFingerprint = spec.AuthKeyFingerprint
		request.ManifestKeyFingerprint = spec.ManifestKeyFingerprint
	case clusterstate.ExecutionKindBuild:
		spec, err := clusterstate.ParseBuildDispatchSpec(command.Intent.DispatchSpec)
		if err != nil {
			return request, err
		}
		request.AuthKeyFingerprint = spec.AuthKeyFingerprint
		request.ManifestKeyFingerprint = spec.ManifestKeyFingerprint
	default:
		return request, errors.New("controlplane: unsupported dispatch execution kind")
	}
	return request, nil
}
