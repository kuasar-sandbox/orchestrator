package controlplane

import (
	"context"
	"errors"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/coordinator"
	"github.com/kuasar-sandbox/orchestrator/internal/placer"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
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
	ref, err := dispatchLeaseRef(command)
	if err != nil {
		return session.DispatchReply{}, err
	}
	lease, err := d.planner.ResolveKeyLease(ctx, placer.KeyLeaseRequest{
		Group: command.Group, AuthKeyFingerprint: ref.AuthKeyFingerprint,
		ManifestKeyFingerprint: ref.ManifestKeyFingerprint,
	})
	if err != nil {
		return session.DispatchReply{}, err
	}
	if lease.ExpiresUnix <= time.Now().Unix() {
		return session.DispatchReply{}, errors.New("controlplane: Provider returned an expired key lease")
	}
	ack, sent, err := d.installer.InstallKeyLease(
		ctx, command.ServeIdentity, command.NodeID, command.NodeEpoch, command.DataEndpoint, lease,
	)
	if err != nil {
		return session.DispatchReply{}, err
	}
	if !sent || ack != ref {
		return session.DispatchReply{}, errors.New("controlplane: selected node did not durably acknowledge the exact key lease")
	}
	return d.next.AdmitAndDispatch(ctx, command)
}

func dispatchLeaseRef(command session.DispatchCommand) (routesync.NodeKeyLeaseRefV1, error) {
	ref := routesync.NodeKeyLeaseRefV1{Version: routesync.NodeKeyLeaseVersionV1, Group: command.Group}
	switch command.Kind {
	case clusterstate.ExecutionKindSandbox:
		spec, err := clusterstate.ParseSandboxDispatchSpec(command.Intent.DispatchSpec)
		if err != nil {
			return ref, err
		}
		ref.AuthKeyFingerprint = spec.AuthKeyFingerprint
		ref.ManifestKeyFingerprint = spec.ManifestKeyFingerprint
	case clusterstate.ExecutionKindBuild:
		spec, err := clusterstate.ParseBuildDispatchSpec(command.Intent.DispatchSpec)
		if err != nil {
			return ref, err
		}
		ref.AuthKeyFingerprint = spec.AuthKeyFingerprint
		ref.ManifestKeyFingerprint = spec.ManifestKeyFingerprint
	default:
		return ref, errors.New("controlplane: unsupported dispatch execution kind")
	}
	return ref, ref.Validate()
}
