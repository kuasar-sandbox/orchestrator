package registry

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// The command ACK and the endpoint lookup are independent owner calls. Keep
// registration accepted if only the later lookup temporarily fails.
type endpointLossAfterACKOwner struct {
	*recordingNodeOwner
	afterACK    atomic.Bool
	recovered   atomic.Bool
	endpointErr error
}

func (o *endpointLossAfterACKOwner) Runtime(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	if o.afterACK.Load() && !o.recovered.Load() {
		return nil, false, o.endpointErr
	}
	return o.recordingNodeOwner.Runtime(ctx, nodeID)
}

func (o *endpointLossAfterACKOwner) SendCommandAndWait(ctx context.Context, nodeID string, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error) {
	ack, err := o.recordingNodeOwner.SendCommandAndWait(ctx, nodeID, cmd, timeout)
	if err == nil && ack != nil && ack.Status == routesync.AckAccepted {
		o.afterACK.Store(true)
	}
	return ack, err
}

func TestReserveBuildEndpointLossAfterACKPreservesAcceptedIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"profile-quorum", metadata.ErrUnavailable},
		{"node-disconnected", ErrNodeGone},
		{"profile-missing", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			reg := testReg(t)
			seedTestNode(t, reg.stores, "n1")
			reg.SetPlacer(placementWithToken("n1"))
			target := &types.BuildTarget{Kind: types.BuildTargetSandbox}
			owner := &endpointLossAfterACKOwner{
				recordingNodeOwner: &recordingNodeOwner{
					runtime: map[string]*NodeRecord{"n1": {NodeID: "n1", APIEndpoint: "node-api:7443"}},
					ack:     &routesync.CmdAck{Status: routesync.AckAccepted, BuildRegister: &routesync.BuildRegisterResult{Target: target}},
				},
				endpointErr: tc.err,
			}
			reg.SetNodeOwner(owner)
			req := BuildReserveReq{Group: "/g", BuildID: "endpoint-recovery", TemplateID: "transient-endpoint-recovery", Profile: types.ProfileBare, Resources: testWireBuildResources()}
			registered, err := reg.ReserveBuild(ctx, req)
			if err != nil || registered == nil {
				t.Fatalf("accepted registration lost: result=%+v err=%v", registered, err)
			}
			if registered.BuildID != req.BuildID || registered.TemplateID != req.TemplateID || registered.NodeID != "n1" || registered.APIEndpoint != "" {
				t.Fatalf("accepted identity/temporary endpoint: %+v", registered)
			}
			stored, found, err := reg.stores.GetBuildInGroup(ctx, req.Group, req.BuildID)
			if err != nil || !found || stored.State != BuildRegistered || !stored.RegistrationTargetSet || !sameBuildTarget(stored.RegistrationTarget, target) {
				t.Fatalf("accepted durable binding: stored=%+v found=%v err=%v", stored, found, err)
			}
			if got, found, err := reg.ResolveBuild(ctx, req.Group, req.BuildID); err == nil || found || got != nil {
				t.Fatalf("unavailable endpoint must fail lookup: got=%+v found=%v err=%v", got, found, err)
			}
			owner.recovered.Store(true)
			got, found, err := reg.ResolveBuild(ctx, req.Group, req.BuildID)
			if err != nil || !found || got == nil || got.BuildID != req.BuildID || got.TemplateID != req.TemplateID || got.NodeID != "n1" || got.APIEndpoint != "node-api:7443" || !sameBuildTarget(got.Target, target) {
				t.Fatalf("recovered original binding: got=%+v found=%v err=%v", got, found, err)
			}
			replay, err := reg.ReserveBuild(ctx, req)
			if err != nil || replay == nil || replay.BuildID != req.BuildID || replay.APIEndpoint != "node-api:7443" || len(owner.commands) != 1 {
				t.Fatalf("replay duplicated accepted registration: result=%+v commands=%d err=%v", replay, len(owner.commands), err)
			}
			// The error is injected only after the node ACK, never during the
			// initial registration admission/capability check.
			if len(owner.commands) != 1 || owner.commands[0].Kind != routesync.CmdBuildRegister || !owner.afterACK.Load() {
				t.Fatal("endpoint failure did not follow a build_register ACK")
			}
		})
	}
}
