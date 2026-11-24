package registry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type groupCommandOwner struct {
	recordingNodeOwner
	call func(context.Context, string, *routesync.Command) (*routesync.CmdAck, error)
}

func (o *groupCommandOwner) SendCommandAndWait(ctx context.Context, node string, cmd *routesync.Command, _ time.Duration) (*routesync.CmdAck, error) {
	o.commands = append(o.commands, cmd)
	return o.call(ctx, node, cmd)
}

func TestGroupBuildUsesPrivateNodeContextAndSurvivesWithdrawal(t *testing.T) {
	r := New(newTestStores(t), time.Second, nil)
	key, entry := registryGroupEntry(t)
	reg := reportRegistryGroup(t, r, "node", nil, nodegroup.Support{Group: "g", Version: 1, Entries: []nodegroup.Entry{entry}})
	o := &groupCommandOwner{}
	o.call = func(ctx context.Context, node string, cmd *routesync.Command) (*routesync.CmdAck, error) {
		if got, _ := ctx.Value(nodeRegistrationContextKey{}).(metadata.Registration); got != reg {
			t.Fatal("command lost selected registration")
		}
		if cmd.GroupAPIKey != key || cmd.ExpectedGroupVersion != 1 || cmd.ImageRepo != "" || cmd.RegistryAuth != "" {
			t.Fatal("Build did not delegate private inputs to Node Group")
		}
		return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted, BuildRegister: &routesync.BuildRegisterResult{}}, nil
	}
	r.SetNodeOwner(o)
	req := BuildReserveReq{APIKey: key, Group: "g", BuildID: "bld-group", Profile: types.ProfileBare, Resources: testWireBuildResources()}
	result, err := r.ReserveBuild(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.stores.groups.ApplyGroupSupport(t.Context(), reg, 2, nil, "g"); err != nil {
		t.Fatal(err)
	}
	again, err := r.ReserveBuild(t.Context(), req)
	if err != nil || again.TemplateID != result.TemplateID || len(o.commands) != 1 {
		t.Fatalf("durable Build lost after Group withdrawal: %+v %v", again, err)
	}
	req.BuildID = "bld-new"
	if _, err := r.ReserveBuild(t.Context(), req); !errors.Is(err, ErrNoNode) && !errors.Is(err, ErrReserveForbidden) {
		t.Fatalf("withdrawn Group accepted new Build: %v", err)
	}
}

func TestGroupCreateRejectionKeepsAmbiguousOwner(t *testing.T) {
	for _, precise := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary_409", true: "precise_conflict"}[precise], func(t *testing.T) {
			r := New(newTestStores(t), 20*time.Millisecond, nil)
			key, entry := registryGroupEntry(t)
			reportRegistryGroup(t, r, "node", nil, nodegroup.Support{Group: "g", Version: 1, Entries: []nodegroup.Entry{entry}, DefaultContext: &nodegroup.DefaultContext{TemplateID: testTemplateRef}})
			o := &groupCommandOwner{}
			o.call = func(_ context.Context, _ string, cmd *routesync.Command) (*routesync.CmdAck, error) {
				ack := &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, HTTPStatus: 409}
				if precise {
					ack.ErrorCode = routesync.ErrorGroupVersionConflict
				}
				return ack, nil
			}
			r.SetNodeOwner(o)
			_, err := r.ReserveSandbox(t.Context(), SandboxReserveRequest{Operation: ReserveCreate, Group: "g", RouteKey: "rk", APIKey: key})
			if err == nil || precise && !errors.Is(err, ErrReserveConflict) {
				t.Fatalf("wrong result: %v", err)
			}
			if len(o.commands) != 1 {
				t.Fatalf("rejection redispatched: %d", len(o.commands))
			}
			owner, err := r.stores.ownership.ReadSandboxOwner(t.Context(), "node", o.commands[0].SID)
			if err != nil || owner.Exists == precise {
				t.Fatalf("wrong cleanup responsibility: %v %v", owner.Exists, err)
			}
		})
	}
}
