package orch

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestPrepareClusterExecSessionImportsAndMintsStableIDToken(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	blockClusterExecLaunch(t, fixture)
	cmd := fixture.command("stable-g1", fixture.token)
	cmd.CmdID = "exec-import"
	cmd.Kind = routesync.CmdExecSession
	cmd.TTLSeconds = 37
	cmd.ExecConditions = []string{"request.cwd == '/workspace'"}
	const preflight = int64(1_800_000_000)
	const signing = int64(1_800_000_100)

	sb, result, err := fixture.o.prepareClusterExecSession(
		context.Background(), cmd, scriptedUnixClock(t, preflight, signing),
	)
	if err != nil {
		t.Fatal(err)
	}
	if sb == nil || sb.ID != cmd.SID || sb.State != types.StateStarting || sb.Cluster == nil ||
		sb.Cluster.Group != cmd.Cluster.Group || sb.Cluster.RouteKey != cmd.Cluster.RouteKey ||
		sb.StableID() != cmd.Cluster.StableID {
		t.Fatalf("prepared sandbox = %+v", sb)
	}
	if result == nil || result.ExecAccessToken == "" {
		t.Fatalf("exec-session result = %+v", result)
	}
	claims, err := keys.ParseAndVerifyExecAccessToken(
		result.ExecAccessToken, sb.ServiceSecret, sb.StableID(), time.Unix(signing+36, 0),
	)
	if err != nil || len(claims.Conditions) != 1 || claims.Conditions[0] != cmd.ExecConditions[0] {
		t.Fatalf("token conditions = %+v, %v", claims, err)
	}
	if err := keys.VerifyExecAccessToken(result.ExecAccessToken, sb.ServiceSecret, sb.StableID(), time.Unix(signing+36, 0)); err != nil {
		t.Fatalf("token before expiry: %v", err)
	}
	if err := keys.VerifyExecAccessToken(result.ExecAccessToken, sb.ServiceSecret, sb.StableID(), time.Unix(signing+37, 0)); err == nil {
		t.Fatal("token accepted at expiry")
	}
	if err := keys.VerifyExecAccessToken(result.ExecAccessToken, sb.ServiceSecret, sb.ID, time.Unix(signing, 0)); err == nil {
		t.Fatal("token accepted NodeSandboxID instead of StableID")
	}
}

func TestPrepareClusterExecSessionMintsPerCommandLongLivedTokens(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	blockClusterExecLaunch(t, fixture)
	first := fixture.command("stable-g1", fixture.token)
	first.CmdID = "exec-first"
	first.Kind = routesync.CmdExecSession
	sb, firstResult, err := fixture.o.prepareClusterExecSession(context.Background(), first, func() int64 { return 1 })
	if err != nil {
		t.Fatal(err)
	}
	second := fixture.command(first.SID, "malformed-token-is-ignored-for-existing-target")
	second.CmdID = "exec-second"
	second.Kind = routesync.CmdExecSession
	_, secondResult, err := fixture.o.prepareClusterExecSession(context.Background(), second, func() int64 { return 1 })
	if err != nil {
		t.Fatal(err)
	}
	if firstResult.ExecAccessToken == secondResult.ExecAccessToken {
		t.Fatal("different commands minted the same exec access token")
	}
	for _, token := range []string{firstResult.ExecAccessToken, secondResult.ExecAccessToken} {
		if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.StableID(), time.Unix(math.MaxInt64, 0)); err != nil {
			t.Fatalf("long-lived token: %v", err)
		}
	}
}

func TestHandleClusterExecSessionReturnsBeforeAsynchronousResume(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	blocker := &blockingClusterConnectVS{started: make(chan struct{}), returned: make(chan struct{})}
	fixture.o.vs = blocker
	asyncCtx, cancel := context.WithCancel(context.Background())
	fixture.o.SetClusterContext(asyncCtx)
	t.Cleanup(func() {
		cancel()
		select {
		case <-blocker.returned:
		case <-time.After(2 * time.Second):
			t.Error("asynchronous exec-session resume did not stop")
		}
	})

	cmd := fixture.command("stable-g1", fixture.token)
	cmd.CmdID = "exec-async"
	cmd.Kind = routesync.CmdExecSession
	ack := fixture.o.HandleCommand(context.Background(), cmd)
	if ack.Status != routesync.AckAccepted || ack.ExecSession == nil || ack.ExecSession.ExecAccessToken == "" || ack.Connect != nil {
		t.Fatalf("exec-session ack = %+v", ack)
	}
	stored, err := fixture.o.st.Get(context.Background(), cmd.SID)
	if err != nil || stored == nil || stored.State != types.StateStarting || stored.RunID != "" {
		t.Fatalf("synchronously imported target = %+v, %v", stored, err)
	}
	if err := keys.VerifyExecAccessToken(ack.ExecSession.ExecAccessToken, stored.ServiceSecret, stored.StableID(), time.Now()); err != nil {
		t.Fatalf("ack token = %v", err)
	}
	select {
	case <-blocker.started:
	case <-time.After(2 * time.Second):
		t.Fatal("accepted exec session did not schedule asynchronous resume")
	}
	cancel()
	select {
	case <-blocker.returned:
	case <-time.After(2 * time.Second):
		t.Fatal("asynchronous exec-session resume did not observe cancellation")
	}
}

func TestClusterExecSessionRejectsForeignFieldsBeforeSideEffects(t *testing.T) {
	tests := map[string]func(*routesync.Command){
		"missing command id": func(cmd *routesync.Command) { cmd.CmdID = "" },
		"negative ttl":       func(cmd *routesync.Command) { cmd.TTLSeconds = -1 },
		"connect timeout":    func(cmd *routesync.Command) { cmd.TimeoutSeconds = 1 },
		"create template":    func(cmd *routesync.Command) { cmd.TemplateRef = "manifest://template" },
		"create config":      func(cmd *routesync.Command) { cmd.Config = map[string]string{"x": "y"} },
		"raw api secret":     func(cmd *routesync.Command) { cmd.APISecret = strings.Repeat("1", 64) },
		"build id":           func(cmd *routesync.Command) { cmd.BuildID = "build-1" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newClusterConnectFixture(t)
			cmd := fixture.command("stable-g1", fixture.token)
			cmd.CmdID = "exec-invalid"
			cmd.Kind = routesync.CmdExecSession
			mutate(cmd)
			ack := fixture.o.HandleCommand(context.Background(), cmd)
			if ack.Status != routesync.AckRejected || ack.ExecSession != nil || ack.Connect != nil {
				t.Fatalf("invalid envelope ack = %+v", ack)
			}
			if sb, err := fixture.o.st.Get(context.Background(), cmd.SID); err != nil || sb != nil {
				t.Fatalf("invalid envelope inserted target: %+v, %v", sb, err)
			}
		})
	}
}

func TestClusterExecSessionRejectsTTLOverflowWithoutResume(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	cmd := fixture.command("stable-g1", fixture.token)
	cmd.CmdID = "exec-overflow"
	cmd.Kind = routesync.CmdExecSession
	cmd.TTLSeconds = math.MaxInt64
	if _, _, err := fixture.o.prepareClusterExecSession(context.Background(), cmd, func() int64 { return 1 }); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("overflow ttl error = %v, want bad request", err)
	}
	stored, err := fixture.o.st.Get(context.Background(), cmd.SID)
	if err != nil || stored != nil {
		t.Fatalf("overflow ttl inserted target = %+v, %v", stored, err)
	}
}

func TestClusterExecSessionRejectsInvalidConditionsBeforeImportOrResume(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	cmd := fixture.command("stable-g1", fixture.token)
	cmd.CmdID = "exec-invalid-condition"
	cmd.Kind = routesync.CmdExecSession
	cmd.ExecConditions = []string{"request.unknown == true"}
	ack := fixture.o.HandleCommand(context.Background(), cmd)
	if ack.Status != routesync.AckRejected || ack.HTTPStatus != 400 || ack.ExecSession != nil {
		t.Fatalf("invalid-condition ack = %+v", ack)
	}
	if sb, err := fixture.o.st.Get(context.Background(), cmd.SID); err != nil || sb != nil {
		t.Fatalf("invalid condition imported target: %+v, %v", sb, err)
	}
}

func TestClusterNonExecCommandRejectsExecConditions(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	cmd := fixture.command("stable-g1", fixture.token)
	cmd.CmdID = "connect-with-exec-condition"
	cmd.Kind = routesync.CmdConnect
	cmd.ExecConditions = []string{"true"}
	ack := fixture.o.HandleCommand(context.Background(), cmd)
	if ack.Status != routesync.AckRejected || ack.HTTPStatus != 400 {
		t.Fatalf("foreign exec conditions ack = %+v", ack)
	}
	if sb, err := fixture.o.st.Get(context.Background(), cmd.SID); err != nil || sb != nil {
		t.Fatalf("foreign exec conditions inserted target: %+v, %v", sb, err)
	}
}

func TestClusterRejectsExplicitEmptyExecConditionsOnWrongCommand(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	var cmd routesync.Command
	if err := json.Unmarshal([]byte(`{"cmd_id":"foreign-empty","kind":"connect","exec_conditions":[]}`), &cmd); err != nil {
		t.Fatal(err)
	}
	ack := fixture.o.HandleCommand(context.Background(), &cmd)
	if ack.Status != routesync.AckRejected || ack.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("explicit empty foreign exec conditions ack = %+v", ack)
	}
}

func TestClusterExecSessionRejectsDeadLocalTarget(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	blockClusterExecLaunch(t, fixture)
	initial := fixture.command("stable-g1", fixture.token)
	initial.CmdID = "exec-import-paused"
	initial.Kind = routesync.CmdExecSession
	if _, _, err := fixture.o.prepareClusterExecSession(context.Background(), initial, func() int64 { return 1 }); err != nil {
		t.Fatal(err)
	}
	if err := fixture.o.st.SetState(context.Background(), initial.SID, types.StateDead); err != nil {
		t.Fatal(err)
	}

	retry := fixture.command(initial.SID, "malformed-token-is-ignored-for-existing-target")
	retry.CmdID = "exec-dead"
	retry.Kind = routesync.CmdExecSession
	ack := fixture.o.HandleCommand(context.Background(), retry)
	if ack.Status != routesync.AckRejected || ack.ExecSession != nil || !strings.Contains(ack.Reason, "not found") {
		t.Fatalf("dead-target ack = %+v", ack)
	}
}

func blockClusterExecLaunch(t *testing.T, fixture *clusterConnectFixture) {
	t.Helper()
	blocker := &blockingClusterConnectVS{started: make(chan struct{}), returned: make(chan struct{})}
	fixture.o.vs = blocker
	launchCtx, cancel := context.WithCancel(context.Background())
	fixture.o.SetLifecycleContext(launchCtx)
	t.Cleanup(func() {
		cancel()
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer drainCancel()
		if err := fixture.o.DrainLaunches(drainCtx); err != nil {
			t.Errorf("drain blocked cluster exec launch: %v", err)
		}
		// The accepted call is asynchronous and may be canceled before the new
		// assignment-first restore flow reaches Attach. If Attach did start, it
		// must have observed cancellation before DrainLaunches returns.
		select {
		case <-blocker.started:
			select {
			case <-blocker.returned:
			default:
				t.Error("blocked cluster exec launch did not stop")
			}
		default:
		}
	})
}
