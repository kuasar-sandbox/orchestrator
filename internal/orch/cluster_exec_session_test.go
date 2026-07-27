package orch

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestPrepareClusterExecSessionImportsAndMintsStableSubjectToken(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	cmd := fixture.command("stable-g1", fixture.token)
	cmd.CmdID = "exec-import"
	cmd.Kind = routesync.CmdExecSession
	cmd.TTLSeconds = 37
	const now = int64(1_800_000_000)

	sb, result, err := fixture.o.prepareClusterExecSession(context.Background(), cmd, now)
	if err != nil {
		t.Fatal(err)
	}
	if sb == nil || sb.ID != cmd.SID || sb.State != types.StatePaused || sb.Cluster == nil ||
		sb.Cluster.Group != cmd.Cluster.Group || sb.Cluster.RouteKey != cmd.Cluster.RouteKey ||
		sb.AuthSandboxID() != cmd.Cluster.AuthSandboxID {
		t.Fatalf("prepared sandbox = %+v", sb)
	}
	if result == nil || result.ExecAccessToken == "" {
		t.Fatalf("exec-session result = %+v", result)
	}
	if err := keys.VerifyExecAccessToken(result.ExecAccessToken, sb.ServiceSecret, sb.AuthSandboxID(), time.Unix(now+36, 0)); err != nil {
		t.Fatalf("token before expiry: %v", err)
	}
	if err := keys.VerifyExecAccessToken(result.ExecAccessToken, sb.ServiceSecret, sb.AuthSandboxID(), time.Unix(now+37, 0)); err == nil {
		t.Fatal("token accepted at expiry")
	}
	if err := keys.VerifyExecAccessToken(result.ExecAccessToken, sb.ServiceSecret, sb.ID, time.Unix(now, 0)); err == nil {
		t.Fatal("token accepted NodeSandboxID instead of stable AuthSandboxID")
	}
}

func TestPrepareClusterExecSessionMintsPerCommandLongLivedTokens(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	first := fixture.command("stable-g1", fixture.token)
	first.CmdID = "exec-first"
	first.Kind = routesync.CmdExecSession
	sb, firstResult, err := fixture.o.prepareClusterExecSession(context.Background(), first, 1)
	if err != nil {
		t.Fatal(err)
	}
	second := fixture.command(first.SID, "malformed-token-is-ignored-for-existing-target")
	second.CmdID = "exec-second"
	second.Kind = routesync.CmdExecSession
	_, secondResult, err := fixture.o.prepareClusterExecSession(context.Background(), second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if firstResult.ExecAccessToken == secondResult.ExecAccessToken {
		t.Fatal("different commands minted the same exec access token")
	}
	for _, token := range []string{firstResult.ExecAccessToken, secondResult.ExecAccessToken} {
		if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.AuthSandboxID(), time.Unix(math.MaxInt64, 0)); err != nil {
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
	if err != nil || stored == nil || stored.State != types.StatePaused {
		t.Fatalf("synchronously imported target = %+v, %v", stored, err)
	}
	if err := keys.VerifyExecAccessToken(ack.ExecSession.ExecAccessToken, stored.ServiceSecret, stored.AuthSandboxID(), time.Now()); err != nil {
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
	cmd.TTLSeconds = 2
	if _, _, err := fixture.o.prepareClusterExecSession(context.Background(), cmd, math.MaxInt64-1); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("overflow ttl error = %v, want bad request", err)
	}
	stored, err := fixture.o.st.Get(context.Background(), cmd.SID)
	if err != nil || stored != nil {
		t.Fatalf("overflow ttl inserted target = %+v, %v", stored, err)
	}
}
