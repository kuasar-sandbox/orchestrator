package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	sandboxctl "github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

func TestBuildRegisterReplayPreservesIdentityAndState(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{ID: "n1"}, svc)
	apiSecretFingerprint := strings.Repeat("a", 64)
	cmd := &routesync.Command{
		CmdID: "c1", Kind: routesync.CmdBuildRegister,
		BuildID: "b1", TemplateRef: "transient-1", Profile: "bare", APISecretFingerprint: apiSecretFingerprint,
		BuildResources: &routesync.BuildResources{CPU: 1000, Memory: 1 << 30},
		Config:         map[string]string{"stub.build_result": "timeout"},
		ImageRepo:      "registry.test/repo", RegistryAuth: `{"auths":{"registry.test":{"auth":"opaque"}}}`,
	}
	if got := node.handleBuildRegister(cmd); got.Status != routesync.AckAccepted {
		t.Fatalf("first build_register ack = %+v", got)
	}
	node.mu.Lock()
	node.builds[cmd.BuildID].State = "ready"
	node.mu.Unlock()

	replay := *cmd
	replay.CmdID = "c2"
	if got := node.handleBuildRegister(&replay); got.Status != routesync.AckAccepted {
		t.Fatalf("identical replay ack = %+v", got)
	}
	select {
	case event := <-node.buildEvents:
		if event.BuildID != cmd.BuildID || event.State != "ready" || event.TemplateID != cmd.TemplateRef {
			t.Fatalf("terminal replay event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal replay acknowledged before republishing the persisted result")
	}
	for name, mutate := range map[string]func(*routesync.Command){
		"profile":  func(c *routesync.Command) { c.Profile = "e2b" },
		"template": func(c *routesync.Command) { c.TemplateRef = "transient-2" },
		"tenant":   func(c *routesync.Command) { c.APISecretFingerprint = strings.Repeat("b", 64) },
		"resources": func(c *routesync.Command) {
			c.BuildResources = &routesync.BuildResources{CPU: 2000, Memory: 1 << 30}
		},
		"config": func(c *routesync.Command) {
			c.Config = map[string]string{"stub.build_result": "timeout", "other": "value"}
		},
		"registry auth": func(c *routesync.Command) { c.RegistryAuth = `{"auths":{}}` },
	} {
		t.Run(name, func(t *testing.T) {
			conflict := replay
			mutate(&conflict)
			if got := node.handleBuildRegister(&conflict); got.Status != routesync.AckRejected {
				t.Fatalf("conflicting replay ack = %+v", got)
			}
		})
	}

	node.mu.Lock()
	stored := node.builds[cmd.BuildID]
	buildCount := len(node.builds)
	node.mu.Unlock()
	if buildCount != 1 || stored.Profile != "bare" || stored.TemplateID != "transient-1" || stored.State != "ready" {
		t.Fatalf("replay changed stored build: count=%d build=%+v", buildCount, stored)
	}
	svc.mu.Lock()
	var registerEvents, stateEvents int
	for _, event := range svc.events {
		switch event.Type {
		case "build_register":
			registerEvents++
		case "build_event":
			stateEvents++
		}
	}
	svc.mu.Unlock()
	if registerEvents != 1 || stateEvents != 1 {
		t.Fatalf("terminal replay restarted build state machine: register=%d state=%d", registerEvents, stateEvents)
	}
}

func TestBuildRegisterExactReplayPrecedesStrictCredentialLease(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{ID: "n1", StrictKeys: true}, svc)
	fingerprint := strings.Repeat("a", 64)
	node.mu.Lock()
	node.keyPairs[fingerprint] = stubKeyPair{APISecretFingerprint: fingerprint, ExpiresUnix: time.Now().Add(time.Hour).Unix()}
	node.mu.Unlock()
	cmd := &routesync.Command{
		CmdID: "register-first", Kind: routesync.CmdBuildRegister,
		BuildID: "strict-replay", TemplateRef: "transient-strict-replay", Profile: "bare",
		APISecretFingerprint: fingerprint,
		BuildResources:       &routesync.BuildResources{CPU: 1000, Memory: 1 << 30},
		Config:               map[string]string{"stub.build_result": "timeout"},
	}
	if got := node.handleBuildRegister(cmd); got.Status != routesync.AckAccepted {
		t.Fatalf("initial registration = %+v", got)
	}
	node.mu.Lock()
	delete(node.keyPairs, fingerprint) // credential lease ended after an ACK loss
	node.mu.Unlock()

	replay := *cmd
	replay.CmdID = "register-replay"
	if got := node.handleBuildRegister(&replay); got.Status != routesync.AckAccepted {
		t.Fatalf("exact replay after key withdrawal = %+v", got)
	}
	conflict := replay
	conflict.CmdID = "register-conflict"
	conflict.Profile = "e2b"
	if got := node.handleBuildRegister(&conflict); got.Status != routesync.AckRejected || got.HTTPStatus != http.StatusConflict {
		t.Fatalf("conflicting replay after key withdrawal = %+v, want 409", got)
	}
	node.mu.Lock()
	buildCount := len(node.builds)
	node.mu.Unlock()
	if buildCount != 1 {
		t.Fatalf("replay changed registration count to %d", buildCount)
	}
}

func TestBuildRegisterEnforcesRegistrationAndExecutionAdmission(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{
		ID: "n1",
		BuildRegistrationCapacity: &routesync.BuildAdmissionLimit{
			MaxBuilds: 1, Resources: &routesync.BuildResources{CPU: 2000, Memory: 2 << 30},
		},
		BuildExecutionCapacity: &routesync.BuildAdmissionLimit{
			MaxBuilds: 1, Resources: &routesync.BuildResources{CPU: 1000, Memory: 1 << 30},
		},
	}, svc)
	command := func(id string, resources *routesync.BuildResources) *routesync.Command {
		return &routesync.Command{CmdID: id, Kind: routesync.CmdBuildRegister, BuildID: id,
			TemplateRef: "transient-" + id, Profile: "bare", BuildResources: resources,
			Config: map[string]string{"stub.build_result": "timeout"}}
	}
	tooLarge := node.handleBuildRegister(command("too-large", &routesync.BuildResources{CPU: 1001, Memory: 1 << 30}))
	if tooLarge.Status != routesync.AckRejected || tooLarge.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("single-build execution rejection = %+v", tooLarge)
	}
	accepted := command("accepted", &routesync.BuildResources{CPU: 1000, Memory: 1 << 30})
	if got := node.handleBuildRegister(accepted); got.Status != routesync.AckAccepted {
		t.Fatalf("accepted registration = %+v", got)
	}
	full := node.handleBuildRegister(command("full", &routesync.BuildResources{CPU: 1000, Memory: 1 << 30}))
	if full.Status != routesync.AckRejected || full.HTTPStatus != http.StatusTooManyRequests {
		t.Fatalf("registration capacity rejection = %+v", full)
	}
	if replay := node.handleBuildRegister(accepted); replay.Status != routesync.AckAccepted {
		t.Fatalf("exact retry after capacity filled = %+v", replay)
	}
}

func TestStubExecutionAdmissionQueuesFIFOByAggregateResources(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{
		ID: "n1",
		BuildRegistrationCapacity: &routesync.BuildAdmissionLimit{
			MaxBuilds: 3, Resources: &routesync.BuildResources{CPU: 2000, Memory: 3 << 30},
		},
		BuildExecutionCapacity: &routesync.BuildAdmissionLimit{
			MaxBuilds: 2, Resources: &routesync.BuildResources{CPU: 1000, Memory: 2 << 30},
		},
	}, svc)
	register := func(id string, cpu int64) {
		t.Helper()
		cmd := &routesync.Command{
			CmdID: "register-" + id, Kind: routesync.CmdBuildRegister,
			BuildID: id, TemplateRef: "transient-" + id, Profile: "bare",
			BuildResources: &routesync.BuildResources{CPU: cpu, Memory: 1 << 30},
			Config:         map[string]string{"stub.build_result": "timeout"},
		}
		if got := node.handleBuildRegister(cmd); got.Status != routesync.AckAccepted {
			t.Fatalf("register %s = %+v", id, got)
		}
	}
	register("first", 600)
	register("second", 600)
	register("third", 400)
	for _, id := range []string{"first", "second", "third"} {
		if err := node.requestBuildExecution(id); err != nil {
			t.Fatal(err)
		}
	}

	node.mu.Lock()
	states := map[string]string{}
	for id, build := range node.builds {
		states[id] = build.State
	}
	node.mu.Unlock()
	if states["first"] != "building" || states["second"] != "registered" || states["third"] != "registered" {
		t.Fatalf("FIFO head was bypassed under aggregate pressure: states=%v", states)
	}
	hb := node.Heartbeat()
	if hb.BuildExecutionUsage.Builds != 1 || hb.BuildExecutionUsage.Resources.CPU != 600 {
		t.Fatalf("execution usage before release = %+v", hb.BuildExecutionUsage)
	}

	if err := node.setBuildState("first", "ready", "", "", true); err != nil {
		t.Fatal(err)
	}
	node.mu.Lock()
	states["first"] = node.builds["first"].State
	states["second"] = node.builds["second"].State
	states["third"] = node.builds["third"].State
	node.mu.Unlock()
	if states["first"] != "ready" || states["second"] != "building" || states["third"] != "building" {
		t.Fatalf("queued Builds did not advance in FIFO order after release: states=%v", states)
	}
	hb = node.Heartbeat()
	if hb.BuildExecutionUsage.Builds != 2 || hb.BuildExecutionUsage.Resources.CPU != 1000 {
		t.Fatalf("execution usage after release = %+v", hb.BuildExecutionUsage)
	}
}

func TestStubExecutionAdmissionFIFOUsesTriggerOrder(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := newService("", log)
	node := newStubNode(stubNodeOptions{
		ID: "n1",
		BuildRegistrationCapacity: &routesync.BuildAdmissionLimit{
			MaxBuilds: 3, Resources: &routesync.BuildResources{CPU: 3000, Memory: 3 << 30},
		},
		BuildExecutionCapacity: &routesync.BuildAdmissionLimit{
			MaxBuilds: 1, Resources: &routesync.BuildResources{CPU: 1000, Memory: 1 << 30},
		},
	}, service)
	for _, id := range []string{"blocker", "registered-first", "triggered-first"} {
		cmd := &routesync.Command{
			CmdID: "register-" + id, Kind: routesync.CmdBuildRegister,
			BuildID: id, TemplateRef: "transient-" + id, Profile: "bare",
			BuildResources: &routesync.BuildResources{CPU: 1000, Memory: 1 << 30},
			Config:         map[string]string{"stub.build_result": "timeout"},
		}
		if got := node.handleBuildRegister(cmd); got.Status != routesync.AckAccepted {
			t.Fatalf("register %s = %+v", id, got)
		}
	}
	for _, id := range []string{"blocker", "triggered-first", "registered-first"} {
		if err := node.requestBuildExecution(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := node.setBuildState("blocker", "ready", "", "", false); err != nil {
		t.Fatal(err)
	}
	node.mu.Lock()
	triggeredState := node.builds["triggered-first"].State
	registeredState := node.builds["registered-first"].State
	node.mu.Unlock()
	if triggeredState != "building" || registeredState != "registered" {
		t.Fatalf("execution FIFO followed registration rather than trigger order: triggered-first=%s registered-first=%s",
			triggeredState, registeredState)
	}
}

func TestAddBuildResourcesFailClosedOnOverflow(t *testing.T) {
	dst := &routesync.BuildResources{CPU: math.MaxInt64 - 1, Memory: 1, Storage: 1}
	addBuildResourcesFailClosed(dst, &routesync.BuildResources{CPU: 2, Memory: 1, Storage: 1})
	if dst.CPU != math.MaxInt64 || dst.Memory != math.MaxInt64 || dst.Storage != math.MaxInt64 {
		t.Fatalf("overflow usage = %+v, want fully occupied", dst)
	}
}

func TestExecSessionMintsBoundTokensAndResumesPausedSandboxAsynchronously(t *testing.T) {
	service, node, sandbox, command := newExecStubFixture(t, routesync.StatePaused, 250*time.Millisecond)
	events, cancel := node.Subscribe()
	defer cancel()
	command.TTLSeconds = 37
	command.MigrationToken = "kmt1.ignored-for-existing-target"
	command.ExecConditions = []string{"request.cwd == '/workspace'"}

	issuedAt := time.Now()
	got := node.HandleCommand(context.Background(), command)
	if got.Status != routesync.AckAccepted || got.Reason != "" || got.Connect != nil ||
		got.ExecSession == nil || got.ExecSession.ExecAccessToken == "" {
		t.Fatalf("exec_session ack = %+v", got)
	}
	if err := keys.VerifyExecAccessToken(
		got.ExecSession.ExecAccessToken, sandbox.ServiceSecret, sandbox.AuthSandboxID, time.Now(),
	); err != nil {
		t.Fatalf("exec access token = invalid: %v", err)
	}
	claims, err := keys.ParseAndVerifyExecAccessToken(
		got.ExecSession.ExecAccessToken, sandbox.ServiceSecret, sandbox.AuthSandboxID, time.Now(),
	)
	if err != nil || len(claims.Conditions) != 1 || claims.Conditions[0] != command.ExecConditions[0] {
		t.Fatalf("exec token conditions = %+v, %v", claims, err)
	}
	if err := keys.VerifyExecAccessToken(
		got.ExecSession.ExecAccessToken, sandbox.ServiceSecret, sandbox.AuthSandboxID, issuedAt.Add(time.Minute),
	); err == nil {
		t.Fatal("TTL-bound exec access token remained valid after expiry")
	}
	node.mu.Lock()
	stateAfterAck := node.sandboxes[sandbox.SID].State
	node.mu.Unlock()
	if stateAfterAck != routesync.StatePaused {
		t.Fatalf("exec_session synchronously resumed sandbox: state=%q", stateAfterAck)
	}
	select {
	case event := <-events:
		if event.Kind != routesync.TypeUpsert || event.Route.SandboxID != sandbox.SID ||
			event.Route.State != routesync.StateRunning {
			t.Fatalf("asynchronous resume event = %+v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("asynchronous exec-session resume did not publish a running route")
	}

	longLived := *command
	longLived.CmdID = "exec-session-2"
	longLived.TTLSeconds = 0
	second := node.HandleCommand(context.Background(), &longLived)
	if second.Status != routesync.AckAccepted || second.ExecSession == nil ||
		second.ExecSession.ExecAccessToken == "" || second.ExecSession.ExecAccessToken == got.ExecSession.ExecAccessToken {
		t.Fatalf("second exec_session ack = %+v", second)
	}
	if err := keys.VerifyExecAccessToken(
		second.ExecSession.ExecAccessToken, sandbox.ServiceSecret, sandbox.AuthSandboxID,
		issuedAt.Add(365*24*time.Hour),
	); err != nil {
		t.Fatalf("long-lived exec access token = invalid: %v", err)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if encoded, err := json.Marshal(service.events); err != nil {
		t.Fatal(err)
	} else if strings.Contains(string(encoded), got.ExecSession.ExecAccessToken) ||
		strings.Contains(string(encoded), second.ExecSession.ExecAccessToken) ||
		strings.Contains(string(encoded), command.ExecConditions[0]) {
		t.Fatal("stub events exposed an exec token or condition source")
	}
}

func TestExecSessionRejectsInvalidEnvelopeAndBindingWithoutMutation(t *testing.T) {
	tests := []struct {
		name          string
		mutateCommand func(*routesync.Command)
		mutateSandbox func(*stubSandbox)
	}{
		{name: "negative ttl", mutateCommand: func(c *routesync.Command) { c.TTLSeconds = -1 }},
		{name: "unrepresentable ttl", mutateCommand: func(c *routesync.Command) { c.TTLSeconds = math.MaxInt64 }},
		{name: "invalid condition", mutateCommand: func(c *routesync.Command) { c.ExecConditions = []string{"request.unknown == true"} }},
		{name: "foreign operation field", mutateCommand: func(c *routesync.Command) { c.TimeoutSeconds = 1 }},
		{name: "wrong credential", mutateCommand: func(c *routesync.Command) { c.APISecretFingerprint = strings.Repeat("f", 64) }},
		{name: "wrong profile", mutateCommand: func(c *routesync.Command) { c.Profile = string(types.ProfileE2B) }},
		{name: "wrong cluster context", mutateCommand: func(c *routesync.Command) { c.Cluster.Group = "/other" }},
		{name: "unavailable state", mutateSandbox: func(s *stubSandbox) { s.State = "creating" }},
		{name: "inconsistent template", mutateSandbox: func(s *stubSandbox) {
			s.TemplateID = types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String()
		}},
		{name: "invalid service secret", mutateSandbox: func(s *stubSandbox) { s.ServiceSecret = "invalid" }},
		{name: "missing target does not import", mutateCommand: func(c *routesync.Command) {
			c.SID = "missing-g1"
			c.MigrationToken = "kmt1.not-imported-by-stub"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, node, sandbox, command := newExecStubFixture(t, routesync.StatePaused, time.Second)
			if test.mutateCommand != nil {
				test.mutateCommand(command)
			}
			if test.mutateSandbox != nil {
				test.mutateSandbox(sandbox)
			}
			beforeState := sandbox.State
			got := node.HandleCommand(context.Background(), command)
			if got.Status != routesync.AckRejected || got.ExecSession != nil || got.Connect != nil {
				t.Fatalf("rejected exec_session ack = %+v", got)
			}
			node.mu.Lock()
			stored := node.sandboxes[sandbox.SID]
			count := len(node.sandboxes)
			node.mu.Unlock()
			if stored != sandbox || stored.State != beforeState || count != 1 {
				t.Fatalf("rejected exec_session changed sandboxes: stored=%+v count=%d", stored, count)
			}
		})
	}
}

func TestStubRejectsExecConditionsOnOtherCommandKinds(t *testing.T) {
	_, node, sandbox, command := newExecStubFixture(t, routesync.StateRunning, time.Second)
	command.Kind = routesync.CmdDelete
	command.ExecConditions = []string{"true"}
	ack := node.HandleCommand(context.Background(), command)
	if ack.Status != routesync.AckRejected {
		t.Fatalf("foreign exec conditions ack = %+v", ack)
	}
	node.mu.Lock()
	stored := node.sandboxes[sandbox.SID]
	node.mu.Unlock()
	if stored != sandbox || stored.State != routesync.StateRunning {
		t.Fatalf("foreign exec conditions mutated sandbox = %+v", stored)
	}
}

func TestStubRejectsExplicitEmptyExecConditionsOnOtherCommandKinds(t *testing.T) {
	_, node, _, _ := newExecStubFixture(t, routesync.StateRunning, time.Second)
	var command routesync.Command
	if err := json.Unmarshal([]byte(`{"cmd_id":"foreign-empty","kind":"delete","exec_conditions":[]}`), &command); err != nil {
		t.Fatal(err)
	}
	if ack := node.HandleCommand(context.Background(), &command); ack.Status != routesync.AckRejected {
		t.Fatalf("explicit empty foreign exec conditions ack = %+v", ack)
	}
}

func TestExecDataGateRequiresConnectAndValidExecKAT(t *testing.T) {
	service, node, sandbox, _ := newExecStubFixture(t, routesync.StateRunning, 0)

	ordinary := httptest.NewRequest(http.MethodGet, "http://sandbox:443/", nil)
	ordinary.Host = "sandbox:443"
	ordinary.Header.Set("E2b-Sandbox-Service", "exec")
	ordinaryResponse := httptest.NewRecorder()
	service.serveData(ordinaryResponse, ordinary)
	if ordinaryResponse.Code != http.StatusMethodNotAllowed || ordinaryResponse.Header().Get("Allow") != http.MethodConnect {
		t.Fatalf("ordinary exec response = %d Allow=%q", ordinaryResponse.Code, ordinaryResponse.Header().Get("Allow"))
	}

	expired, err := keys.MintExecAccessToken(
		sandbox.ServiceSecret, sandbox.AuthSandboxID, time.Now().Add(-time.Second).Unix(),
	)
	if err != nil {
		t.Fatal(err)
	}
	wrongSubject, err := keys.MintExecAccessToken(sandbox.ServiceSecret, "other-stable", 0)
	if err != nil {
		t.Fatal(err)
	}
	wrongAudience, err := keys.MintForwardAccessToken(sandbox.ServiceSecret, sandbox.AuthSandboxID)
	if err != nil {
		t.Fatal(err)
	}
	for name, token := range map[string]string{
		"missing":        "",
		"malformed":      "not-a-kat",
		"expired":        expired,
		"wrong subject":  wrongSubject,
		"wrong audience": wrongAudience,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", nil)
			request.Host = "sandbox:443"
			request.Header.Set("E2b-Sandbox-Id", sandbox.SID)
			request.Header.Set("E2b-Sandbox-Service", "exec")
			if token != "" {
				request.Header.Set("X-Access-Token", token)
			}
			response := httptest.NewRecorder()
			service.serveData(response, request)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("invalid exec token status = %d, want 401", response.Code)
			}
			service.mu.Lock()
			hits := len(service.dataHits)
			service.mu.Unlock()
			node.mu.Lock()
			state := node.sandboxes[sandbox.SID].State
			node.mu.Unlock()
			if hits != 0 || state != routesync.StateRunning {
				t.Fatalf("invalid exec token caused side effects: hits=%d state=%q", hits, state)
			}
		})
	}

	token, err := keys.MintExecAccessToken(sandbox.ServiceSecret, sandbox.AuthSandboxID, 0)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(service.serveData))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	header := fmt.Sprintf(
		"CONNECT sandbox:443 HTTP/1.1\r\nHost: sandbox:443\r\nE2b-Sandbox-Id: %s\r\n"+
			"E2b-Sandbox-Service: exec\r\nX-Access-Token: %s\r\n\r\n",
		sandbox.SID, token,
	)
	const secretArgv = "/bin/argv-must-not-be-observed"
	frame := stubExecFrame(secretArgv)
	if _, err := conn.Write(append([]byte(header), frame...)); err != nil {
		t.Fatal(err)
	}
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(statusLine, "HTTP/1.1 200 ") {
		t.Fatalf("CONNECT status line = %q", statusLine)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	tail, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(tail) != "exec-admitted" {
		t.Fatalf("exec response tail = %q", tail)
	}
	service.mu.Lock()
	hits := append([]dataHit(nil), service.dataHits...)
	service.mu.Unlock()
	if len(hits) != 1 || hits[0].SandboxID != sandbox.SID || hits[0].Path != "/exec-admitted" ||
		hits[0].Method != "EXEC" || hits[0].Host != "exec" {
		t.Fatalf("exec data hits = %+v", hits)
	}
	if encoded, err := json.Marshal(hits); err != nil {
		t.Fatal(err)
	} else if strings.Contains(string(encoded), token) || strings.Contains(string(encoded), secretArgv) {
		t.Fatal("exec data observation exposed the access token or argv")
	}
}

func TestExecDataConditionFailureReturnsGenericCtlErrorWithoutHit(t *testing.T) {
	service, _, sandbox, _ := newExecStubFixture(t, routesync.StateRunning, 0)
	token, err := keys.MintExecAccessTokenWithConditions(
		sandbox.ServiceSecret, sandbox.AuthSandboxID, 0,
		[]string{`request.argv == ['/bin/allowed']`},
	)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(service.serveData))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tcpConn := conn.(*net.TCPConn)
	defer tcpConn.Close()
	_ = tcpConn.SetDeadline(time.Now().Add(5 * time.Second))
	header := fmt.Sprintf(
		"CONNECT sandbox:443 HTTP/1.1\r\nHost: sandbox:443\r\nE2b-Sandbox-Id: %s\r\n"+
			"E2b-Sandbox-Service: exec\r\nX-Access-Token: %s\r\n\r\n",
		sandbox.SID, token,
	)
	if _, err := tcpConn.Write(append([]byte(header), stubExecFrame("/bin/denied")...)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(tcpConn)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d", response.StatusCode)
	}
	if err := tcpConn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	var rejected sandboxctl.Response
	if err := sandboxctl.ReadMessage(reader, &rejected); err != nil {
		t.Fatal(err)
	}
	if rejected.Type != sandboxctl.TypeError || rejected.Msg != "exec request rejected" {
		t.Fatalf("rejection = %+v", rejected)
	}
	service.mu.Lock()
	hits := len(service.dataHits)
	service.mu.Unlock()
	if hits != 0 {
		t.Fatalf("denied exec produced %d data hits", hits)
	}
}

func stubExecFrame(argv0 string) []byte {
	payload, _ := json.Marshal(map[string]any{
		"type": "exec_request",
		"exec": map[string]any{"argv": []string{argv0}},
	})
	frame := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	return frame
}

func TestKeyPutStoresPairAndStrictLifecycleUsesAPISecretFingerprint(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{ID: "n1", StrictKeys: true}, svc)
	apiSecret := strings.Repeat("11", 32)
	manifestKey := strings.Repeat("22", 32)
	apiSecretFingerprint := testStubFingerprint(t, apiSecret)
	manifestKeyFingerprint := testStubFingerprint(t, manifestKey)
	expiresUnix := time.Now().Add(time.Hour).Unix()

	put := &routesync.Command{
		CmdID: "key-1", Kind: routesync.CmdKeyPut,
		APISecretFingerprint: apiSecretFingerprint, APISecretType: "inline", APISecret: apiSecret,
		ManifestKeyFingerprint: manifestKeyFingerprint, ManifestKeyType: "inline", ManifestKey: manifestKey,
		ExpiresUnix: expiresUnix,
	}
	if got := node.HandleCommand(context.Background(), put); got.Status != routesync.AckAccepted {
		t.Fatalf("key_put ack = %+v", got)
	}

	node.mu.Lock()
	pair, found := node.keyPairs[apiSecretFingerprint]
	node.mu.Unlock()
	if !found || pair.APISecret != apiSecret || pair.ManifestKey != manifestKey || pair.ExpiresUnix != expiresUnix {
		t.Fatal("stub did not retain the installed key pair")
	}

	create := &routesync.Command{
		CmdID: "create-1", Kind: routesync.CmdCreate, SID: "sb1",
		TemplateRef: types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(), Profile: "e2b",
		Cluster:              &routesync.ClusterSandboxContext{Group: "/g", RouteKey: "rk", AuthSandboxID: "sb1"},
		APISecretFingerprint: apiSecretFingerprint,
		Config: map[string]string{
			"stub.create_result":           "timeout",
			clusterstate.ObjectMetadataKey: `{"group":"forged","route_key":"forged"}`,
			sandboxcfg.NsCredentials:       `{"envd_access_token":"envd-override","traffic_access_token":"traffic-override"}`,
		},
	}
	if got := node.HandleCommand(context.Background(), create); got.Status != routesync.AckAccepted {
		t.Fatalf("create ack = %+v", got)
	}
	node.mu.Lock()
	storedSandbox := node.sandboxes[create.SID]
	serviceSecret := storedSandbox.ServiceSecret
	envdToken := storedSandbox.EnvdAccessToken
	trafficToken := storedSandbox.TrafficAccessToken
	forwardToken := storedSandbox.ForwardAccessToken
	commandMetadata := node.commands[len(node.commands)-1].Metadata
	node.mu.Unlock()
	wantServiceSecret, err := keys.DeriveServiceSecret(apiSecret, create.Cluster.AuthSandboxID)
	if err != nil {
		t.Fatal(err)
	}
	if serviceSecret != wantServiceSecret || envdToken != "envd-override" || trafficToken != "traffic-override" {
		t.Fatal("stub did not materialize the expected credential overrides")
	}
	if err := keys.VerifyForwardAccessToken(forwardToken, serviceSecret, create.Cluster.AuthSandboxID); err != nil {
		t.Fatalf("stub forward access token = invalid: %v", err)
	}
	if storedSandbox.Profile != create.Profile || !sameStubClusterContext(storedSandbox.Cluster, create.Cluster) {
		t.Fatal("stub sandbox context did not match the trusted command")
	}
	if _, found := storedSandbox.Metadata[clusterstate.ObjectMetadataKey]; found {
		t.Fatalf("stub sandbox retained reserved cluster metadata: %+v", storedSandbox.Metadata)
	}
	if _, found := storedSandbox.Metadata[sandboxcfg.NsCredentials]; found {
		t.Fatalf("stub sandbox retained credentials metadata: %+v", storedSandbox.Metadata)
	}
	if _, found := commandMetadata[sandboxcfg.NsCredentials]; found {
		t.Fatalf("stub command log exposed credentials metadata: %+v", commandMetadata)
	}
	route := storedSandbox.routeEntry()
	if route.AuthSandboxID != create.Cluster.AuthSandboxID || route.APISecret != apiSecret ||
		route.APISecretFingerprint != apiSecretFingerprint || route.ManifestKeyFingerprint != manifestKeyFingerprint ||
		route.ServiceSecret != serviceSecret || route.EnvdAccessToken != envdToken ||
		route.TrafficAccessToken != trafficToken || route.ForwardAccessToken != forwardToken {
		t.Fatal("stub route did not project the sandbox credential record")
	}

	build := &routesync.Command{
		CmdID: "build-1", Kind: routesync.CmdBuildRegister, BuildID: "b1",
		TemplateRef: "template-1", Profile: "bare", APISecretFingerprint: apiSecretFingerprint,
		BuildResources: &routesync.BuildResources{CPU: 1000, Memory: 1 << 30},
		Config:         map[string]string{"stub.build_result": "timeout"},
	}
	if got := node.HandleCommand(context.Background(), build); got.Status != routesync.AckAccepted {
		t.Fatalf("build_register ack = %+v", got)
	}

	missing := *create
	missing.CmdID = "create-2"
	missing.SID = "sb2"
	missing.APISecretFingerprint = strings.Repeat("f", 64)
	if got := node.HandleCommand(context.Background(), &missing); got.Status != routesync.AckRejected {
		t.Fatalf("create with missing pair ack = %+v", got)
	}

	if got := node.HandleCommand(context.Background(), &routesync.Command{
		CmdID: "drop-1", Kind: routesync.CmdKeyDrop, APISecretFingerprint: apiSecretFingerprint,
	}); got.Status != routesync.AckAccepted {
		t.Fatalf("key_drop ack = %+v", got)
	}
	node.mu.Lock()
	node.sandboxes[create.SID].State = routesync.StatePaused
	node.mu.Unlock()
	wrongConnect := &routesync.Command{
		CmdID: "connect-wrong", Kind: routesync.CmdConnect, SID: create.SID,
		APISecretFingerprint: strings.Repeat("f", 64),
	}
	if got := node.HandleCommand(context.Background(), wrongConnect); got.Status != routesync.AckRejected {
		t.Fatalf("connect with wrong binding ack = %+v", got)
	}
	node.mu.Lock()
	state := node.sandboxes[create.SID].State
	node.mu.Unlock()
	if state != routesync.StatePaused {
		t.Fatalf("rejected connect changed state to %q", state)
	}
	wrongContext := *wrongConnect
	wrongContext.CmdID = "connect-wrong-context"
	wrongContext.APISecretFingerprint = apiSecretFingerprint
	wrongContext.Profile = create.Profile
	wrongContext.Cluster = &routesync.ClusterSandboxContext{Group: "/other", RouteKey: "rk", AuthSandboxID: "sb1"}
	if got := node.HandleCommand(context.Background(), &wrongContext); got.Status != routesync.AckRejected {
		t.Fatalf("connect with wrong context ack = %+v", got)
	}
	connect := *wrongConnect
	connect.CmdID = "connect-1"
	connect.APISecretFingerprint = apiSecretFingerprint
	connect.Profile = create.Profile
	connect.Cluster = create.Cluster
	connect.TimeoutSeconds = 31
	deadlineFloor := time.Now().Add(30 * time.Second).Unix()
	if got := node.HandleCommand(context.Background(), &connect); got.Status != routesync.AckAccepted {
		t.Fatalf("connect after key drop ack = %+v", got)
	} else if got.Connect == nil || got.Connect.NodeSandboxID != create.SID ||
		got.Connect.TemplateID != create.TemplateRef || got.Connect.Profile != create.Profile ||
		got.Connect.EnvdAccessToken != envdToken || got.Connect.TrafficAccessToken != trafficToken ||
		got.Connect.ForwardAccessToken != forwardToken {
		t.Fatalf("connect result = %+v", got.Connect)
	}
	node.mu.Lock()
	connectDeadline := node.sandboxes[create.SID].DeadlineUnix
	node.mu.Unlock()
	if connectDeadline < deadlineFloor || connectDeadline > time.Now().Add(32*time.Second).Unix() {
		t.Fatalf("connect deadline = %d, want approximately now+31s", connectDeadline)
	}

	wrongDelete := &routesync.Command{
		CmdID: "delete-wrong", Kind: routesync.CmdDelete, SID: create.SID,
		APISecretFingerprint: strings.Repeat("f", 64),
	}
	if got := node.HandleCommand(context.Background(), wrongDelete); got.Status != routesync.AckRejected {
		t.Fatalf("delete with wrong binding ack = %+v", got)
	}
	if node.getSandbox(create.SID) == nil {
		t.Fatal("rejected delete removed sandbox")
	}
	deleteCmd := *wrongDelete
	deleteCmd.CmdID = "delete-1"
	deleteCmd.APISecretFingerprint = apiSecretFingerprint
	if got := node.HandleCommand(context.Background(), &deleteCmd); got.Status != routesync.AckAccepted {
		t.Fatalf("delete after key drop ack = %+v", got)
	}
	if node.getSandbox(create.SID) != nil {
		t.Fatal("accepted delete kept sandbox")
	}
}

func TestNodeSnapshotRedactsCredentialMaterial(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{ID: "n1"}, svc)
	apiSecret := strings.Repeat("66", 32)
	manifestKey := strings.Repeat("77", 32)
	apiRef := "secret://tenant/api-root"
	manifestRef := "secret://tenant/manifest-root"
	node.keyPairs["inline"] = stubKeyPair{
		APISecretFingerprint: testStubFingerprint(t, apiSecret), APISecretType: "inline", APISecret: apiSecret,
		ManifestKeyFingerprint: testStubFingerprint(t, manifestKey), ManifestKeyType: "inline", ManifestKey: manifestKey,
	}
	node.keyPairs["ref"] = stubKeyPair{
		APISecretFingerprint: strings.Repeat("8", 64), APISecretType: "ref", APISecretRef: apiRef,
		ManifestKeyFingerprint: strings.Repeat("9", 64), ManifestKeyType: "ref", ManifestKeyRef: manifestRef,
	}
	serviceSecret := strings.Repeat("aa", 32)
	forwardToken, err := keys.MintForwardAccessToken(serviceSecret, "stable-sandbox")
	if err != nil {
		t.Fatal(err)
	}
	envdToken, trafficToken := "private-envd-token", "private-traffic-token"
	node.sandboxes["node-sandbox"] = &stubSandbox{
		SID:                    "node-sandbox",
		Profile:                string(types.ProfileE2B),
		Metadata:               map[string]string{sandboxcfg.NsCredentials: `{"service_secret":"` + serviceSecret + `"}`, "visible": "value"},
		State:                  routesync.StateRunning,
		AuthSandboxID:          "stable-sandbox",
		APISecret:              apiSecret,
		APISecretFingerprint:   testStubFingerprint(t, apiSecret),
		ManifestKeyFingerprint: testStubFingerprint(t, manifestKey),
		ServiceSecret:          serviceSecret,
		EnvdAccessToken:        envdToken,
		TrafficAccessToken:     trafficToken,
		ForwardAccessToken:     forwardToken,
	}
	node.publishRoute(node.sandboxes["node-sandbox"].routeEntry())
	svc.appendDataHit(dataHit{NodeID: node.ID, SandboxID: "node-sandbox", Path: "/health"})

	raw, err := json.Marshal(struct {
		Node     nodeSnapshot `json:"node"`
		Events   []eventLog   `json:"events"`
		DataHits []dataHit    `json:"data_hits"`
	}{Node: node.snapshot(), Events: svc.events, DataHits: svc.dataHits})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{apiSecret, manifestKey, apiRef, manifestRef, serviceSecret, envdToken, trafficToken, forwardToken} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("stub observation leaked credential material")
		}
	}
	if !strings.Contains(string(raw), testStubFingerprint(t, apiSecret)) ||
		!strings.Contains(string(raw), testStubFingerprint(t, manifestKey)) {
		t.Fatal("node snapshot omitted credential fingerprints")
	}
}

func TestStubCredentialProfilesAndPublicResponses(t *testing.T) {
	apiSecret := strings.Repeat("ab", 32)
	authSandboxID := "stable-sandbox"

	e2b, err := materializeStubCredentials(types.ProfileE2B, apiSecret, authSandboxID, sandboxcfg.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	wantServiceSecret, err := keys.DeriveServiceSecret(apiSecret, authSandboxID)
	if err != nil {
		t.Fatal(err)
	}
	if e2b.ServiceSecret != wantServiceSecret || e2b.EnvdAccessToken == "" || e2b.TrafficAccessToken == "" ||
		e2b.EnvdAccessToken == e2b.TrafficAccessToken {
		t.Fatalf("e2b credential defaults were not independently materialized")
	}
	if err := keys.VerifyForwardAccessToken(e2b.ForwardAccessToken, e2b.ServiceSecret, authSandboxID); err != nil {
		t.Fatalf("e2b forward access token = invalid: %v", err)
	}
	e2bResponse := (&stubSandbox{
		SID: "node-sandbox", Profile: string(types.ProfileE2B), TemplateID: "e2b-img-template",
		EnvdAccessToken: e2b.EnvdAccessToken, TrafficAccessToken: e2b.TrafficAccessToken,
		ForwardAccessToken: e2b.ForwardAccessToken,
	}).connectResponse()
	for _, field := range []string{"envdAccessToken", "trafficAccessToken", "forwardAccessToken"} {
		if e2bResponse[field] == nil {
			t.Fatalf("e2b response omitted %s", field)
		}
	}
	assertNoInternalCredentialFields(t, e2bResponse)

	override := strings.Repeat("cd", 32)
	bare, err := materializeStubCredentials(types.ProfileBare, apiSecret, authSandboxID, sandboxcfg.Credentials{ServiceSecret: override})
	if err != nil {
		t.Fatal(err)
	}
	if bare.ServiceSecret != override || bare.EnvdAccessToken != "" || bare.TrafficAccessToken != "" {
		t.Fatalf("bare credentials did not preserve the profile contract")
	}
	if err := keys.VerifyForwardAccessToken(bare.ForwardAccessToken, bare.ServiceSecret, authSandboxID); err != nil {
		t.Fatalf("bare forward access token = invalid: %v", err)
	}
	bareResponse := (&stubSandbox{
		SID: "node-sandbox", Profile: string(types.ProfileBare), TemplateID: "bare-img-template",
		ForwardAccessToken: bare.ForwardAccessToken,
	}).connectResponse()
	if len(bareResponse) != 3 || bareResponse["forwardAccessToken"] != bare.ForwardAccessToken {
		t.Fatal("bare response did not contain exactly the public forward credential")
	}
	if _, ok := bareResponse["envdAccessToken"]; ok {
		t.Fatal("bare response exposed envdAccessToken")
	}
	if _, ok := bareResponse["trafficAccessToken"]; ok {
		t.Fatal("bare response exposed trafficAccessToken")
	}
	assertNoInternalCredentialFields(t, bareResponse)

	detailResponse := (&stubSandbox{
		SID: "node-sandbox", Profile: string(types.ProfileE2B), TemplateID: "e2b-img-template",
		State: routesync.StateRunning, AuthSandboxID: authSandboxID,
		APISecret: apiSecret, APISecretFingerprint: testStubFingerprint(t, apiSecret),
		ServiceSecret: e2b.ServiceSecret, EnvdAccessToken: e2b.EnvdAccessToken,
		TrafficAccessToken: e2b.TrafficAccessToken, ForwardAccessToken: e2b.ForwardAccessToken,
		Cluster:  &routesync.ClusterSandboxContext{Group: "/g", RouteKey: "rk", AuthSandboxID: authSandboxID},
		Metadata: map[string]string{sandboxcfg.NsCredentials: `{"envd_access_token":"private"}`, "visible": "value"},
	}).publicDetailResponse("n1")
	if detailResponse["sandboxID"] != "node-sandbox" || detailResponse["clientID"] != "n1" {
		t.Fatal("public detail response omitted sandbox identity")
	}
	if metadata, ok := detailResponse["metadata"].(map[string]string); !ok || metadata["visible"] != "value" {
		t.Fatal("public detail response omitted visible metadata")
	} else if _, found := metadata[sandboxcfg.NsCredentials]; found {
		t.Fatal("public detail response exposed credentials metadata")
	}
	assertNoInternalCredentialFields(t, detailResponse)
}

func TestStubCreateRejectsUnavailableAPISecretMaterial(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{ID: "n1", StrictKeys: true}, svc)
	apiFingerprint := strings.Repeat("8", 64)
	manifestFingerprint := strings.Repeat("9", 64)
	put := &routesync.Command{
		CmdID: "key-ref", Kind: routesync.CmdKeyPut,
		APISecretFingerprint: apiFingerprint, APISecretType: "ref", APISecretRef: "secret://tenant/api",
		ManifestKeyFingerprint: manifestFingerprint, ManifestKeyType: "ref", ManifestKeyRef: "secret://tenant/manifest",
	}
	if got := node.HandleCommand(context.Background(), put); got.Status != routesync.AckAccepted {
		t.Fatalf("ref key_put ack = %+v", got)
	}
	create := &routesync.Command{
		CmdID: "create-ref", Kind: routesync.CmdCreate, SID: "node-sandbox",
		TemplateRef: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(), Profile: string(types.ProfileBare),
		APISecretFingerprint: apiFingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "/g", RouteKey: "rk", AuthSandboxID: "stable-sandbox"},
	}
	got := node.HandleCommand(context.Background(), create)
	if got.Status != routesync.AckRejected || got.Reason != "API secret material unavailable" {
		t.Fatalf("ref-backed create ack = %+v", got)
	}
	if node.getSandbox(create.SID) != nil {
		t.Fatal("ref-backed create inserted a sandbox")
	}
}

func assertNoInternalCredentialFields(t *testing.T, response map[string]any) {
	t.Helper()
	for _, field := range []string{
		"accessToken", "apiSecret", "apiSecretFingerprint", "manifestKey", "manifestKeyFingerprint",
		"serviceSecret", "authSandboxID", "nodeSandboxID", "execAccessToken", "cluster", "behavior",
	} {
		if _, ok := response[field]; ok {
			t.Fatalf("public response exposed %s", field)
		}
	}
}

func TestKeyPutRejectsHalfPairAndFingerprintRebinding(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{ID: "n1"}, svc)
	apiSecret := strings.Repeat("33", 32)
	manifestKey := strings.Repeat("44", 32)
	apiSecretFingerprint := testStubFingerprint(t, apiSecret)
	manifestKeyFingerprint := testStubFingerprint(t, manifestKey)

	half := &routesync.Command{
		CmdID: "key-half", Kind: routesync.CmdKeyPut,
		APISecretFingerprint: apiSecretFingerprint, APISecretType: "inline", APISecret: apiSecret,
	}
	if got := node.HandleCommand(context.Background(), half); got.Status != routesync.AckRejected {
		t.Fatalf("half key pair ack = %+v", got)
	}

	put := &routesync.Command{
		CmdID: "key-1", Kind: routesync.CmdKeyPut,
		APISecretFingerprint: apiSecretFingerprint, APISecretType: "inline", APISecret: apiSecret,
		ManifestKeyFingerprint: manifestKeyFingerprint, ManifestKeyType: "inline", ManifestKey: manifestKey,
		ExpiresUnix: time.Now().Add(time.Hour).Unix(),
	}
	if got := node.HandleCommand(context.Background(), put); got.Status != routesync.AckAccepted {
		t.Fatalf("initial key_put ack = %+v", got)
	}

	rebound := *put
	rebound.CmdID = "key-2"
	rebound.ManifestKey = strings.Repeat("55", 32)
	rebound.ManifestKeyFingerprint = testStubFingerprint(t, rebound.ManifestKey)
	if got := node.HandleCommand(context.Background(), &rebound); got.Status != routesync.AckRejected {
		t.Fatalf("rebound key_put ack = %+v", got)
	}
	node.mu.Lock()
	stored := node.keyPairs[apiSecretFingerprint]
	node.mu.Unlock()
	if stored.ManifestKeyFingerprint != manifestKeyFingerprint || stored.ManifestKey != manifestKey {
		t.Fatalf("rejected rebind changed stored pair: %+v", stored)
	}

	renewed := *put
	renewed.CmdID = "key-3"
	renewed.ExpiresUnix++
	if got := node.HandleCommand(context.Background(), &renewed); got.Status != routesync.AckAccepted {
		t.Fatalf("renewed key_put ack = %+v", got)
	}
	node.mu.Lock()
	stored = node.keyPairs[apiSecretFingerprint]
	node.mu.Unlock()
	if stored.ExpiresUnix != renewed.ExpiresUnix {
		t.Fatalf("renewal expiry = %d, want %d", stored.ExpiresUnix, renewed.ExpiresUnix)
	}
}

func newExecStubFixture(
	t *testing.T,
	state string,
	resumeDelay time.Duration,
) (*service, *stubNode, *stubSandbox, *routesync.Command) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{ID: "n1", CreateDelay: resumeDelay}, svc)
	svc.addNode(node)
	authSandboxID := "stable-sandbox"
	serviceSecret := strings.Repeat("ab", 32)
	forwardToken, err := keys.MintForwardAccessToken(serviceSecret, authSandboxID)
	if err != nil {
		t.Fatal(err)
	}
	cluster := &routesync.ClusterSandboxContext{
		Group: "/g", RouteKey: "rk", AuthSandboxID: authSandboxID,
	}
	sandbox := &stubSandbox{
		SID:                    "stable-sandbox-g0",
		Profile:                string(types.ProfileBare),
		State:                  state,
		TemplateID:             types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
		AuthSandboxID:          authSandboxID,
		APISecretFingerprint:   strings.Repeat("c", 64),
		ManifestKeyFingerprint: strings.Repeat("d", 64),
		ServiceSecret:          serviceSecret,
		ForwardAccessToken:     forwardToken,
		Cluster:                cloneStubClusterContext(cluster),
	}
	node.sandboxes[sandbox.SID] = sandbox
	command := &routesync.Command{
		CmdID: "exec-session-1", Kind: routesync.CmdExecSession, SID: sandbox.SID,
		Profile: sandbox.Profile, APISecretFingerprint: sandbox.APISecretFingerprint,
		Cluster: cloneStubClusterContext(cluster),
	}
	return svc, node, sandbox, command
}

func testStubFingerprint(t *testing.T, secretHex string) string {
	t.Helper()
	raw, err := hex.DecodeString(secretHex)
	if err != nil {
		t.Fatalf("decode test secret: %v", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
