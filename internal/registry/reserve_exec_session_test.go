package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func testExecSessionReserve(record *SandboxRecord) SandboxReserveRequest {
	return SandboxReserveRequest{
		Operation: ReserveExecSession, Group: record.Group, RouteKey: record.RouteKey,
		ExpectedSandboxID: record.SandboxID, APIKey: testAPIKeyValue(),
	}
}

func TestServeReserveExecSessionCarriesQueryAndReturnsTypedResult(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "stable", "n1", StatePaused)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "n1:9443"}); err != nil {
		t.Fatal(err)
	}
	var command *routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		clone := *cmd
		command = &clone
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckAccepted,
			ExecSession: &routesync.ExecSessionResult{ExecAccessToken: "kat1.exec"},
		})
	}})
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	request := httptest.NewRequest(http.MethodPost,
		RouteLinkReservePath+"?group=/g&route_key=rk&operation=exec-session&sid=stable",
		strings.NewReader(`{"ttl_seconds":37,"conditions":["request.cwd == '/workspace'"]}`))
	request.Header.Set("X-API-KEY", testAPIKeyValue())
	request.Header.Set("X-Kuasar-Migration-Token", "kmt1.opaque")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", response.Code, response.Body.String())
	}
	var result ReserveResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.ExecSession == nil || result.ExecSession.ExecAccessToken != "kat1.exec" || result.Connect != nil ||
		result.Route.SandboxID != record.SandboxID || result.Route.NodeSandboxID != record.NodeSandboxID ||
		result.Route.State != string(StateReserved) || result.Route.RouteRevision <= 0 {
		t.Fatalf("reserve result = %+v", result)
	}
	if command == nil || command.Kind != routesync.CmdExecSession || command.SID != record.NodeSandboxID ||
		command.Profile != record.Profile || command.APISecretFingerprint != record.APISecretFingerprint ||
		command.TTLSeconds != 37 || command.MigrationToken != "kmt1.opaque" || command.TimeoutSeconds != 0 ||
		len(command.ExecConditions) != 1 || command.ExecConditions[0] != "request.cwd == '/workspace'" ||
		command.Cluster == nil || command.Cluster.Group != record.Group || command.Cluster.RouteKey != record.RouteKey ||
		command.Cluster.StableID != record.StableID {
		t.Fatalf("exec-session command = %+v", command)
	}
	if command.APISecretType != "" || command.APISecret != "" || command.APISecretRef != "" ||
		command.ManifestKeyFingerprint != "" || command.ManifestKeyType != "" || command.ManifestKey != "" ||
		command.ManifestKeyRef != "" || len(command.Config) != 0 {
		t.Fatalf("command carried credential roots or config: %+v", command)
	}
	if bytes.Contains(response.Body.Bytes(), []byte(testAPIKeyValue())) {
		t.Fatal("route-link result exposed the request API key")
	}
}

func TestReserveExecSessionAuthenticatesBeforeStateOrCommand(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "stable", "n1", StatePaused)
	initialRevision, err := reg.stores.PutSandbox(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "n1:9443"}); err != nil {
		t.Fatal(err)
	}
	commands := 0
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(*routesync.Command) { commands++ }})

	for _, test := range []struct {
		name      string
		apiKey    string
		sandboxID string
		want      error
	}{
		{name: "missing key", sandboxID: record.SandboxID, want: ErrReserveUnauthorized},
		{name: "wrong key", apiKey: "e2b_bad", sandboxID: record.SandboxID, want: ErrReserveForbidden},
		{name: "wrong stable identity", apiKey: testAPIKeyValue(), sandboxID: "replacement", want: ErrSandboxNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := testExecSessionReserve(record)
			req.APIKey = test.apiKey
			req.ExpectedSandboxID = test.sandboxID
			if result, err := reg.ReserveSandbox(ctx, req); !errors.Is(err, test.want) || result != nil {
				t.Fatalf("result = %+v, error = %v, want %v", result, err, test.want)
			}
		})
	}
	stored, revision, found, err := reg.stores.GetSandbox(ctx, record.Group, record.RouteKey)
	if err != nil || !found || revision != initialRevision || stored.State != StatePaused || commands != 0 {
		t.Fatalf("failed authentication side effects: stored=%+v revision=%d commands=%d found=%v err=%v",
			stored, revision, commands, found, err)
	}
}

func TestReserveExecSessionRejectsTTLOverflowBeforeStateOrCommand(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "stable", "n1", StatePaused)
	initialRevision, err := reg.stores.PutSandbox(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "n1:9443"}); err != nil {
		t.Fatal(err)
	}
	commands := 0
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(*routesync.Command) { commands++ }})

	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	request := httptest.NewRequest(http.MethodPost,
		RouteLinkReservePath+"?group=/g&route_key=rk&operation=exec-session&sid=stable&ttl_seconds=9223372036854775807", nil)
	request.Header.Set("X-API-KEY", testAPIKeyValue())
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %q", response.Code, response.Body.String())
	}
	stored, revision, found, err := reg.stores.GetSandbox(ctx, record.Group, record.RouteKey)
	if err != nil || !found || revision != initialRevision || stored.State != StatePaused ||
		stored.NodeID != record.NodeID || stored.NodeSandboxID != record.NodeSandboxID || commands != 0 {
		t.Fatalf("TTL overflow side effects: stored=%+v revision=%d commands=%d found=%v err=%v",
			stored, revision, commands, found, err)
	}
}

func TestReserveExecSessionRejectsTTLOutsideTimeRepresentationBeforeStateOrCommand(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "stable", "n1", StatePaused)
	initialRevision, err := reg.stores.PutSandbox(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "n1:9443"}); err != nil {
		t.Fatal(err)
	}
	commands := 0
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(*routesync.Command) { commands++ }})

	// This TTL fits int64 when added to the current Unix timestamp, but the
	// resulting instant exceeds time.Time's internal second representation.
	const unixToInternal = int64(62_135_596_800)
	ttlSeconds := int64(math.MaxInt64) - unixToInternal
	request := httptest.NewRequest(http.MethodPost,
		RouteLinkReservePath+"?group=/g&route_key=rk&operation=exec-session&sid=stable&ttl_seconds="+
			strconv.FormatInt(ttlSeconds, 10), nil)
	request.Header.Set("X-API-KEY", testAPIKeyValue())
	response := httptest.NewRecorder()
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %q", response.Code, response.Body.String())
	}
	stored, revision, found, err := reg.stores.GetSandbox(ctx, record.Group, record.RouteKey)
	if err != nil || !found || revision != initialRevision || stored.State != StatePaused ||
		stored.NodeID != record.NodeID || stored.NodeSandboxID != record.NodeSandboxID || commands != 0 {
		t.Fatalf("TTL representation overflow side effects: stored=%+v revision=%d commands=%d found=%v err=%v",
			stored, revision, commands, found, err)
	}
}

func TestReserveExecSessionAuthenticatesBeforeReplacementPlacement(t *testing.T) {
	ctx := context.Background()
	placements := 0
	reg := testReg(t)
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return nil, ErrNoNode
	}))
	record := testE2BSandboxRecord("/g", "rk", "stable", "gone", StatePaused)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	req := testExecSessionReserve(record)
	req.APIKey = "e2b_bad"
	req.MigrationToken = "kmt1.migration"
	if result, err := reg.ReserveSandbox(ctx, req); !errors.Is(err, ErrReserveForbidden) || result != nil {
		t.Fatalf("result = %+v, error = %v, want forbidden", result, err)
	}
	if placements != 0 {
		t.Fatalf("failed authentication reached placement %d times", placements)
	}
}

func TestReserveExecSessionIssuesEveryRequestWithoutWaitingForReady(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "stable", "n1", StatePaused)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "n1:9443"}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var commands []*routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		clone := *cmd
		mu.Lock()
		commands = append(commands, &clone)
		index := len(commands)
		mu.Unlock()
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckAccepted,
			ExecSession: &routesync.ExecSessionResult{ExecAccessToken: "kat1.exec-" + string(rune('0'+index))},
		})
	}})

	for index := 1; index <= 2; index++ {
		req := testExecSessionReserve(record)
		req.TTLSeconds = 37
		result, err := reg.ReserveSandbox(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		wantToken := "kat1.exec-" + string(rune('0'+index))
		if result.ExecSession == nil || result.ExecSession.ExecAccessToken != wantToken ||
			result.Route.State != string(StateReserved) {
			t.Fatalf("request %d result = %+v", index, result)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(commands) != 2 || commands[0].CmdID == commands[1].CmdID ||
		commands[0].Kind != routesync.CmdExecSession || commands[1].Kind != routesync.CmdExecSession {
		t.Fatalf("per-request commands = %+v", commands)
	}
}

func TestReserveExecSessionDoesNotMutateExistingReadyOrReservedWorkflow(t *testing.T) {
	for _, state := range []SandboxState{StateReady, StateReserved} {
		for _, accepted := range []bool{true, false} {
			t.Run(string(state)+"/accepted="+strconv.FormatBool(accepted), func(t *testing.T) {
				ctx := context.Background()
				reg := testReg(t)
				record := testE2BSandboxRecord("/g", "rk", "stable", "n1", state)
				initialRevision, err := reg.stores.PutSandbox(ctx, record)
				if err != nil {
					t.Fatal(err)
				}
				if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "n1:9443"}); err != nil {
					t.Fatal(err)
				}
				commands := 0
				reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
					commands++
					ack := &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, Reason: "refused"}
					if accepted {
						ack.Status = routesync.AckAccepted
						ack.ExecSession = &routesync.ExecSessionResult{ExecAccessToken: "kat1.exec"}
					}
					go reg.ackCommand(ack)
				}})

				result, reserveErr := reg.ReserveSandbox(ctx, testExecSessionReserve(record))
				if accepted {
					if reserveErr != nil || result == nil || result.ExecSession == nil ||
						result.ExecSession.ExecAccessToken != "kat1.exec" {
						t.Fatalf("result = %+v, error = %v", result, reserveErr)
					}
				} else if reserveErr == nil || result != nil {
					t.Fatalf("result = %+v, error = %v, want rejection", result, reserveErr)
				}

				stored, revision, found, err := reg.stores.GetSandbox(ctx, record.Group, record.RouteKey)
				if err != nil || !found || revision != initialRevision || commands != 1 ||
					stored.State != state || stored.NodeID != record.NodeID ||
					stored.NodeSandboxID != record.NodeSandboxID || stored.SandboxGeneration != record.SandboxGeneration {
					t.Fatalf("exec-session disturbed existing workflow: stored=%+v revision=%d initial=%d commands=%d found=%v err=%v",
						stored, revision, initialRevision, commands, found, err)
				}
			})
		}
	}
}

func TestReserveExecSessionRejectsInvalidTypedResultAndRollsBack(t *testing.T) {
	for _, test := range []struct {
		name string
		ack  func(string) *routesync.CmdAck
	}{
		{name: "missing result", ack: func(id string) *routesync.CmdAck {
			return &routesync.CmdAck{CmdID: id, Status: routesync.AckAccepted}
		}},
		{name: "empty token", ack: func(id string) *routesync.CmdAck {
			return &routesync.CmdAck{CmdID: id, Status: routesync.AckAccepted, ExecSession: &routesync.ExecSessionResult{}}
		}},
		{name: "rejected", ack: func(id string) *routesync.CmdAck {
			return &routesync.CmdAck{CmdID: id, Status: routesync.AckRejected, Reason: "refused"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			reg := testReg(t)
			record := testE2BSandboxRecord("/g", "rk", "stable", "n1", StatePaused)
			if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
				t.Fatal(err)
			}
			if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "n1:9443"}); err != nil {
				t.Fatal(err)
			}
			reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
				go reg.ackCommand(test.ack(cmd.CmdID))
			}})
			if result, err := reg.ReserveSandbox(ctx, testExecSessionReserve(record)); err == nil || result != nil {
				t.Fatalf("result = %+v, error = %v, want failure", result, err)
			}
			stored, _, found, err := reg.stores.GetSandbox(ctx, record.Group, record.RouteKey)
			if err != nil || !found || stored.State != StatePaused || stored.NodeSandboxID != record.NodeSandboxID {
				t.Fatalf("failed command rollback: stored=%+v found=%v err=%v", stored, found, err)
			}
		})
	}
}

func TestReserveExecSessionReplacesLostNodeWithMigrationToken(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	original := testE2BSandboxRecord("/g", "rk", "stable", "gone", StatePaused)
	if _, err := reg.stores.PutSandbox(ctx, original); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "n1:9443"}); err != nil {
		t.Fatal(err)
	}
	var command *routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		clone := *cmd
		command = &clone
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckAccepted,
			ExecSession: &routesync.ExecSessionResult{ExecAccessToken: "kat1.migrated"},
		})
	}})
	placements := 0
	reg.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		placements++
		foundGone := false
		for _, nodeID := range req.ExcludeNodeIDs {
			foundGone = foundGone || nodeID == original.NodeID
		}
		if !foundGone || req.SandboxID != original.SandboxID {
			t.Fatalf("replacement request = %+v", req)
		}
		return &Placement{NodeID: "n1", APISecretFingerprint: original.APISecretFingerprint}, nil
	}))
	if result, err := reg.ReserveSandbox(ctx, testExecSessionReserve(original)); !errors.Is(err, ErrNodeGone) || result != nil {
		t.Fatalf("replacement without migration result = %+v, error = %v, want node gone", result, err)
	}
	if placements != 0 {
		t.Fatalf("replacement without migration reached placement %d times", placements)
	}
	req := testExecSessionReserve(original)
	req.TTLSeconds = 37
	req.MigrationToken = "kmt1.migration"
	result, err := reg.ReserveSandbox(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	wantNodeSandboxID := EncodeNodeSandboxID(original.SandboxID, 1)
	if placements != 1 || result.ExecSession == nil || result.ExecSession.ExecAccessToken != "kat1.migrated" ||
		result.Route.SandboxID != original.SandboxID || result.Route.NodeSandboxID != wantNodeSandboxID ||
		result.Route.NodeID != "n1" || result.Route.State != string(StateReserved) {
		t.Fatalf("replacement result = %+v, placements = %d", result, placements)
	}
	if command == nil || command.Kind != routesync.CmdExecSession || command.SID != wantNodeSandboxID ||
		command.MigrationToken != req.MigrationToken || command.TTLSeconds != req.TTLSeconds {
		t.Fatalf("replacement command = %+v", command)
	}
	stored, _, found, err := reg.stores.GetSandbox(ctx, original.Group, original.RouteKey)
	if err != nil || !found || stored.NodeSandboxID != wantNodeSandboxID || stored.SandboxGeneration != 1 ||
		stored.NextSandboxGeneration != 2 || stored.State != StateReserved || !sameRouteCredentials(stored, original) {
		t.Fatalf("replacement record = %+v, found=%v, err=%v", stored, found, err)
	}
}

func TestReserveExecSessionReplacementRetriesRejectedCandidate(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	original := testE2BSandboxRecord("/g", "rk", "stable", "gone", StatePaused)
	if _, err := reg.stores.PutSandbox(ctx, original); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"n1", "n2"} {
		if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: nodeID, DataEndpoint: nodeID + ":9443"}); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	commands := map[string]*routesync.Command{}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		clone := *cmd
		mu.Lock()
		commands["n1"] = &clone
		mu.Unlock()
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckRejected,
			Reason: "target environment incompatible", HTTPStatus: http.StatusConflict,
		})
	}})
	reg.addNode(&fakeConn{nodeID: "n2", onCmd: func(cmd *routesync.Command) {
		clone := *cmd
		mu.Lock()
		commands["n2"] = &clone
		mu.Unlock()
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckAccepted,
			ExecSession: &routesync.ExecSessionResult{ExecAccessToken: "kat1.retry"},
		})
	}})
	placements := 0
	reg.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		placements++
		has := func(want string) bool {
			for _, nodeID := range req.ExcludeNodeIDs {
				if nodeID == want {
					return true
				}
			}
			return false
		}
		if !has(original.NodeID) {
			t.Fatalf("placement %d did not exclude lost node: %+v", placements, req.ExcludeNodeIDs)
		}
		if placements == 1 {
			return &Placement{NodeID: "n1", APISecretFingerprint: original.APISecretFingerprint}, nil
		}
		if !has("n1") {
			t.Fatalf("retry did not exclude rejected candidate: %+v", req.ExcludeNodeIDs)
		}
		return &Placement{NodeID: "n2", APISecretFingerprint: original.APISecretFingerprint}, nil
	}))
	req := testExecSessionReserve(original)
	req.MigrationToken = "kmt1.migration"
	result, err := reg.ReserveSandbox(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if placements != 2 || result.ExecSession == nil || result.ExecSession.ExecAccessToken != "kat1.retry" ||
		result.Route.NodeID != "n2" || result.Route.NodeSandboxID != EncodeNodeSandboxID(original.SandboxID, 2) {
		t.Fatalf("retry result = %+v, placements = %d", result, placements)
	}
	mu.Lock()
	n1, n2 := commands["n1"], commands["n2"]
	mu.Unlock()
	if n1 == nil || n1.SID != EncodeNodeSandboxID(original.SandboxID, 1) ||
		n2 == nil || n2.SID != EncodeNodeSandboxID(original.SandboxID, 2) ||
		n1.Kind != routesync.CmdExecSession || n2.Kind != routesync.CmdExecSession {
		t.Fatalf("replacement commands: n1=%+v n2=%+v", n1, n2)
	}
	stored, _, found, err := reg.stores.GetSandbox(ctx, original.Group, original.RouteKey)
	if err != nil || !found || stored.SandboxGeneration != 2 || stored.NextSandboxGeneration != 3 ||
		stored.NodeSandboxID != n2.SID || stored.State != StateReserved {
		t.Fatalf("replacement record = %+v, found=%v err=%v", stored, found, err)
	}
}

func TestReserveExecSessionReplacementStopsOnRequestWideNodeRejection(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	original := testE2BSandboxRecord("/g", "rk", "stable", "gone", StatePaused)
	if _, err := reg.stores.PutSandbox(ctx, original); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"n1", "n2"} {
		if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: nodeID, DataEndpoint: nodeID + ":9443"}); err != nil {
			t.Fatal(err)
		}
	}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckRejected,
			Reason: "invalid migration token", HTTPStatus: http.StatusBadRequest,
		})
	}})
	secondCommands := 0
	reg.addNode(&fakeConn{nodeID: "n2", onCmd: func(*routesync.Command) {
		secondCommands++
	}})
	placements := 0
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		nodeID := "n1"
		if placements > 1 {
			nodeID = "n2"
		}
		return &Placement{NodeID: nodeID, APISecretFingerprint: original.APISecretFingerprint}, nil
	}))

	req := testExecSessionReserve(original)
	req.MigrationToken = "kmt1.invalid-for-all-candidates"
	result, err := reg.ReserveSandbox(ctx, req)
	var rejected *nodeCommandRejection
	if result != nil || !errors.As(err, &rejected) || rejected.status != http.StatusBadRequest ||
		rejected.reason != "invalid migration token" {
		t.Fatalf("result=%+v err=%v rejection=%+v", result, err, rejected)
	}
	if placements != 1 || secondCommands != 0 {
		t.Fatalf("terminal rejection placements=%d secondCommands=%d, want 1/0", placements, secondCommands)
	}
	stored, _, found, err := reg.stores.GetSandbox(ctx, original.Group, original.RouteKey)
	if err != nil || !found || stored.NodeID != original.NodeID ||
		stored.NodeSandboxID != original.NodeSandboxID || stored.SandboxGeneration != original.SandboxGeneration ||
		stored.NextSandboxGeneration != 2 || stored.State != original.State {
		t.Fatalf("terminal rejection rollback=%+v found=%v err=%v", stored, found, err)
	}
}

func TestReserveExecSessionReturnsRouteCurrentAfterAck(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "stable", "n1", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"n1", "n2"} {
		if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: nodeID, DataEndpoint: nodeID + ":9443"}); err != nil {
			t.Fatal(err)
		}
	}
	reg.addNode(&fakeConn{nodeID: "n2"})
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		current := *record
		current.NodeID = "n2"
		current.NodeSandboxID = EncodeNodeSandboxID(record.SandboxID, 1)
		current.SandboxGeneration = 1
		current.NextSandboxGeneration = 2
		current.State = StateReserved
		if _, err := reg.stores.PutSandbox(ctx, &current); err != nil {
			t.Error(err)
		}
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckAccepted,
			ExecSession: &routesync.ExecSessionResult{ExecAccessToken: "kat1.current"},
		})
	}})
	result, err := reg.ReserveSandbox(ctx, testExecSessionReserve(record))
	if err != nil {
		t.Fatal(err)
	}
	if result.Route.NodeID != "n2" || result.Route.NodeSandboxID != EncodeNodeSandboxID(record.SandboxID, 1) ||
		result.Route.DataEndpoint != "n2:9443" || result.Route.State != string(StateReserved) {
		t.Fatalf("result did not use current route: %+v", result)
	}
}

func TestReserveExecSessionRejectsForeignFieldsAndRouteLinkBody(t *testing.T) {
	record := testE2BSandboxRecord("/g", "rk", "stable", "n1", StatePaused)
	for index, req := range []SandboxReserveRequest{
		{Operation: ReserveCreate, Group: record.Group, RouteKey: record.RouteKey, APIKey: testAPIKeyValue(), TTLSeconds: 1},
		{Operation: ReserveConnect, Group: record.Group, RouteKey: record.RouteKey, ExpectedSandboxID: record.SandboxID, APIKey: testAPIKeyValue(), TTLSeconds: 1},
		{Operation: ReserveData, Group: record.Group, RouteKey: record.RouteKey, ExpectedSandboxID: record.SandboxID, AccessToken: "token", TTLSeconds: 1},
		{Operation: ReserveExecSession, Group: record.Group, RouteKey: record.RouteKey, ExpectedSandboxID: record.SandboxID, APIKey: testAPIKeyValue(), Port: 1},
		{Operation: ReserveExecSession, Group: record.Group, RouteKey: record.RouteKey, ExpectedSandboxID: record.SandboxID, APIKey: testAPIKeyValue(), TimeoutSeconds: 1},
		{Operation: ReserveExecSession, Group: record.Group, RouteKey: record.RouteKey, ExpectedSandboxID: record.SandboxID, APIKey: testAPIKeyValue(), AccessToken: "token"},
		{Operation: ReserveExecSession, Group: record.Group, RouteKey: record.RouteKey, ExpectedSandboxID: record.SandboxID, APIKey: testAPIKeyValue(), TTLSeconds: -1},
	} {
		if _, err := testReg(t).ReserveSandbox(context.Background(), req); !errors.Is(err, ErrReserveBadRequest) {
			t.Fatalf("case %d error = %v, want bad request", index, err)
		}
	}

	reg := testReg(t)
	if _, err := reg.stores.PutSandbox(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	for _, rawQuery := range []string{
		"group=/g&route_key=rk&operation=exec-session&sid=stable&ttl_seconds=-1",
		"group=/g&route_key=rk&operation=exec-session&sid=stable&ttl_seconds=9223372036854775808",
	} {
		request := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?"+rawQuery, nil)
		request.Header.Set("X-API-KEY", testAPIKeyValue())
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("query %q status = %d, body = %q", rawQuery, response.Code, response.Body.String())
		}
	}
	for _, body := range []string{
		`null`,
		`{"unknown":true}`,
		`{"ttl_seconds":-1}`,
		`{"ttl_seconds":0,"conditions":null}`,
		`{"ttl_seconds":0,"conditions":[""]}`,
		`{"ttl_seconds":0,"ttl_seconds":1}`,
	} {
		request := httptest.NewRequest(http.MethodPost,
			RouteLinkReservePath+"?group=/g&route_key=rk&operation=exec-session&sid=stable", strings.NewReader(body))
		request.Header.Set("X-API-KEY", testAPIKeyValue())
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			responseBody, _ := io.ReadAll(response.Result().Body)
			t.Fatalf("exec-session route-link body %q status = %d, body = %q", body, response.Code, responseBody)
		}
	}
}
