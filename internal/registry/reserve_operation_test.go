package registry

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func testConnectResult(record *SandboxRecord, nodeSandboxID string) *routesync.ConnectResult {
	return &routesync.ConnectResult{
		NodeSandboxID:      nodeSandboxID,
		TemplateID:         record.TemplateID,
		Profile:            record.Profile,
		EnvdAccessToken:    record.EnvdAccessToken,
		TrafficAccessToken: record.TrafficAccessToken,
		ForwardAccessToken: record.ForwardAccessToken,
	}
}

func testConnectReserve(record *SandboxRecord) SandboxReserveRequest {
	return SandboxReserveRequest{
		Operation: ReserveConnect, Group: record.Group, RouteKey: record.RouteKey,
		ExpectedSandboxID: record.SandboxID, APIKey: testAPIKeyValue(),
	}
}

func testDataReserve(record *SandboxRecord, port int, token string) SandboxReserveRequest {
	return SandboxReserveRequest{
		Operation: ReserveData, Group: record.Group, RouteKey: record.RouteKey,
		ExpectedSandboxID: record.SandboxID, Port: port, AccessToken: token,
	}
}

func TestReserveCreateAuthenticatesBeforePlacementOrRouteMutation(t *testing.T) {
	ctx := context.Background()
	placements := 0
	reg := testReg(t)
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return nil, ErrNoNode
	}))

	for _, req := range []SandboxReserveRequest{
		{Operation: ReserveCreate, Group: "/g", RouteKey: "missing"},
		{Operation: ReserveCreate, Group: "/g", RouteKey: "wrong", APIKey: "e2b_bad"},
	} {
		_, err := reg.ReserveSandbox(ctx, req)
		if req.APIKey == "" && !errors.Is(err, ErrReserveUnauthorized) {
			t.Fatalf("missing API key error=%v, want unauthorized", err)
		}
		if req.APIKey != "" && !errors.Is(err, ErrReserveForbidden) {
			t.Fatalf("wrong API key error=%v, want forbidden", err)
		}
		if _, _, found, getErr := reg.stores.GetSandbox(ctx, req.Group, req.RouteKey); getErr != nil || found {
			t.Fatalf("failed auth wrote route: found=%v err=%v", found, getErr)
		}
	}
	if placements != 0 {
		t.Fatalf("failed auth reached placement %d times", placements)
	}
}

func TestReserveCreateRollbackDoesNotCrossRecreatedLineage(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, ackStatus := range []string{routesync.AckAccepted, routesync.AckRejected} {
			name := "fresh"
			if existing {
				name = "paused"
			}
			t.Run(name+"/"+ackStatus, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				const (
					group    = "/g"
					routeKey = "rk"
				)

				reg := testReg(t)
				reg.SetPlacer(placementWithToken("n1"))
				if existing {
					original := testE2BSandboxRecord(group, routeKey, "sb-original", "n1", StatePaused)
					if _, err := reg.stores.PutSandbox(ctx, original); err != nil {
						t.Fatal(err)
					}
				}
				if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "n1:9443"}); err != nil {
					t.Fatal(err)
				}
				commands := make(chan *routesync.Command, 1)
				reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
					commands <- cmd
				}})

				done := make(chan error, 1)
				go func() {
					_, err := reg.ReserveSandbox(ctx, testCreateReserve(group, routeKey, nil))
					done <- err
				}()

				var command *routesync.Command
				select {
				case command = <-commands:
				case <-ctx.Done():
					t.Fatal("old reserve did not dispatch its lifecycle command")
				}
				old, _, found, err := reg.stores.GetSandbox(ctx, group, routeKey)
				if err != nil || !found || old.State != StateReserved {
					t.Fatalf("old reserved route=%+v found=%v err=%v", old, found, err)
				}
				if err := reg.stores.DeleteSandbox(ctx, group, routeKey); err != nil {
					t.Fatal(err)
				}
				replacement := testE2BSandboxRecord(group, routeKey, "sb-replacement", "n2", StateReserved)
				replacementRev, err := reg.stores.PutSandbox(ctx, replacement)
				if err != nil {
					t.Fatal(err)
				}

				reason := ""
				if ackStatus == routesync.AckRejected {
					reason = "old command rejected"
				}
				reg.ackCommand(&routesync.CmdAck{CmdID: command.CmdID, Status: ackStatus, Reason: reason})

				select {
				case err := <-done:
					if err == nil {
						t.Fatal("old reserve unexpectedly succeeded after route-key recreation")
					}
					if ackStatus == routesync.AckAccepted && !errors.Is(err, ErrSandboxNotFound) {
						t.Fatalf("old accepted reserve error=%v, want sandbox not found", err)
					}
				case <-ctx.Done():
					t.Fatal("old reserve did not stop after route-key recreation")
				}

				stored, storedRev, found, err := reg.stores.GetSandbox(context.Background(), group, routeKey)
				if err != nil || !found || storedRev != replacementRev ||
					stored.SandboxID != replacement.SandboxID || stored.NodeSandboxID != replacement.NodeSandboxID ||
					stored.SandboxGeneration != replacement.SandboxGeneration || stored.NodeID != replacement.NodeID ||
					stored.State != replacement.State {
					t.Fatalf("old rollback changed replacement: stored=%+v rev=%d found=%v err=%v, want=%+v rev=%d",
						stored, storedRev, found, err, replacement, replacementRev)
				}
			})
		}
	}
}

func TestReserveRejectsOperationSpecificAndOversizedFields(t *testing.T) {
	reg := testReg(t)
	tests := []SandboxReserveRequest{
		{Operation: ReserveCreate, Group: "/g", RouteKey: "rk", APIKey: testAPIKeyValue(), ExpectedSandboxID: "sb"},
		{Operation: ReserveCreate, Group: "/g", RouteKey: "rk", APIKey: testAPIKeyValue(), Port: 1},
		{Operation: ReserveCreate, Group: "/g", RouteKey: "rk", APIKey: testAPIKeyValue(), AccessToken: "token"},
		{Operation: ReserveConnect, Group: "/g", RouteKey: "rk", ExpectedSandboxID: "sb", APIKey: "key", Port: 1},
		{Operation: ReserveConnect, Group: "/g", RouteKey: "rk", ExpectedSandboxID: "sb", APIKey: "key", AccessToken: "token"},
		{Operation: ReserveData, Group: "/g", RouteKey: "rk", ExpectedSandboxID: "sb", AccessToken: "token", APIKey: "key"},
		{Operation: ReserveData, Group: "/g", RouteKey: "rk", ExpectedSandboxID: "sb", AccessToken: "token", TimeoutSeconds: 1},
		{Operation: ReserveConnect, Group: "/g", RouteKey: "rk", ExpectedSandboxID: "sb", APIKey: "key", MigrationToken: strings.Repeat("x", migrationtoken.MaxWireSize+1)},
	}
	for i, req := range tests {
		if _, err := reg.ReserveSandbox(context.Background(), req); !errors.Is(err, ErrReserveBadRequest) {
			t.Fatalf("case %d error=%v, want bad request", i, err)
		}
	}
}

func TestReserveConnectReadyReturnsTypedAckWithoutWaitingForReadyEvent(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "sb-connect", "n1", StateReady)
	initialRevision, err := reg.stores.PutSandbox(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "127.0.0.1:9443"}); err != nil {
		t.Fatal(err)
	}
	var command *routesync.Command
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		copy := *cmd
		command = &copy
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckAccepted,
			Connect: testConnectResult(record, cmd.SID),
		})
	}})
	req := testConnectReserve(record)
	req.TimeoutSeconds = 37
	req.MigrationToken = "kmt1.existing-target-is-node-validated"
	result, err := reg.ReserveSandbox(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if command == nil || command.Kind != routesync.CmdConnect || command.SID != record.NodeSandboxID ||
		command.TimeoutSeconds != 37 || command.MigrationToken != req.MigrationToken {
		t.Fatalf("connect command=%+v", command)
	}
	if result.Connect == nil || result.Connect.NodeSandboxID != record.NodeSandboxID ||
		result.Route.SandboxID != record.SandboxID || result.Route.NodeSandboxID != record.NodeSandboxID ||
		result.Route.State != string(StateReady) || result.Route.RouteRevision <= initialRevision {
		t.Fatalf("connect result=%+v", result)
	}
	stored, _, found, err := reg.stores.GetSandbox(ctx, record.Group, record.RouteKey)
	if err != nil || !found || stored.State != StateReady || stored.SandboxGeneration != 0 || stored.NextSandboxGeneration != 1 {
		t.Fatalf("ready connect changed identity/state: stored=%+v found=%v err=%v", stored, found, err)
	}
}

func TestReserveConnectReservedReturnsItsOwnTypedAckWithoutWaitingForRouteEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "sb-connect", "n1", StateReserved)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "127.0.0.1:9443"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckAccepted,
			Connect: testConnectResult(record, cmd.SID),
		})
	}})
	result, err := reg.ReserveSandbox(ctx, testConnectReserve(record))
	if err != nil {
		t.Fatal(err)
	}
	if result.Connect == nil || result.Route.State != string(StateReserved) ||
		result.Route.NodeSandboxID != record.NodeSandboxID {
		t.Fatalf("reserved connect result=%+v", result)
	}
}

func TestReserveConnectAuthenticatesStoredAPISecretWithoutSideEffects(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "sb-connect", "n1", StatePaused)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	commands := 0
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(*routesync.Command) { commands++ }})
	otherSecret := make([]byte, 32)
	for i := range otherSecret {
		otherSecret[i] = byte(i + 1)
	}
	wrongKey, err := apikey.Mint(otherSecret)
	if err != nil {
		t.Fatal(err)
	}
	for _, apiKey := range []string{"bad-format", wrongKey} {
		req := testConnectReserve(record)
		req.APIKey = apiKey
		if _, err := reg.ReserveSandbox(ctx, req); !errors.Is(err, ErrReserveForbidden) {
			t.Fatalf("API key %q error=%v, want forbidden", apiKey, err)
		}
	}
	stored, _, found, err := reg.stores.GetSandbox(ctx, record.Group, record.RouteKey)
	if err != nil || !found || stored.State != StatePaused || commands != 0 {
		t.Fatalf("failed auth side effects: stored=%+v commands=%d err=%v", stored, commands, err)
	}
}

func TestReserveConnectRejectRollsPausedStateBackWithoutConsumingGeneration(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "sb-connect", "n1", StatePaused)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, Reason: "resume refused"})
	}})
	if _, err := reg.ReserveSandbox(ctx, testConnectReserve(record)); err == nil {
		t.Fatal("rejected connect unexpectedly succeeded")
	}
	stored, _, found, err := reg.stores.GetSandbox(ctx, record.Group, record.RouteKey)
	if err != nil || !found || stored.State != StatePaused || stored.NodeSandboxID != record.NodeSandboxID ||
		stored.SandboxGeneration != 0 || stored.NextSandboxGeneration != 1 {
		t.Fatalf("connect rejection did not restore paused route: stored=%+v found=%v err=%v", stored, found, err)
	}
}

func TestReserveConnectMigrationRetriesCandidateWithHigherGeneration(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	original := testE2BSandboxRecord("/g", "rk", "sb-migrate", "gone", StatePaused)
	initialRevision, err := reg.stores.PutSandbox(ctx, original)
	if err != nil {
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
		mu.Lock()
		commands["n1"] = cmd
		mu.Unlock()
		go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, Reason: "candidate refused"})
	}})
	reg.addNode(&fakeConn{nodeID: "n2", onCmd: func(cmd *routesync.Command) {
		mu.Lock()
		commands["n2"] = cmd
		mu.Unlock()
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckAccepted,
			Connect: testConnectResult(original, cmd.SID),
		})
	}})
	placements := 0
	reg.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		placements++
		has := func(nodeID string) bool {
			for _, excluded := range req.ExcludeNodeIDs {
				if excluded == nodeID {
					return true
				}
			}
			return false
		}
		if !has(original.NodeID) {
			t.Fatalf("placement %d did not exclude original node: %+v", placements, req.ExcludeNodeIDs)
		}
		nodeID := "n1"
		if placements > 1 {
			if !has("n1") {
				t.Fatalf("retry did not exclude rejected node: %+v", req.ExcludeNodeIDs)
			}
			nodeID = "n2"
		}
		return &Placement{NodeID: nodeID, APISecretFingerprint: original.APISecretFingerprint}, nil
	}))
	req := testConnectReserve(original)
	req.MigrationToken = "kmt1.migration-ciphertext"
	result, err := reg.ReserveSandbox(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if placements != 2 || result.Connect == nil || result.Route.SandboxID != original.SandboxID ||
		result.Route.NodeSandboxID != EncodeNodeSandboxID(original.SandboxID, 2) || result.Route.NodeID != "n2" ||
		result.Route.RouteRevision <= initialRevision {
		t.Fatalf("migration result=%+v placements=%d", result, placements)
	}
	mu.Lock()
	n1, n2 := commands["n1"], commands["n2"]
	mu.Unlock()
	if n1 == nil || n1.SID != EncodeNodeSandboxID(original.SandboxID, 1) ||
		n2 == nil || n2.SID != EncodeNodeSandboxID(original.SandboxID, 2) ||
		n1.Kind != routesync.CmdConnect || n2.Kind != routesync.CmdConnect ||
		n1.MigrationToken != req.MigrationToken || n2.MigrationToken != req.MigrationToken {
		t.Fatalf("migration commands: n1=%+v n2=%+v", n1, n2)
	}
	stored, _, found, err := reg.stores.GetSandbox(ctx, original.Group, original.RouteKey)
	if err != nil || !found || stored.State != StateReserved || stored.SandboxGeneration != 2 ||
		stored.NextSandboxGeneration != 3 || stored.NodeSandboxID != n2.SID || !sameRouteCredentials(stored, original) {
		t.Fatalf("migration route=%+v found=%v err=%v", stored, found, err)
	}
}

func TestReserveConnectMigratesReservedTargetAfterNodeLoss(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	original := testE2BSandboxRecord("/g", "rk", "sb-migrate", "gone", StateReserved)
	if _, err := reg.stores.PutSandbox(ctx, original); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "n1:9443"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckAccepted,
			Connect: testConnectResult(original, cmd.SID),
		})
	}})
	reg.SetPlacer(placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		for _, nodeID := range req.ExcludeNodeIDs {
			if nodeID == original.NodeID {
				return &Placement{NodeID: "n1", APISecretFingerprint: original.APISecretFingerprint}, nil
			}
		}
		t.Fatalf("placement did not exclude lost reserved node: %+v", req.ExcludeNodeIDs)
		return nil, ErrNoNode
	}))
	req := testConnectReserve(original)
	req.MigrationToken = "kmt1.migration-ciphertext"
	result, err := reg.ReserveSandbox(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	wantNodeSID := EncodeNodeSandboxID(original.SandboxID, 1)
	if result.Connect == nil || result.Connect.NodeSandboxID != wantNodeSID ||
		result.Route.SandboxID != original.SandboxID || result.Route.NodeSandboxID != wantNodeSID ||
		result.Route.NodeID != "n1" || result.Route.DataEndpoint != "n1:9443" ||
		result.Route.State != string(StateReserved) {
		t.Fatalf("reserved migration result=%+v", result)
	}
}

func TestReserveConnectRequiresLiveDataEndpoint(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "sb-connect", "n1", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckAccepted,
			Connect: testConnectResult(record, cmd.SID),
		})
	}})
	if result, err := reg.ReserveSandbox(ctx, testConnectReserve(record)); !errors.Is(err, ErrNodeGone) || result != nil {
		t.Fatalf("connect without route target result=%+v err=%v, want node gone", result, err)
	}
}

func TestReserveDataReadyAuthenticatesTargetTokenAndRequiresEndpoint(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "sb-data", "n1", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "127.0.0.1:9443"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1"})

	result, err := reg.ReserveSandbox(ctx, testDataReserve(record, 49983, record.EnvdAccessToken))
	if err != nil || result.Route.DataEndpoint != "127.0.0.1:9443" || result.Route.RouteRevision <= 0 || result.Connect != nil {
		t.Fatalf("ready data result=%+v err=%v", result, err)
	}
	if _, err := reg.ReserveSandbox(ctx, testDataReserve(record, 49983, record.ForwardAccessToken)); !errors.Is(err, ErrReserveUnauthorized) {
		t.Fatalf("wrong target token error=%v, want unauthorized", err)
	}
	if _, err := reg.ReserveSandbox(ctx, testDataReserve(record, 8080, record.ForwardAccessToken)); err != nil {
		t.Fatalf("forward data token rejected: %v", err)
	}

	conn, _ := reg.node("n1")
	reg.removeNode(conn)
	if _, err := reg.ReserveSandbox(ctx, testDataReserve(record, 8080, record.ForwardAccessToken)); !errors.Is(err, ErrNodeGone) {
		t.Fatalf("ready route without runtime error=%v, want node gone", err)
	}
}

func TestReserveDataWaiterRetriesWhenConcurrentActivationRollsBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "sb-data", "n1", StatePaused)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "127.0.0.1:9443"}); err != nil {
		t.Fatal(err)
	}
	firstCommand := make(chan *routesync.Command, 1)
	releaseFirst := make(chan struct{})
	var mu sync.Mutex
	commands := 0
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		mu.Lock()
		commands++
		commandNumber := commands
		mu.Unlock()
		if commandNumber == 1 {
			firstCommand <- cmd
			go func() {
				<-releaseFirst
				reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, Reason: "retry"})
			}()
			return
		}
		go func() {
			reg.ackCommand(&routesync.CmdAck{
				CmdID: cmd.CmdID, Status: routesync.AckAccepted,
				Connect: testConnectResult(record, cmd.SID),
			})
			route := testRouteFromRecord(record, cmd.SID, routesync.StateRunning)
			route.TemplateID = record.TemplateID
			reg.applyRoute(context.Background(), "n1", &route)
		}()
	}})

	firstDone := make(chan error, 1)
	go func() {
		_, err := reg.ReserveSandbox(ctx, testDataReserve(record, 8080, record.ForwardAccessToken))
		firstDone <- err
	}()
	select {
	case <-firstCommand:
	case <-ctx.Done():
		t.Fatal("first data activation did not dispatch")
	}
	secondDone := make(chan error, 1)
	go func() {
		result, err := reg.ReserveSandbox(ctx, testDataReserve(record, 8080, record.ForwardAccessToken))
		if err == nil && (result == nil || result.Route.State != string(StateReady)) {
			err = fmt.Errorf("unexpected result: %+v", result)
		}
		secondDone <- err
	}()
	close(releaseFirst)
	if err := <-firstDone; err == nil {
		t.Fatal("rejected activation unexpectedly succeeded")
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("waiter did not retry rolled-back activation: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if commands != 2 {
		t.Fatalf("connect commands=%d, want reject then retry", commands)
	}
}

func TestReserveDataWaitDoesNotCrossDeletedLineage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reg := testReg(t)
	old := testE2BSandboxRecord("/g", "rk", "sb-old", "n1", StateReserved)
	if _, err := reg.stores.PutSandbox(ctx, old); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := reg.ReserveSandbox(ctx, testDataReserve(old, 8080, old.ForwardAccessToken))
		done <- err
	}()
	time.Sleep(25 * time.Millisecond)
	if err := reg.stores.DeleteSandbox(ctx, old.Group, old.RouteKey); err != nil {
		t.Fatal(err)
	}
	replacement := testE2BSandboxRecord(old.Group, old.RouteKey, "sb-new", "n2", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrSandboxNotFound) {
		t.Fatalf("old lineage waiter error=%v, want not found", err)
	}
}

func TestAuthenticateRecordAPIKeyUsesMACNotRawSecret(t *testing.T) {
	record := testE2BSandboxRecord("/g", "rk", "sb", "n1", StateReady)
	if err := authenticateRecordAPIKey(record, record.APISecret); !errors.Is(err, ErrReserveForbidden) {
		t.Fatalf("raw APISecret error=%v, want forbidden", err)
	}
	secret, err := hex.DecodeString(record.APISecret)
	if err != nil {
		t.Fatal(err)
	}
	apiKey, err := apikey.Mint(secret)
	if err != nil {
		t.Fatal(err)
	}
	if err := authenticateRecordAPIKey(record, apiKey); err != nil {
		t.Fatalf("minted API key rejected: %v", err)
	}
}
