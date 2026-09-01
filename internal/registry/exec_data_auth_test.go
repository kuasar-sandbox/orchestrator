package registry

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestReserveExecDataRejectsKATBeforePausedMutationOrCommand(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "stable", "n1", StatePaused)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	var commands atomic.Int32
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(*routesync.Command) { commands.Add(1) }})
	wrongSID, err := keys.MintExecAccessToken(record.ServiceSecret, "other-stable", 0)
	if err != nil {
		t.Fatal(err)
	}
	expired, err := keys.MintExecAccessToken(record.ServiceSecret, record.StableID, time.Now().Add(-time.Second).Unix())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		token string
	}{
		{name: "missing"},
		{name: "malformed", token: "not-a-kat"},
		{name: "expired", token: expired},
		{name: "wrong sid", token: wrongSID},
		{name: "wrong audience", token: record.ForwardAccessToken},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before, beforeRev, found, err := reg.stores.GetSandbox(ctx, record.Group, record.RouteKey)
			if err != nil || !found {
				t.Fatalf("read before reserve: found=%v err=%v", found, err)
			}
			_, err = reg.ReserveSandbox(ctx, SandboxReserveRequest{
				Operation: ReserveData, Group: record.Group, RouteKey: record.RouteKey,
				ExpectedSandboxID: record.SandboxID, Service: reserveDataServiceExec,
				AccessToken: test.token,
			})
			if !errors.Is(err, ErrReserveUnauthorized) {
				t.Fatalf("ReserveSandbox error = %v, want unauthorized", err)
			}
			after, afterRev, found, err := reg.stores.GetSandbox(ctx, record.Group, record.RouteKey)
			if err != nil || !found || beforeRev != afterRev || after.State != before.State || after.NodeSandboxID != before.NodeSandboxID {
				t.Fatalf("invalid KAT changed record: before=%+v/%d after=%+v/%d found=%v err=%v",
					before, beforeRev, after, afterRev, found, err)
			}
			if commands.Load() != 0 {
				t.Fatalf("invalid KAT dispatched %d CmdConnect commands", commands.Load())
			}
		})
	}
}

func TestReserveExecDataAcceptsKATAndIgnoresOptionalPortForAuth(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "stable", "n1", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "127.0.0.1:9443"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1"})
	token, err := keys.MintExecAccessToken(record.ServiceSecret, record.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, port := range []int{0, 49983, 8123} {
		result, err := reg.ReserveSandbox(ctx, SandboxReserveRequest{
			Operation: ReserveData, Group: record.Group, RouteKey: record.RouteKey,
			ExpectedSandboxID: record.SandboxID, Service: reserveDataServiceExec,
			Port: port, AccessToken: token,
		})
		if err != nil || result == nil || result.Route.NodeSandboxID != record.NodeSandboxID || result.Connect != nil {
			t.Fatalf("port %d result=%+v err=%v", port, result, err)
		}
	}
}

func TestRouteLinkReserveDataCarriesExecServiceHeaderWithoutBody(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "stable", "n1", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "127.0.0.1:9443"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1"})
	token, err := keys.MintExecAccessToken(record.ServiceSecret, record.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	request := func(service string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost,
			RouteLinkReservePath+"?operation=data&group=/g&route_key=rk&sid=stable&port=8123", nil)
		req.Header.Set("X-Access-Token", token)
		if service != "" {
			req.Header.Set("E2b-Sandbox-Service", service)
		}
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		return resp
	}
	if resp := request(reserveDataServiceExec); resp.Code != http.StatusOK {
		t.Fatalf("exec reserve status=%d body=%q", resp.Code, resp.Body.String())
	} else {
		var result ReserveResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil || result.Route.NodeSandboxID != record.NodeSandboxID {
			t.Fatalf("exec reserve response=%+v err=%v", result, err)
		}
	}
	if resp := request(""); resp.Code != http.StatusUnauthorized {
		t.Fatalf("service-less reserve accepted exec KAT: status=%d body=%q", resp.Code, resp.Body.String())
	}
}

func TestReserveRejectsExecServiceOnOtherOperations(t *testing.T) {
	record := testE2BSandboxRecord("/g", "rk", "stable", "n1", StateReady)
	for _, req := range []SandboxReserveRequest{
		{Operation: ReserveConnect, Group: record.Group, RouteKey: record.RouteKey, ExpectedSandboxID: record.SandboxID, APIKey: testAPIKeyValue(), Service: reserveDataServiceExec},
		{Operation: ReserveExecSession, Group: record.Group, RouteKey: record.RouteKey, ExpectedSandboxID: record.SandboxID, APIKey: testAPIKeyValue(), Service: reserveDataServiceExec},
		{Operation: ReserveData, Group: record.Group, RouteKey: record.RouteKey, ExpectedSandboxID: record.SandboxID, AccessToken: "token", Service: "unknown"},
	} {
		if _, err := testReg(t).ReserveSandbox(context.Background(), req); !errors.Is(err, ErrReserveBadRequest) {
			t.Fatalf("request %+v error=%v, want bad request", req, err)
		}
	}
}
