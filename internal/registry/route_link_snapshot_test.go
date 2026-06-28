package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSnapshotRouteRoundTrip(t *testing.T) {
	ctx := context.Background()
	src := testReg(t)
	if _, err := src.stores.PutSandbox(ctx, &SandboxRecord{
		Group: "/g", RouteKey: "rk", SID: "sb-1", State: StateReady, NodeID: "n1", AccessToken: "tok",
	}); err != nil {
		t.Fatal(err)
	}

	var raw bytes.Buffer
	sum, err := src.ExportSnapshot(ctx, &raw, SnapshotOptions{Kind: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Routes != 1 {
		t.Fatalf("summary=%+v, want 1 route", sum)
	}

	dst := testReg(t)
	sum, err = dst.ImportSnapshot(ctx, strings.NewReader(raw.String()))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Routes != 1 {
		t.Fatalf("import summary=%+v, want 1 route", sum)
	}
	route, _, found, err := dst.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found {
		t.Fatalf("route found=%v err=%v", found, err)
	}
	if route.SID != "sb-1" || route.State != StateReady || route.NodeID != "n1" || route.AccessToken != "tok" {
		t.Fatalf("imported route=%+v", route)
	}
	if rr, found, err := dst.ResolveSID(ctx, "sb-1"); err != nil || !found || rr.RouteKey != "rk" || rr.AccessToken != "tok" {
		t.Fatalf("sid index resolve=%+v found=%v err=%v", rr, found, err)
	}
}

func TestSnapshotRouteLinkAPI(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(SnapshotRecord{Type: SnapshotKindRoute, Route: &SandboxRecord{
		Group: "/g", RouteKey: "rk", SID: "sb-1", State: StateReady, AccessToken: "tok",
	}}); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+RouteLinkImportPath, "application/x-ndjson", &body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("import status=%s", resp.Status)
	}
	if route, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk"); err != nil || !found || route.SID != "sb-1" || route.AccessToken != "tok" {
		t.Fatalf("imported route=%+v found=%v err=%v", route, found, err)
	}
}
