package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
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
	sum, err := src.ExportSnapshot(ctx, &raw, SnapshotOptions{Kind: "route_link", Group: "/g"})
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
	if rr, found, err := dst.ResolveSID(ctx, "/g", "rk", "sb-1"); err != nil || !found || rr.RouteKey != "rk" || rr.AccessToken != "tok" {
		t.Fatalf("sid resolve=%+v found=%v err=%v", rr, found, err)
	}
}

func TestSnapshotIncludesBuildRecords(t *testing.T) {
	ctx := context.Background()
	src := testReg(t)
	if err := src.stores.PutBuild(ctx, &BuildRecord{
		Group: "/g", BuildID: "bld-1", NodeID: "n1", Resources: &routesync.BuildResources{CPU: 1000},
		State: BuildBuilding, TemplateID: "tmpl-1", Reason: "running", CreatedU: 99,
	}); err != nil {
		t.Fatal(err)
	}

	var raw bytes.Buffer
	sum, err := src.ExportSnapshot(ctx, &raw, SnapshotOptions{Kind: "route_link", Group: "/g"})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Builds != 1 {
		t.Fatalf("summary=%+v, want 1 build record", sum)
	}
	if !strings.Contains(raw.String(), `"type":"build"`) {
		t.Fatalf("snapshot did not export build record type: %s", raw.String())
	}

	dst := testReg(t)
	if _, err := dst.ImportSnapshot(ctx, strings.NewReader(raw.String())); err != nil {
		t.Fatal(err)
	}
	got, found, err := dst.stores.GetBuildInGroup(ctx, "/g", "bld-1")
	if err != nil || !found || got.State != BuildBuilding || got.Resources == nil || got.Resources.CPU != 1000 || got.CreatedU != 99 {
		t.Fatalf("imported build=%+v found=%v err=%v", got, found, err)
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

func TestSnapshotImportRejectsSameNodeSandboxIDCollision(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	existing := &SandboxRecord{
		Group: "/existing", RouteKey: "rk-existing", SID: "same", NodeID: "n1", State: StateReady,
	}
	if _, err := reg.stores.PutSandbox(ctx, existing); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", clusterstate.NodeSandboxRef{
		Group: existing.Group, RouteKey: existing.RouteKey, SandboxID: existing.SID,
	}); err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	if err := json.NewEncoder(&raw).Encode(SnapshotRecord{Type: SnapshotKindRoute, Route: &SandboxRecord{
		Group: "/imported", RouteKey: "rk-imported", SID: "same", NodeID: "n1", State: StateReady,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.ImportSnapshot(ctx, &raw); !errors.Is(err, errNodeSandboxIDConflict) {
		t.Fatalf("ImportSnapshot err=%v", err)
	}
	if ref, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", "same"); err != nil || !found || ref.Group != existing.Group || ref.RouteKey != existing.RouteKey {
		t.Fatalf("original ownership=%+v found=%v err=%v", ref, found, err)
	}
	if _, _, found, err := reg.stores.GetSandbox(ctx, "/imported", "rk-imported"); err != nil || found {
		t.Fatalf("conflicting imported route found=%v err=%v", found, err)
	}
}

func TestSnapshotImportRestoresExistingRouteOnOwnershipConflict(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	previous := &SandboxRecord{
		Group: "/target", RouteKey: "rk", SID: "old", NodeID: "n2", State: StateReady,
	}
	if _, err := reg.stores.PutSandbox(ctx, previous); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "n2", clusterstate.NodeSandboxRef{
		Group: previous.Group, RouteKey: previous.RouteKey, SandboxID: previous.SID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.stores.PutSandbox(ctx, &SandboxRecord{
		Group: "/owner", RouteKey: "rk-owner", SID: "same", NodeID: "n1", State: StateReady,
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", clusterstate.NodeSandboxRef{
		Group: "/owner", RouteKey: "rk-owner", SandboxID: "same",
	}); err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	if err := json.NewEncoder(&raw).Encode(SnapshotRecord{Type: SnapshotKindRoute, Route: &SandboxRecord{
		Group: previous.Group, RouteKey: previous.RouteKey, SID: "same", NodeID: "n1", State: StateReady,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.ImportSnapshot(ctx, &raw); !errors.Is(err, errNodeSandboxIDConflict) {
		t.Fatalf("ImportSnapshot err=%v", err)
	}
	got, _, found, err := reg.stores.GetSandbox(ctx, previous.Group, previous.RouteKey)
	if err != nil || !found || got.SID != previous.SID || got.NodeID != previous.NodeID {
		t.Fatalf("restored route=%+v found=%v err=%v", got, found, err)
	}
	if ref, found, err := reg.stores.GetNodeSandboxRef(ctx, "n2", previous.SID); err != nil || !found || ref.Group != previous.Group || ref.RouteKey != previous.RouteKey {
		t.Fatalf("previous ownership=%+v found=%v err=%v", ref, found, err)
	}
	if ref, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", "same"); err != nil || !found || ref.Group != "/owner" || ref.RouteKey != "rk-owner" {
		t.Fatalf("conflicting ownership=%+v found=%v err=%v", ref, found, err)
	}
}

func TestSnapshotImportReplacesExistingRouteOwnership(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	previous := &SandboxRecord{
		Group: "/target", RouteKey: "rk", SID: "old", NodeID: "n1", State: StateReady,
	}
	if _, err := reg.stores.PutSandbox(ctx, previous); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.AddNodeSandboxRef(ctx, previous.NodeID, clusterstate.NodeSandboxRef{
		Group: previous.Group, RouteKey: previous.RouteKey, SandboxID: previous.SID,
	}); err != nil {
		t.Fatal(err)
	}
	imported := &SandboxRecord{
		Group: previous.Group, RouteKey: previous.RouteKey, SID: "new", NodeID: "n2", State: StateReady,
	}
	var raw bytes.Buffer
	if err := json.NewEncoder(&raw).Encode(SnapshotRecord{Type: SnapshotKindRoute, Route: imported}); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.ImportSnapshot(ctx, &raw); err != nil {
		t.Fatal(err)
	}
	got, _, found, err := reg.stores.GetSandbox(ctx, imported.Group, imported.RouteKey)
	if err != nil || !found || got.SID != imported.SID || got.NodeID != imported.NodeID {
		t.Fatalf("imported route=%+v found=%v err=%v", got, found, err)
	}
	if ref, found, err := reg.stores.GetNodeSandboxRef(ctx, previous.NodeID, previous.SID); err != nil || found {
		t.Fatalf("previous ownership=%+v found=%v err=%v", ref, found, err)
	}
	if ref, found, err := reg.stores.GetNodeSandboxRef(ctx, imported.NodeID, imported.SID); err != nil || !found || ref.Group != imported.Group || ref.RouteKey != imported.RouteKey {
		t.Fatalf("imported ownership=%+v found=%v err=%v", ref, found, err)
	}
}

func TestSnapshotExportRequiresGroup(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	var raw bytes.Buffer
	if _, err := reg.ExportSnapshot(ctx, &raw, SnapshotOptions{Kind: "route_link"}); err == nil {
		t.Fatal("expected group-scoped export to require group")
	}
}
