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

func TestSnapshotExportRedactsGroupSecrets(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	if err := reg.stores.PutGroup(ctx, &GroupConfig{
		Group:        "/g",
		ProjectID:    "p",
		ManifestKey:  testMK,
		AuthKey:      testAuthKey,
		RegistryAuth: `{"auths":{"r":{}}}`,
		TemplateRef:  "tmpl",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.stores.PutSandbox(ctx, &SandboxRecord{
		Group: "/g", RouteKey: "rk", SID: "sb-1", State: StateReady, NodeID: "n1", TemplateID: "tmpl",
	}); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	sum, err := reg.ExportSnapshot(ctx, &out, SnapshotOptions{Kind: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Groups != 1 || sum.Routes != 1 {
		t.Fatalf("summary=%+v, want 1 group and 1 route", sum)
	}
	if strings.Contains(out.String(), testMK) || strings.Contains(out.String(), testAuthKey) || strings.Contains(out.String(), "auths") {
		t.Fatalf("redacted export leaked a secret: %s", out.String())
	}
}

func TestSnapshotImportRoundTripInline(t *testing.T) {
	ctx := context.Background()
	src := testRegWithBox(t)
	if err := src.stores.PutGroup(ctx, &GroupConfig{
		Group: "/g", ProjectID: "p", ManifestKey: testMK, AuthKey: testAuthKey,
		SandboxConfig: map[string]string{"a": "1"}, NodeSelectors: []map[string]string{{"zone": "east"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := src.stores.PutSandbox(ctx, &SandboxRecord{
		Group: "/g", RouteKey: "rk", SID: "sb-1", State: StateReady, NodeID: "n1",
	}); err != nil {
		t.Fatal(err)
	}

	var raw bytes.Buffer
	if _, err := src.ExportSnapshot(ctx, &raw, SnapshotOptions{Kind: "all", IncludeSecrets: SnapshotIncludeSecretsInline}); err != nil {
		t.Fatal(err)
	}
	dst := testRegWithBox(t)
	sum, err := dst.ImportSnapshot(ctx, strings.NewReader(raw.String()))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Groups != 1 || sum.Routes != 1 {
		t.Fatalf("summary=%+v, want 1 group and 1 route", sum)
	}
	g, found, err := dst.stores.GetGroupByID(ctx, "/g")
	if err != nil || !found {
		t.Fatalf("group found=%v err=%v", found, err)
	}
	if g.ManifestKey != testMK || g.AuthKey != testAuthKey || g.SandboxConfig["a"] != "1" {
		t.Fatalf("imported group=%+v", g)
	}
	route, _, found, err := dst.stores.GetSandbox(ctx, "/g", "rk")
	if err != nil || !found {
		t.Fatalf("route found=%v err=%v", found, err)
	}
	if route.SID != "sb-1" || route.State != StateReady || route.NodeID != "n1" {
		t.Fatalf("imported route=%+v", route)
	}
	if rr, found, err := dst.ResolveSID(ctx, "sb-1"); err != nil || !found || rr.RouteKey != "rk" {
		t.Fatalf("sid index resolve=%+v found=%v err=%v", rr, found, err)
	}
}

func TestSnapshotRouteLinkAPI(t *testing.T) {
	ctx := context.Background()
	reg := testRegWithBox(t)
	if err := reg.stores.PutGroup(ctx, &GroupConfig{Group: "/g", ManifestKey: testMK, AuthKey: testAuthKey}); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + RouteLinkExportPath + "?kind=groups&include_secrets=inline")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("export status=%s", resp.Status)
	}
	var rec SnapshotRecord
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatal(err)
	}
	if rec.Type != SnapshotKindGroup || rec.Group == nil || rec.Group.ManifestKey != testMK {
		t.Fatalf("export record=%+v", rec)
	}

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(SnapshotRecord{Type: SnapshotKindRoute, Route: &SandboxRecord{
		Group: "/g", RouteKey: "rk", SID: "sb-1", State: StateReady,
	}}); err != nil {
		t.Fatal(err)
	}
	resp, err = http.Post(srv.URL+RouteLinkImportPath, "application/x-ndjson", &body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("import status=%s", resp.Status)
	}
	if route, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk"); err != nil || !found || route.SID != "sb-1" {
		t.Fatalf("imported route=%+v found=%v err=%v", route, found, err)
	}
}
