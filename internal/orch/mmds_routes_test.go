package orch

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const testMMDSSpec = `{"version":1,"routes":[{"path":"/x","type":"static","data":"d"}]}`

func assertMMDSBadRequestBoundary(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("error = %v, want api.ErrBadRequest", err)
	}
	if errors.Is(err, api.ErrTargetIncompatible) {
		t.Fatalf("error = %v, must not classify MMDS validation as api.ErrTargetIncompatible", err)
	}
	var validationErr *sandboxcfg.MMDSValidationError
	if errors.As(err, &validationErr) {
		t.Fatalf("error = %v, MMDS validation type crossed the bad-request boundary", err)
	}
}

func testMMDSTemplateRef() string {
	return types.TemplateID{
		Profile: types.ProfileBare,
		Kind:    types.KindImg,
		Ref:     "manifest://" + strings.Repeat("a", 64),
	}.String()
}

func testMMDSRoutesConfig() config.MMDSRoutesConfig {
	return config.MMDSRoutesConfig{
		Enabled:             true,
		MaxRoutesPerSandbox: 32,
		MaxNamespaceBytes:   65536,
		Secret:              config.MMDSSecretRoutesConfig{MaxPerSandbox: 16},
		Service:             config.MMDSServiceRoutesConfig{MaxPerSandbox: 16},
		Static:              config.MMDSStaticRoutesConfig{MaxBodyBytes: 16384},
	}
}

// TestPrecheckClusterAppliesNodeLocalMMDSRoutePolicy proves cluster create
// now applies this node's own mmds.routes policy (registry has no
// registry-owned policy of its own, so this is the only admission cluster
// mode has): with the policy left at its zero value (Enabled=false), a
// declaration that would otherwise be well-formed is rejected exactly like a
// standalone create's identical declaration would be.
func TestPrecheckClusterAppliesNodeLocalMMDSRoutePolicy(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Enabled = true
	// MMDS.Routes deliberately left disabled (zero value).
	o := testOrchCfg(t, cfg)
	o.SetMMDSRuntimeAvailable(true)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := &routesync.Command{
		SID:                  "stable-g0",
		TemplateRef:          testMMDSTemplateRef(),
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		Config:               map[string]string{sandboxcfg.NsMMDS: testMMDSSpec},
	}
	_, _, _, err := o.precheckCluster(context.Background(), cmd)
	assertMMDSBadRequestBoundary(t, err)
	if !strings.Contains(err.Error(), "MMDS metadata: MMDS routes are disabled by policy") {
		t.Fatalf("error = %v, want the disabled-by-policy detail", err)
	}
}

func TestHandleCommandCreateRejectsUnavailableMMDSWithHTTP409(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Enabled = true
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	cfg.Proxy.Mode = config.ProxyExternal
	o := testOrchCfg(t, cfg)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	ack := o.HandleCommand(context.Background(), &routesync.Command{
		Kind:                 routesync.CmdCreate,
		CmdID:                "create-invalid-mmds",
		SID:                  "stable-g0",
		TemplateRef:          testMMDSTemplateRef(),
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		Config:               map[string]string{sandboxcfg.NsMMDS: testMMDSSpec},
	})
	if ack.Status != routesync.AckRejected || ack.HTTPStatus != http.StatusConflict ||
		ack.Reason != api.ErrTargetIncompatible.Error() {
		t.Fatalf("ack = %+v, want retryable HTTP 409", ack)
	}
}

// TestHandleCommandMigrationImportAcceptsUnavailableMMDSByKeepingMetadataDormant
// proves a cluster connect/exec-session import no longer rejects (with 409,
// prompting the registry to retry another node) just because the target node
// has no MMDS runtime capability: the token's own carried-forward
// kuasar-sandbox.mmds is only checked against this node's mmds.routes policy
// at import time, never gated on MMDSRuntimeAvailable -- it's not the
// tenant's active input for this specific request, so persisting it dormant
// and letting admittedMMDSMetadata's own read-time capability gate keep it
// unserved until capability returns is correct; failing/retrying the whole
// reconnect over it would be wrong.
func TestHandleCommandMigrationImportAcceptsUnavailableMMDSByKeepingMetadataDormant(t *testing.T) {
	for _, kind := range []string{routesync.CmdConnect, routesync.CmdExecSession} {
		t.Run(string(kind), func(t *testing.T) {
			fixture := newClusterConnectFixture(t)
			fixture.o.cfg.MMDS.Routes = testMMDSRoutesConfig()
			fixture.source.Metadata[sandboxcfg.NsMMDS] = testMMDSSpec
			token, err := fixture.o.mintSandboxToken(fixture.source, fixture.source.SnapshotRef)
			if err != nil {
				t.Fatal(err)
			}
			cmd := fixture.command("stable-g1", token)
			cmd.Kind = kind
			if kind == routesync.CmdExecSession {
				cmd.TTLSeconds = 30
			}

			ack := fixture.o.HandleCommand(context.Background(), cmd)
			if ack.Status != routesync.AckAccepted {
				t.Fatalf("ack = %+v, want accepted (MMDS runtime unavailability must not block an import of the token's own declaration)", ack)
			}
			stored, err := fixture.o.st.Get(context.Background(), "stable-g1")
			if err != nil {
				t.Fatal(err)
			}
			if _, found := stored.Metadata[sandboxcfg.NsMMDS]; !found {
				t.Fatal("the migration token's own MMDS metadata was dropped from the imported row")
			}
		})
	}
}

func TestPrepareClusterConnectKeepsTokenMMDSDespiteUnavailableRuntime(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	fixture.o.cfg.MMDS.Routes = testMMDSRoutesConfig()
	fixture.source.Metadata[sandboxcfg.NsMMDS] = testMMDSSpec
	token, err := fixture.o.mintSandboxToken(fixture.source, fixture.source.SnapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := fixture.o.prepareClusterConnect(context.Background(), fixture.command("stable-g1", token), 0)
	if err != nil {
		t.Fatalf("prepareClusterConnect = %v, want success (the token's own value is never gated on runtime capability)", err)
	}
	if _, found := sb.Metadata[sandboxcfg.NsMMDS]; !found {
		t.Fatal("the migration token's own MMDS metadata was dropped from the connected row")
	}
}

// TestPrepareClusterConnectKeepsTokenMMDS proves the migration token's own
// carried-forward declaration is what a cluster connect/exec-session import
// ends up with -- there is no redeclaration mechanism on the request itself
// to override it.
func TestPrepareClusterConnectKeepsTokenMMDS(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	fixture.o.cfg.MMDS.Routes = testMMDSRoutesConfig()
	fixture.source.Metadata[sandboxcfg.NsMMDS] = testMMDSSpec
	token, err := fixture.o.mintSandboxToken(fixture.source, fixture.source.SnapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	fixture.o.cfg.MMDS.Enabled = true // SetMMDSRuntimeAvailable(true) is a no-op unless this is also set
	fixture.o.SetMMDSRuntimeAvailable(true)
	cmd := fixture.command("stable-g1", token)

	sb, err := fixture.o.prepareClusterConnect(context.Background(), cmd, 0)
	if err != nil {
		t.Fatalf("prepareClusterConnect = %v, want success", err)
	}
	got, found := sb.Metadata[sandboxcfg.NsMMDS]
	if !found {
		t.Fatal("the migration token's own MMDS metadata did not survive import")
	}
	if !strings.Contains(got, "/x") {
		t.Fatalf("imported row metadata = %q, want the token's own spec", got)
	}
}

func TestHandleCommandCreateRejectsMalformedMMDSWithHTTP400BeforeCapability(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	o := testOrchCfg(t, cfg)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	ack := o.HandleCommand(context.Background(), &routesync.Command{
		Kind:                 routesync.CmdCreate,
		CmdID:                "create-invalid-mmds",
		SID:                  "stable-g0",
		TemplateRef:          testMMDSTemplateRef(),
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		Config:               map[string]string{sandboxcfg.NsMMDS: `{"version":1,"routes":[`},
	})
	if ack.Status != routesync.AckRejected || ack.HTTPStatus != http.StatusBadRequest ||
		!strings.Contains(ack.Reason, "MMDS metadata: is not valid JSON") {
		t.Fatalf("ack = %+v, want structural HTTP 400", ack)
	}
}

func TestPrecheckClusterMMDSValidationHasSingleErrorClassification(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	o := testOrchCfg(t, cfg)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := &routesync.Command{
		Kind:                 routesync.CmdCreate,
		SID:                  "stable-g0",
		TemplateRef:          testMMDSTemplateRef(),
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		Config:               map[string]string{sandboxcfg.NsMMDS: `{"version":1,"routes":[`},
	}
	_, _, _, err := o.precheckCluster(context.Background(), cmd)
	assertMMDSBadRequestBoundary(t, err)
	if !strings.Contains(err.Error(), "MMDS metadata: is not valid JSON") {
		t.Fatalf("error = %v, want actionable public MMDS validation detail", err)
	}
}

func TestPrecheckClusterCanonicalizesMMDS(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Enabled = true
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	o := testOrchCfg(t, cfg)
	o.SetMMDSRuntimeAvailable(true)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := &routesync.Command{
		SID:                  "stable-g0",
		TemplateRef:          testMMDSTemplateRef(),
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		// Static shorthand (no explicit "type") -- must come back canonicalized.
		Config: map[string]string{sandboxcfg.NsMMDS: `{"version":1,"routes":[{"path":"/x","data":"d"}]}`},
	}
	if _, _, _, err := o.precheckCluster(context.Background(), cmd); err != nil {
		t.Fatalf("precheckCluster: %v", err)
	}
	got := cmd.Config[sandboxcfg.NsMMDS]
	if !strings.Contains(got, `"type":"static"`) {
		t.Fatalf("expected canonicalized static type in persisted config, got %q", got)
	}
	if !strings.Contains(got, `"content_type":"application/octet-stream"`) {
		t.Fatalf("expected default content_type filled in, got %q", got)
	}
}

func TestMigrationSandboxMetadataPreservesMMDS(t *testing.T) {
	meta := map[string]string{
		sandboxcfg.NsMMDS:        testMMDSSpec,
		sandboxcfg.NsCredentials: `{"service_secret":"x"}`,
		"keep":                   "value",
	}
	got := migrationSandboxMetadata(meta)
	if got[sandboxcfg.NsMMDS] != testMMDSSpec {
		t.Fatalf("mmds specification did not survive migration metadata scrubbing: %+v", got)
	}
	if _, ok := got[sandboxcfg.NsCredentials]; ok {
		t.Fatalf("credentials leaked into migration metadata: %+v", got)
	}
	if got["keep"] != "value" {
		t.Fatalf("unrelated metadata was dropped: %+v", got)
	}
}

func TestClusterSandboxMetadataPreservesMMDS(t *testing.T) {
	config := map[string]string{
		sandboxcfg.NsMMDS:        testMMDSSpec,
		sandboxcfg.NsCredentials: `{"service_secret":"x"}`,
		"keep":                   "value",
	}
	got := clusterSandboxMetadata(config)
	if got[sandboxcfg.NsMMDS] != testMMDSSpec {
		t.Fatalf("mmds specification did not survive cluster metadata scrubbing: %+v", got)
	}
	if _, ok := got[sandboxcfg.NsCredentials]; ok {
		t.Fatalf("credentials leaked into cluster metadata: %+v", got)
	}
	if got["keep"] != "value" {
		t.Fatalf("unrelated metadata was dropped: %+v", got)
	}
}

func TestCreateRejectsMMDSWhenPolicyDisabled(t *testing.T) {
	root := t.TempDir()
	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(root, "run")
	cfg.Paths.BaseRoot = filepath.Join(root, "base")
	// MMDSRoutes.Enabled defaults to false (zero value) -- deliberately not set.
	o := testOrchCfg(t, cfg)
	o.vs = stubVS{}
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	_, err := o.Create(context.Background(), api.CreateReq{
		APIKey: apiKey, TemplateID: testMMDSTemplateRef(), TimeoutSec: 60,
		Metadata: map[string]string{sandboxcfg.NsMMDS: testMMDSSpec},
	})
	assertMMDSBadRequestBoundary(t, err)
	if !strings.Contains(err.Error(), "MMDS metadata: MMDS routes are disabled by policy") {
		t.Fatalf("Create error = %v, want actionable public MMDS validation detail", err)
	}
	for _, path := range []string{cfg.Paths.RunRoot, cfg.Paths.BaseRoot} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("disabled-policy mmds metadata created %s: stat error = %v", path, err)
		}
	}
}

// TestCreateRejectsMMDSWhenRuntimeUnavailable proves that a standalone create
// is rejected when this node's static mmds.routes policy admits the
// declaration but the process has no actual MMDS serving capability right
// now (e.g. proxy.mode=external before any proxy master registers with
// mmds_listen -- MMDSRuntimeAvailable defaults to false until then and stays
// false forever if proxy.yaml omits it). precheckCluster already applies this
// same MMDSRuntimeAvailable check for cluster create; standalone create has
// no other node to retry on, so admitting here would persist and publish
// static routes nothing can ever serve.
func TestCreateRejectsMMDSWhenRuntimeUnavailable(t *testing.T) {
	root := t.TempDir()
	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(root, "run")
	cfg.Paths.BaseRoot = filepath.Join(root, "base")
	cfg.MMDS.Enabled = true
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	o := testOrchCfg(t, cfg)
	o.vs = stubVS{}
	// MMDSRuntimeAvailable deliberately left at its zero-value false: no
	// SetMMDSRuntimeAvailable(true) call, simulating a node whose external
	// proxy master has not (or will never) register with mmds_listen.
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	_, err := o.Create(context.Background(), api.CreateReq{
		APIKey: apiKey, TemplateID: testMMDSTemplateRef(), TimeoutSec: 60,
		Metadata: map[string]string{sandboxcfg.NsMMDS: testMMDSSpec},
	})
	assertMMDSBadRequestBoundary(t, err)
	if !strings.Contains(err.Error(), "MMDS is unavailable on this node") {
		t.Fatalf("Create error = %v, want the runtime-unavailable detail", err)
	}
	for _, path := range []string{cfg.Paths.RunRoot, cfg.Paths.BaseRoot} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("unavailable-runtime mmds metadata created %s: stat error = %v", path, err)
		}
	}
}

// validateMMDSRouteEntryTransport's Create-side wiring (orch.go) has no
// round-trip test here: reaching a canonical form that overflows RouteEntry
// once double-escaped requires getting within a small margin of
// maxMMDSCanonicalBytes, which after lowering that constant to 128 KiB (see
// its doc comment -- driven by migrationtoken.MaxWireSize headroom, not this
// path) tops out at 256 KiB even in the worst case, nowhere near routesync's
// 1 MiB frame. That leaves the wiring here as defense-in-depth against
// maxMMDSCanonicalBytes and routesync.maxFrame drifting apart later, same
// rationale as the import-side wiring in migrate.go.
// validateMMDSRouteEntryTransport's own unit tests (mmds_transport_test.go)
// cover the size-detection logic directly.

// TestMMDSRouteRejectsMetadataThatNoLongerPassesPolicy proves a persisted
// specification that predates a restart/config change (e.g. mmds.routes
// tightened or disabled since the sandbox was created) is not served just
// because it once was: Reconcile re-adopts a still-running sandbox's stored
// Metadata unchanged (orch.go's Reconcile -> o.cache), and that row never
// re-runs ExtractMMDS on its own, so without admittedMMDSMetadata's re-check
// MMDSRoute would trust it forever.
func TestMMDSRouteRejectsMetadataThatNoLongerPassesPolicy(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Enabled = true
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	cfg.MMDS.Routes.MaxRoutesPerSandbox = 1 // now stricter than when this spec was admitted
	o := &Orchestrator{cfg: cfg, log: slog.New(slog.NewTextHandler(io.Discard, nil)), reg: map[string]*types.Sandbox{
		"stale": {ID: "stale", State: types.StateRunning, Metadata: map[string]string{
			sandboxcfg.NsMMDS: `{"version":1,"routes":[{"path":"/a","type":"static","data":"a"},{"path":"/b","type":"static","data":"b"}]}`,
		}},
	}}
	// Runtime available so the only thing that can make this "" is the
	// policy re-check below, not the separate MMDSRuntimeAvailable gate.
	o.SetMMDSRuntimeAvailable(true)

	if _, ok, err := o.MMDSRoute("stale", "/a"); err != nil || ok {
		t.Fatalf("MMDSRoute(stale) = ok=%v err=%v, want ok=false (the spec no longer passes the current 1-route limit)", ok, err)
	}
}

// TestRouteEntryOmitsMMDSRoutesThatNoLongerPassPolicy is
// TestMMDSRouteRejectsMetadataThatNoLongerPassesPolicy's proxy_mode=external
// counterpart: routeEntry feeds both ordinary publish and Range's full
// snapshot to a (re)connecting external proxy (which reads straight from the
// store, bypassing any in-memory adoption step entirely), so it must apply
// the same current-policy re-check as MMDSRoute rather than forwarding
// sb.Metadata[NsMMDS] unconditionally.
func TestRouteEntryOmitsMMDSRoutesThatNoLongerPassPolicy(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Enabled = true
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	cfg.MMDS.Routes.MaxRoutesPerSandbox = 1
	o := &Orchestrator{cfg: cfg, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	// Runtime available so the only thing that can make this "" is the
	// policy re-check below, not the separate MMDSRuntimeAvailable gate.
	o.SetMMDSRuntimeAvailable(true)
	sb := &types.Sandbox{ID: "stale", Profile: types.ProfileBare, State: types.StateRunning, Metadata: map[string]string{
		sandboxcfg.NsMMDS: `{"version":1,"routes":[{"path":"/a","type":"static","data":"a"},{"path":"/b","type":"static","data":"b"}]}`,
	}}

	entry := o.routeEntry(sb)
	if entry.MMDSRoutes != "" {
		t.Fatalf("routeEntry.MMDSRoutes = %q, want empty (the spec no longer passes the current 1-route limit)", entry.MMDSRoutes)
	}
}

// TestAdmittedMMDSMetadataAppliesNodeLocalPolicyToClusterSandboxes proves
// admittedMMDSMetadata (both MMDSRoute and routeEntry go through it) now
// applies this node's own mmds.routes config to a cluster-admitted sandbox
// too: cluster mode has no registry-owned policy of its own, so every node in
// a cluster deployment is expected to run the same mmds.routes configuration,
// and that configuration is what governs every row on this node regardless
// of whether it's a cluster or standalone sandbox.
func TestAdmittedMMDSMetadataAppliesNodeLocalPolicyToClusterSandboxes(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Enabled = true
	// cfg.MMDS.Routes left disabled (zero value).
	o := &Orchestrator{cfg: cfg, log: slog.New(slog.NewTextHandler(io.Discard, nil)), reg: map[string]*types.Sandbox{
		"cluster-sb": {
			ID: "cluster-sb", State: types.StateRunning,
			Cluster:  &types.ClusterSandboxContext{Group: "/g", RouteKey: "rk"},
			Metadata: map[string]string{sandboxcfg.NsMMDS: testMMDSSpec},
		},
	}}
	o.SetMMDSRuntimeAvailable(true)

	if _, ok, err := o.MMDSRoute("cluster-sb", "/x"); err != nil || ok {
		t.Fatalf("MMDSRoute(cluster-sb) = ok=%v err=%v, want ok=false (mmds.routes disabled on this node)", ok, err)
	}
	if entry := o.routeEntry(o.reg["cluster-sb"]).MMDSRoutes; entry != "" {
		t.Fatalf("routeEntry(cluster-sb).MMDSRoutes = %q, want empty (mmds.routes disabled on this node)", entry)
	}

	o.cfg.MMDS.Routes = testMMDSRoutesConfig()
	route, ok, err := o.MMDSRoute("cluster-sb", "/x")
	if err != nil || !ok || route.Data != "d" {
		t.Fatalf("MMDSRoute(cluster-sb) = %+v ok=%v err=%v, want the cluster-admitted route served once policy is enabled", route, ok, err)
	}
	if entry := o.routeEntry(o.reg["cluster-sb"]).MMDSRoutes; entry == "" {
		t.Fatal("routeEntry(cluster-sb).MMDSRoutes is empty, want the cluster-admitted spec forwarded once policy is enabled")
	}
}

// TestAdmittedMMDSMetadataFailsClosedWhenRuntimeUnavailable proves
// admittedMMDSMetadata (both MMDSRoute and routeEntry go through it) is
// gated on MMDSRuntimeAvailable() independently of schema/policy admission:
// a declaration that still passes every admission check must stop being
// served/published the moment this process loses its actual MMDS serving
// capability (e.g. proxy_mode=external's proxy master registration lease
// lapses), not just when the declaration itself becomes invalid.
func TestAdmittedMMDSMetadataFailsClosedWhenRuntimeUnavailable(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Enabled = true
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	sb := &types.Sandbox{ID: "running", State: types.StateRunning, Metadata: map[string]string{sandboxcfg.NsMMDS: testMMDSSpec}}
	o := &Orchestrator{cfg: cfg, log: slog.New(slog.NewTextHandler(io.Discard, nil)), reg: map[string]*types.Sandbox{"running": sb}}

	// Runtime available: the declaration is admitted and servable.
	o.SetMMDSRuntimeAvailable(true)
	if _, ok, err := o.MMDSRoute("running", "/x"); err != nil || !ok {
		t.Fatalf("MMDSRoute while runtime available = ok=%v err=%v, want the specified route", ok, err)
	}
	if o.routeEntry(sb).MMDSRoutes == "" {
		t.Fatal("routeEntry.MMDSRoutes empty while runtime available, want the admitted spec forwarded")
	}

	// Runtime becomes unavailable without the declaration itself changing:
	// both read paths must now fail closed instead of continuing to serve.
	o.SetMMDSRuntimeAvailable(false)
	if _, ok, err := o.MMDSRoute("running", "/x"); err != nil || ok {
		t.Fatalf("MMDSRoute while runtime unavailable = ok=%v err=%v, want ok=false (fail closed)", ok, err)
	}
	if entry := o.routeEntry(sb).MMDSRoutes; entry != "" {
		t.Fatalf("routeEntry.MMDSRoutes = %q while runtime unavailable, want empty (fail closed)", entry)
	}
}

// TestAdmittedMMDSMetadataReturnsCanonicalFormForClusterRows proves cluster
// rows serve/publish ExtractMMDS's *canonical* output, not the raw stored
// string it validated: a row reconciled from schema-valid but
// non-canonical metadata (e.g. static shorthand omitting type/content_type --
// possible if it predates a canonicalization change, or slipped in through
// some other path) must still come back with type/content_type filled in.
// LookupMMDSRoute does not reapply those defaults itself (the stored form is
// assumed already canonical), so returning the raw string would leave
// route.Type == "" and the guest GET would 503 as an unimplemented
// secret/service route instead of serving the static content.
func TestAdmittedMMDSMetadataReturnsCanonicalFormForClusterRows(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Enabled = true
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	// Static shorthand: no "type", no "content_type" -- schema-valid, not canonical.
	nonCanonical := `{"version":1,"routes":[{"path":"/x","data":"d"}]}`
	sb := &types.Sandbox{
		ID: "cluster-sb", State: types.StateRunning,
		Cluster:  &types.ClusterSandboxContext{Group: "/g", RouteKey: "rk"},
		Metadata: map[string]string{sandboxcfg.NsMMDS: nonCanonical},
	}
	o := &Orchestrator{cfg: cfg, log: slog.New(slog.NewTextHandler(io.Discard, nil)), reg: map[string]*types.Sandbox{"cluster-sb": sb}}
	o.SetMMDSRuntimeAvailable(true)

	route, ok, err := o.MMDSRoute("cluster-sb", "/x")
	if err != nil || !ok {
		t.Fatalf("MMDSRoute = ok=%v err=%v, want the specified route", ok, err)
	}
	if route.Type != sandboxcfg.MMDSRouteStatic {
		t.Fatalf("route.Type = %q, want %q (canonical defaults must be filled in)", route.Type, sandboxcfg.MMDSRouteStatic)
	}
	if route.ContentType != "application/octet-stream" {
		t.Fatalf("route.ContentType = %q, want the canonical default", route.ContentType)
	}

	entry := o.routeEntry(sb).MMDSRoutes
	if !strings.Contains(entry, `"type":"static"`) || !strings.Contains(entry, `"content_type":"application/octet-stream"`) {
		t.Fatalf("routeEntry.MMDSRoutes = %q, want the canonicalized form", entry)
	}
}

// TestMMDSRouteFailsClosedWhenPaused proves a specified MMDS route is only
// servable while the sandbox is running, matching SandboxInfo's existing
// gate on the built-in root metadata path. MmdsSecret is deliberately NOT
// gated the same way (a GET verifies a token minted moments earlier), so a
// token minted before pause remains verifiable; without this check that
// stale-but-valid token could still read a paused sandbox's specified
// routes instead of failing closed until resume.
func TestMMDSRouteFailsClosedWhenPaused(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Enabled = true
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	o := &Orchestrator{cfg: cfg, log: slog.New(slog.NewTextHandler(io.Discard, nil)), reg: map[string]*types.Sandbox{
		"running": {ID: "running", State: types.StateRunning, Metadata: map[string]string{sandboxcfg.NsMMDS: testMMDSSpec}},
		"paused":  {ID: "paused", State: types.StatePaused, Metadata: map[string]string{sandboxcfg.NsMMDS: testMMDSSpec}},
	}}
	o.SetMMDSRuntimeAvailable(true)

	route, ok, err := o.MMDSRoute("running", "/x")
	if err != nil || !ok || route.Data != "d" {
		t.Fatalf("MMDSRoute(running) = %+v ok=%v err=%v, want the specified route", route, ok, err)
	}

	route, ok, err = o.MMDSRoute("paused", "/x")
	if err != nil || ok {
		t.Fatalf("MMDSRoute(paused) = %+v ok=%v err=%v, want ok=false (fail closed) while paused", route, ok, err)
	}
}

// TestRegisterBuildRejectsMMDSWhenPolicyDisabled proves a build's own
// kuasar-sandbox.mmds declaration (for its synthetic "MMDS visibility" row,
// see runBuildUnit) is validated against this node's mmds.routes policy just
// like Create's declaration is -- with the policy at its zero value
// (Enabled=false, the default), it's rejected rather than silently stored.
func TestRegisterBuildRejectsMMDSWhenPolicyDisabled(t *testing.T) {
	o := testOrch(t) // zero-value config.Config: MMDS.Routes.Enabled defaults to false
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	_, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Name: "bare-template", Tags: []string{"bare-tag"}, Profile: types.ProfileBare,
		Metadata: map[string]string{sandboxcfg.NsMMDS: testMMDSSpec},
	})
	if !errors.Is(err, api.ErrBadRequest) || !strings.Contains(err.Error(), "MMDS routes are disabled by policy") {
		t.Fatalf("RegisterBuild error = %v, want the disabled-by-policy detail", err)
	}
}

// TestRegisterBuildAcceptsAndCanonicalizesMMDS is
// TestRegisterBuildRejectsMMDSWhenPolicyDisabled's positive counterpart: with
// mmds.routes enabled, the declaration is accepted, canonicalized, and stored
// on the Build record -- runBuildUnit copies it onto the build's own
// synthetic sandbox row so the build pipeline can fetch its own specified
// routes, but it is never inherited by sandboxes later created from the
// resulting template (MergeCreateMetadata excludes NsMMDS from template
// defaults regardless of what's stored here).
func TestRegisterBuildAcceptsAndCanonicalizesMMDS(t *testing.T) {
	cfg := &config.Config{}
	cfg.MMDS.Routes = testMMDSRoutesConfig()
	o := testOrchCfg(t, cfg)
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	// Static shorthand (no explicit "type") -- must come back canonicalized.
	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Name: "bare-template", Tags: []string{"bare-tag"}, Profile: types.ProfileBare,
		Metadata: map[string]string{sandboxcfg.NsMMDS: `{"version":1,"routes":[{"path":"/x","data":"d"}]}`},
	})
	if err != nil {
		t.Fatalf("RegisterBuild: %v", err)
	}
	got := b.Metadata[sandboxcfg.NsMMDS]
	if !strings.Contains(got, `"type":"static"`) || !strings.Contains(got, `"content_type":"application/octet-stream"`) {
		t.Fatalf("stored build metadata = %q, want canonicalized MMDS", got)
	}

	// TriggerBuild's own redeclaration fully replaces (not merges with) register's.
	alt := `{"version":1,"routes":[{"path":"/redeclared","type":"static","data":"fresh"}]}`
	if err := o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
		FromImage: "reg.example.com/app:tag",
		Metadata:  map[string]string{sandboxcfg.NsMMDS: alt},
	}, api.BuildAuth{}); err != nil {
		t.Fatalf("TriggerBuild: %v", err)
	}
	b2, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	got = b2.Metadata[sandboxcfg.NsMMDS]
	if !strings.Contains(got, "/redeclared") || strings.Contains(got, "/x") {
		t.Fatalf("trigger-time metadata = %q, want only the trigger's own spec, not register's", got)
	}
}

// TestClusterBuildRegisterRejectsMMDSWhenPolicyDisabled is
// TestRegisterBuildRejectsMMDSWhenPolicyDisabled's cluster counterpart
// (CmdBuildRegister -> registerClusterBuild): the same node-local mmds.routes
// policy applies, cluster or not.
func TestClusterBuildRegisterRejectsMMDSWhenPolicyDisabled(t *testing.T) {
	o := testOrch(t)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	ack := o.HandleCommand(context.Background(), &routesync.Command{
		Kind:                 routesync.CmdBuildRegister,
		CmdID:                "build-register-invalid-mmds",
		BuildID:              "bld-1",
		TemplateRef:          testMMDSTemplateRef(),
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Config:               map[string]string{sandboxcfg.NsMMDS: testMMDSSpec},
	})
	if ack.Status != routesync.AckRejected || ack.HTTPStatus != http.StatusBadRequest ||
		!strings.Contains(ack.Reason, "MMDS routes are disabled by policy") {
		t.Fatalf("ack = %+v, want HTTP 400 with the disabled-by-policy detail", ack)
	}
}
