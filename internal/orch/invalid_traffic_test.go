package orch

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestCorruptTrafficProjectionKeepsAllLifecycleStatesAndMMDSSecrets(t *testing.T) {
	o, sb := mmdsAdminTestOrchestrator(t)
	if err := o.PutMMDSRouteSecretValue(context.Background(), sb.ID, "declared", []byte("value")); err != nil {
		t.Fatal(err)
	}
	sb.Metadata[sandboxcfg.NsTraffic] = `{"max_inflight":{"total":null}}`
	for _, state := range []types.State{types.StateStarting, types.StateRunning, types.StatePaused} {
		sb.State = state
		entry, err := o.routeEntryContext(context.Background(), sb)
		if err == nil || !entry.TrafficPolicyInvalid || entry.State != string(state) || entry.MaxInflightPatch != nil {
			t.Fatalf("bad projection=%+v err=%v", entry, err)
		}
		if entry.StableID != sb.StableID() || entry.ServiceSecret != sb.ServiceSecret || entry.MmdsSecret == "" || entry.RunID != sb.RunID {
			t.Fatal("lost identity with invalid traffic")
		}
		if state != types.StatePaused && (entry.MMDSRoutes == "" || entry.MMDSRouteSecretValues == nil || string((*entry.MMDSRouteSecretValues)["declared"]) != "value") {
			t.Fatal("invalid traffic disabled independent MMDS projection")
		}
	}
}

func TestBuildTrafficIsRuntimeOnlyAndCanonicalCreateDoesNotReadRetainedRows(t *testing.T) {
	cfg := &config.Config{Paths: config.PathsConfig{RunRoot: filepath.Join(t.TempDir(), "run"), BaseRoot: filepath.Join(t.TempDir(), "base")}}
	cfg.MMDS.Enabled = true
	maxBuilds := int64(8)
	cfg.Builder.Admission.Execution = &config.BuildAdmissionLimitConfig{MaxBuilds: &maxBuilds}
	cfg.Builder.Admission.Registration = &config.BuildAdmissionLimitConfig{MaxBuilds: &maxBuilds}
	o, lifecycleCtx := newAsyncConnectTestOrchestrator(t, cfg, &countingLauncher{})
	blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
	o.vs = blocked
	defer func() {
		close(blocked.gate)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := o.DrainLaunches(ctx); err != nil {
			t.Error(err)
		}
	}()
	apiKey, manifestKey, _ := allowlistedBuildIdentity(t, o)
	registered, err := o.RegisterBuild(context.Background(), apiKey, api.RegisterSpec{Profile: types.ProfileE2B, Resources: testBuildResources(), Metadata: map[string]string{sandboxcfg.NsTraffic: `{"max_inflight":{"total":3,"exec":1}}`}})
	if err != nil {
		t.Fatal(err)
	}
	runtime := o.publishRecoveredBuildMMDS(registered)
	if runtime == nil || runtime.ID != buildMMDSID(registered.BuildID) {
		t.Fatal("Build runtime must use its isolated synthetic route identity")
	}
	defer o.setMMDSBuildOwner(runtime.ID, "")
	defer o.uncache(runtime.ID)
	seen := false
	if err := o.Range(context.Background(), func(entry routesync.RouteEntry) error {
		if entry.SandboxID == runtime.ID {
			seen = true
			if entry.TrafficPolicyInvalid || entry.MaxInflightPatch == nil || entry.MaxInflightPatch.Total == nil || *entry.MaxInflightPatch.Total != 3 {
				t.Fatalf("Build runtime lost traffic patch: %+v", entry)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !seen {
		t.Fatal("Build runtime missing in full sync")
	}
	for i, kind := range []types.Kind{types.KindImg, types.KindSbx, types.KindSnp} {
		canonical := types.TemplateID{Profile: types.ProfileBare, Kind: kind, Ref: "manifest://" + strings.Repeat(string(rune('a'+i)), 64)}.String()
		retained := &types.Build{BuildID: "traffic-retained-" + string(kind), TemplateID: "transient-traffic-" + string(kind), PersistID: canonical, APISecret: registered.APISecret, ManifestKey: manifestKey, Profile: types.ProfileBare, Kind: kind, Status: types.BuildReady, Names: []string{canonical}, Aliases: []string{canonical}, CreatedUnix: 1, Metadata: map[string]string{sandboxcfg.NsTraffic: `{"max_inflight":{"total":3,"exec":1}}`}}
		if err := o.st.PutBuild(context.Background(), retained); err != nil {
			t.Fatal(err)
		}
		for _, metadata := range []map[string]string{nil, {sandboxcfg.NsTraffic: `{"max_inflight":{"forward":0}}`}} {
			created, err := o.Create(lifecycleCtx, api.CreateReq{APIKey: apiKey, TemplateID: canonical, TimeoutSec: 60, Metadata: metadata})
			if err != nil {
				t.Fatal(err)
			}
			raw, present := created.Metadata[sandboxcfg.NsTraffic]
			if metadata == nil {
				if present {
					t.Fatalf("canonical %s inherited Build traffic: %s", kind, raw)
				}
			} else {
				patch, err := sandboxcfg.ParseTrafficPatch(raw)
				if err != nil || patch.MaxInflight == nil || patch.MaxInflight.Total != nil || patch.MaxInflight.Exec != nil || patch.MaxInflight.Forward == nil || *patch.MaxInflight.Forward != 0 {
					t.Fatalf("canonical %s merged retained traffic: %s (%v)", kind, raw, err)
				}
			}
		}
	}
	// Assert the retained rows were not deleted to make the absence test pass.
	if build, err := o.st.GetBuild(context.Background(), "traffic-retained-"+string(types.KindImg)); err != nil || build == nil {
		t.Fatalf("retained Build absent: %v", err)
	}
}

// Range must complete a mixed snapshot, rather than returning the projection
// error and preventing the stream from reaching its Bookmark.
func TestInvalidTrafficRangeKeepsBadAndGoodPersistedRoutes(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	expected := make(map[string]types.State)
	for _, state := range []types.State{types.StateStarting, types.StateRunning, types.StatePaused} {
		for _, invalid := range []bool{false, true} {
			sid := "good-" + string(state)
			var metadata map[string]string
			if invalid {
				sid = "bad-" + string(state)
				metadata = map[string]string{sandboxcfg.NsTraffic: `{"max_inflight":{"total":null}}`}
			}
			sb := &types.Sandbox{
				ID: sid, Profile: types.ProfileBare, State: state, Metadata: metadata,
				APISecret: strings.Repeat("1", 64), ManifestKey: strings.Repeat("2", 64),
				ResumeSource: types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://" + strings.Repeat("a", 64)},
			}
			if state == types.StateStarting {
				sb.LaunchMode = types.LaunchCold
			}
			if err := materializeSandboxCredentials(sb, sandboxcfg.Credentials{}); err != nil {
				t.Fatal(err)
			}
			if err := o.st.InsertSandbox(ctx, sb); err != nil {
				t.Fatal(err)
			}
			expected[sid] = state
		}
	}
	seen := make(map[string]bool)
	if err := o.Range(ctx, func(route routesync.RouteEntry) error {
		state, found := expected[route.SandboxID]
		if !found || seen[route.SandboxID] || route.State != string(state) || route.TrafficPolicyInvalid != strings.HasPrefix(route.SandboxID, "bad-") {
			t.Fatalf("incorrect mixed snapshot entry: %+v", route)
		}
		seen[route.SandboxID] = true
		return nil
	}); err != nil {
		t.Fatalf("one invalid policy aborted snapshot: %v", err)
	}
	if len(seen) != len(expected) {
		t.Fatalf("snapshot contains %d of %d routes", len(seen), len(expected))
	}
}
