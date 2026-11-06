package orch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/resctl"
	"gopkg.in/yaml.v3"
)

type resourcePreflightVS struct {
	attaches atomic.Int64
}

func (v *resourcePreflightVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	v.attaches.Add(1)
	return &vswitch.Port{
		Port: "resource-preflight", FloatingIP: "169.254.1.2",
		MAC: "02:00:00:00:00:81", InnerIP: "169.254.1.1",
	}, nil
}

func (*resourcePreflightVS) Detach(context.Context, string) error { return nil }
func (*resourcePreflightVS) TapFD(string) vswitch.TapFD           { return vswitch.TapFD{} }

func TestFreshRestoreSnapshotReadFailureIsAsynchronousAndRollsBackDead(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	vs := &resourcePreflightVS{}
	o.vs = vs
	lc.artifactPrepareErr = errors.New("capacity probe failed")
	req := createRequestFixture(t, o, "8")
	req.TemplateID = types.TemplateID{
		Profile: types.ProfileBare,
		Kind:    types.KindSnp,
		Ref:     "manifest://" + strings.Repeat("b", 64),
	}.String()

	created, err := o.Create(ctx, req)
	if err != nil || created == nil || created.State != types.StateStarting {
		t.Fatalf("Create acceptance = %+v, %v", created, err)
	}
	assertAsyncFreshRestoreFailure(t, o, ctx, created, vs, lc)
}

func TestFreshRestoreCapacityMismatchIsAsynchronousResourceFailure(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	vs := &resourcePreflightVS{}
	o.vs = vs
	req := createRequestFixture(t, o, "9")
	req.TemplateID = types.TemplateID{
		Profile: types.ProfileBare,
		Kind:    types.KindSnp,
		Ref:     "manifest://" + strings.Repeat("c", 64),
	}.String()
	req.Metadata[sandboxcfg.NsResource] = `{"capacity":{"memory":"4GiB"}}`

	created, err := o.Create(ctx, req)
	if err != nil || created == nil || created.State != types.StateStarting {
		t.Fatalf("Create acceptance = %+v, %v", created, err)
	}
	assertAsyncFreshRestoreFailure(t, o, ctx, created, vs, lc)
}

func TestStaticStartupRequestIsAccepted(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	vs := &resourcePreflightVS{}
	o.vs = vs
	req := createRequestFixture(t, o, "a")
	req.Metadata[sandboxcfg.NsResource] = `{"startup":{"memory":"1GiB"}}`

	if _, err := o.Create(ctx, req); err != nil {
		t.Fatalf("Create error = %v", err)
	}
}

func TestStaticExplicitStartupSurvivesPauseAndResume(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		for _, mode := range []types.ResumeMode{types.ResumeMemory, types.ResumeCold} {
			t.Run(fmt.Sprintf("disabled=%t/%s", disabled, mode), func(t *testing.T) {
				installCheckpointSandboxCtl(t)
				cfg := &config.Config{}
				cfg.Checkpoint.Mode = config.CheckpointLocal
				if disabled {
					cfg.ResourceListen = &config.ResourceListenConfig{Enabled: false}
				}
				launchModes := make(chan string, 2)
				lc := &countingLauncher{artifactLaunchModes: launchModes}
				o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
				barrier := &fakePauseBarrier{fakeRouteBarrier: newFakeRouteBarrier(nil, nil)}
				barrier.release()
				o.pauseBarriers = barrier
				req := createRequestFixture(t, o, "6")
				req.Metadata[sandboxcfg.NsResource] = `{"startup":{"memory":"1GiB"}}`
				created, err := o.Create(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				initial := waitForSandbox(t, o, ctx, created.ID, func(sb *types.Sandbox) bool { return sb.State == types.StateRunning }, "initial static image")
				if _, err := o.Pause(ctx, created.ID, req.APIKey, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}); err != nil {
					t.Fatal(err)
				}
				paused := waitForSandbox(t, o, ctx, created.ID, func(sb *types.Sandbox) bool { return sb.State == types.StatePaused && sb.RunID == "" }, "released checkpoint")
				if paused.ResumeSource.Ref != checkpointSnapshotRef || paused.ResumeSource.SandboxRef != checkpointSandboxRef {
					t.Fatalf("checkpoint source=%+v", paused.ResumeSource)
				}
				if _, _, err := o.ensureResumeAccepted(ctx, created.ID, nil, types.ResumeRequest{Trigger: types.ResumeTriggerConnect, Mode: mode}, nil); err != nil {
					t.Fatal(err)
				}
				current := waitForSandbox(t, o, ctx, created.ID, func(sb *types.Sandbox) bool { return sb.State == types.StateRunning }, "static resume")
				if current.RunID == initial.RunID {
					t.Fatal("resume reused retired runner")
				}
				wantMode := types.LaunchMemory
				if mode == types.ResumeCold {
					wantMode = types.LaunchCold
				}
				select {
				case got := <-launchModes:
					if got != string(wantMode) {
						t.Fatalf("artifact launch=%s want %s", got, wantMode)
					}
				default:
					t.Fatal("missing artifact preparation")
				}
				body, err := os.ReadFile(o.sandboxConfigPath(current))
				if err != nil {
					t.Fatal(err)
				}
				var rendered rtconfig.SandboxConfig
				if err := yaml.Unmarshal(body, &rendered); err != nil {
					t.Fatal(err)
				}
				if rendered.Resources.Control.Controller != "" || rendered.Resources.Startup == nil || rendered.Resources.Startup.Memory != "1GiB" {
					t.Fatalf("static policy changed on resume: %+v", rendered.Resources)
				}
				// Use the actual sandboxer adapter with the rendered static config.
				// A memory restore must ignore startup in favor of the saved budget.
				hooks, err := resctl.NewControllerHooks(resctl.ControllerHookOptions{Context: ctx}, &rendered)
				if err != nil {
					t.Fatal(err)
				}
				defer hooks.Release("test complete")
				var snapshot uint64
				wantBudget := uint64(1 << 30)
				if mode == types.ResumeMemory {
					snapshot, wantBudget = 1536<<20, 1536<<20
				}
				budget, err := hooks.Admit(current.ID, snapshot)
				if err != nil || budget != wantBudget {
					t.Fatalf("SDK initial budget=%d want=%d err=%v", budget, wantBudget, err)
				}
			})
		}
	}
}

func TestStartupCreateUsesSameResolverForStaticAndDynamicArtifacts(t *testing.T) {
	for _, controller := range []string{"omitted", "disabled", "dynamic"} {
		for _, kind := range []types.Kind{types.KindImg, types.KindSbx, types.KindSnp} {
			for _, patch := range []string{`{}`, `{"startup":{}}`, `{"startup":{"memory":"1GiB"}}`, `{"startup":{"memory":"bad"}}`, `{"startup":{"memory":"3GiB"}}`} {
				t.Run(fmt.Sprintf("%s/%s/%s", controller, kind, patch), func(t *testing.T) {
					cfg := &config.Config{}
					if controller != "omitted" {
						cfg.ResourceListen = &config.ResourceListenConfig{Enabled: controller == "dynamic"}
					}
					lc := &countingLauncher{}
					o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
					if controller == "dynamic" {
						o.SetResourceControllerSocketIdentity(filepath.Join(cfg.Paths.RunRoot, "resource.sock"))
					}
					vs := &resourcePreflightVS{}
					o.vs = vs
					req := createRequestFixture(t, o, "7")
					req.TemplateID = types.TemplateID{Profile: types.ProfileBare, Kind: kind, Ref: "manifest://" + strings.Repeat("7", 64)}.String()
					req.Metadata[sandboxcfg.NsResource] = patch
					created, err := o.Create(ctx, req)
					invalid, oversized := strings.Contains(patch, "bad"), strings.Contains(patch, "3GiB")
					if invalid || oversized && kind == types.KindImg {
						if !errors.Is(err, api.ErrBadRequest) {
							t.Fatalf("invalid synchronous input=%v", err)
						}
						assertNoFreshLaunchSideEffects(t, o, ctx, vs, lc)
						return
					}
					if err != nil || created == nil {
						t.Fatalf("Create acceptance=%+v %v", created, err)
					}
					if oversized {
						assertAsyncFreshRestoreFailure(t, o, ctx, created, vs, lc)
						return
					}
					current := waitForSandbox(t, o, ctx, created.ID, func(sb *types.Sandbox) bool { return sb.State == types.StateRunning }, "running with startup")
					body, err := os.ReadFile(o.sandboxConfigPath(current))
					if err != nil {
						t.Fatal(err)
					}
					var rendered rtconfig.SandboxConfig
					if err := yaml.Unmarshal(body, &rendered); err != nil {
						t.Fatal(err)
					}
					want := "2GiB"
					if strings.Contains(patch, "1GiB") {
						want = "1GiB"
					}
					if rendered.Resources.Startup == nil || rendered.Resources.Startup.Memory != want {
						t.Fatalf("startup=%+v", rendered.Resources.Startup)
					}
					if (rendered.Resources.Control.Controller != "") != (controller == "dynamic") {
						t.Fatal("static test silently enabled dynamic controller")
					}
				})
			}
		}
	}
}

func TestPausedResumeSnapshotReadFailureReturnsToPausedAsynchronously(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	vs := &resourcePreflightVS{}
	o.vs = vs
	lc.artifactPrepareErr = errors.New("resume capacity probe failed")
	sb := &types.Sandbox{
		ID: "resource-resume-probe", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{
			Profile: types.ProfileBare, Kind: types.KindImg,
			Ref: "manifest://" + strings.Repeat("d", 64),
		}.String(),
		State: types.StatePaused,
		ResumeSource: types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("e", 64),
		},
		APISecret:   deriveTestAPISecret(t, strings.Repeat("f", 64)),
		ManifestKey: strings.Repeat("f", 64),
		RunDir:      nodepath.SandboxRunDir(cfg.Paths.RunRoot, "resource-resume-probe"),
		BaseDir:     nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, "resource-resume-probe"),
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	accepted, attempt, err := o.ensureResumeAccepted(ctx, sb.ID, nil, types.ResumeRequest{
		Trigger: types.ResumeTriggerConnect,
		Mode:    types.ResumeAuto,
	}, nil)
	if err != nil || accepted == nil || accepted.State != types.StateStarting || attempt == nil {
		t.Fatalf("ensureResumeAccepted = %+v, %+v, %v", accepted, attempt, err)
	}
	stored := waitForSandbox(t, o, ctx, sb.ID, func(current *types.Sandbox) bool { return current.State == types.StatePaused }, "paused after task snapshot failure")
	if stored.RunID != "" || stored.VswitchPort != "" {
		t.Fatalf("stored sandbox retained ownership = %+v", stored)
	}
	if vs.attaches.Load() != 0 || lc.starts.Load() != 1 || lc.stops.Load() != 1 {
		t.Fatalf("resume side effects: attaches=%d starts=%d", vs.attaches.Load(), lc.starts.Load())
	}
}

func TestDynamicLaunchUsesCanonicalResourceControllerIdentity(t *testing.T) {
	realParent := filepath.Join(t.TempDir(), "canonical")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatal(err)
	}
	aliasParent := filepath.Join(filepath.Dir(realParent), "alias")
	if err := os.Symlink(realParent, aliasParent); err != nil {
		t.Fatal(err)
	}
	listen := filepath.Join(aliasParent, "resource.sock")
	rcfg := &config.ResourceListenConfig{
		Enabled: true,
		Socket:  listen,
		Resources: config.ResourceHostConfig{
			PhysicalMemory: "64GiB", PhysicalCPU: "8",
			HostReserved: config.ResourceHostReserved{Memory: "1GiB", CPU: 1},
		},
	}
	resolved, err := nodectl.Resolve(rcfg)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Listen == resolved.SocketIdentity {
		t.Fatalf("test did not create distinct listen and canonical identity: %+v", resolved)
	}
	cfg := buildNetworkTestConfig()
	cfg.ResourceListen = rcfg
	cfg.Paths.RunRoot = t.TempDir()
	cfg.Paths.BaseRoot = t.TempDir()
	cfg.Sandbox.Boot.Kernel = "/kernel"
	cfg.Sandbox.Boot.Runtime = "/runtime"
	o := testOrchCfg(t, cfg)
	o.vs = stubVS{}
	o.SetResourceControllerSocketIdentity(resolved.SocketIdentity)
	tmpl := types.TemplateID{
		Profile: types.ProfileBare, Kind: types.KindImg,
		Ref: "manifest://" + strings.Repeat("1", 64),
	}
	sb := &types.Sandbox{ID: "canonical-controller", Profile: types.ProfileBare, LaunchMode: types.LaunchImage}
	prep, err := o.prepareSandboxLaunch(context.Background(), sb, tmpl)
	if err != nil {
		t.Fatal(err)
	}
	body, err := o.sandboxParams(sb, tmpl, prep.Spec, prep.Network, prep.Resources).BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	var rendered rtconfig.SandboxConfig
	if err := yaml.Unmarshal(body, &rendered); err != nil {
		t.Fatal(err)
	}
	resources := rendered.Resources
	if resources.Control.Controller != resolved.SocketIdentity {
		t.Fatalf("controller = %q, want canonical identity %q (Listen %q)", resources.Control.Controller, resolved.SocketIdentity, resolved.Listen)
	}
	if resources.Control.CgroupPath != "" || resources.Control.CgroupFD != 0 {
		t.Fatalf("renderer leaked cgroup capability: %+v", resources.Control)
	}
	if resources.Startup == nil || resources.Startup.Memory != "2GiB" {
		t.Fatalf("dynamic default startup = %+v", resources.Startup)
	}
	if resources.WatermarkHigh == nil || resources.WatermarkHigh.Ratio != 0.875 || resources.Control.Sensor != nil {
		t.Fatalf("renderer fixed sandboxer-owned defaults: %+v", resources)
	}
	if resources.Allocatable.DeflateOnOOM == nil || !*resources.Allocatable.DeflateOnOOM {
		t.Fatalf("balloon deflate_on_oom = %v", resources.Allocatable.DeflateOnOOM)
	}
}

func assertNoFreshLaunchSideEffects(t *testing.T, o *Orchestrator, ctx context.Context, vs *resourcePreflightVS, lc *countingLauncher) {
	t.Helper()
	if vs.attaches.Load() != 0 || lc.starts.Load() != 0 {
		t.Fatalf("launch side effects: attaches=%d starts=%d", vs.attaches.Load(), lc.starts.Load())
	}
	rows, _, err := o.st.List(ctx, "", "", 100, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("preflight failure persisted sandbox rows: %+v", rows)
	}
}

func assertAsyncFreshRestoreFailure(t *testing.T, o *Orchestrator, ctx context.Context, created *types.Sandbox, vs *resourcePreflightVS, lc *countingLauncher) {
	t.Helper()
	dead := waitForSandbox(t, o, ctx, created.ID, func(current *types.Sandbox) bool { return current.State == types.StateDead }, "dead after async restore preparation failure")
	if dead.RunID != "" || dead.VswitchPort != "" || dead.FloatingIP != "" {
		t.Fatalf("dead sandbox retained ownership = %+v", dead)
	}
	if vs.attaches.Load() != 0 || lc.starts.Load() != 1 || lc.stops.Load() != 1 {
		t.Fatalf("async restore cleanup: attaches=%d starts=%d stops=%d", vs.attaches.Load(), lc.starts.Load(), lc.stops.Load())
	}
	assertTerminatedDataRetained(t, created)
}
