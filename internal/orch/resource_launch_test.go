package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
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
	lc.snapshotPrepareErr = errors.New("capacity probe failed")
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

func TestStaticStartupRestoreRequestIsRejectedSynchronously(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	vs := &resourcePreflightVS{}
	o.vs = vs
	req := createRequestFixture(t, o, "7")
	req.TemplateID = types.TemplateID{
		Profile: types.ProfileBare,
		Kind:    types.KindSnp,
		Ref:     "manifest://" + strings.Repeat("7", 64),
	}.String()
	req.Metadata[sandboxcfg.NsResource] = `{"startup":{"memory":"1GiB"}}`

	if _, err := o.Create(ctx, req); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("Create error = %v, want ErrBadRequest", err)
	}
	assertNoFreshLaunchSideEffects(t, o, ctx, vs, lc)
}

func TestPausedResumeSnapshotReadFailureReturnsToPausedAsynchronously(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	vs := &resourcePreflightVS{}
	o.vs = vs
	lc.snapshotPrepareErr = errors.New("resume capacity probe failed")
	sb := &types.Sandbox{
		ID: "resource-resume-probe", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{
			Profile: types.ProfileBare, Kind: types.KindImg,
			Ref: "manifest://" + strings.Repeat("d", 64),
		}.String(),
		State:       types.StatePaused,
		SnapshotRef: "manifest://" + strings.Repeat("e", 64),
		APISecret:   deriveTestAPISecret(t, strings.Repeat("f", 64)),
		ManifestKey: strings.Repeat("f", 64),
		RunDir:      filepath.Join(cfg.Paths.RunRoot, "resource-resume-probe"),
		BaseDir:     filepath.Join(cfg.Paths.BaseRoot, "resource-resume-probe"),
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	accepted, attempt, err := o.ensureResumeAccepted(ctx, sb.ID, nil, nil)
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
	sb := &types.Sandbox{ID: "canonical-controller", Profile: types.ProfileBare}
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
	for _, dir := range []string{created.RunDir, created.BaseDir} {
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("async restore cleanup retained %s: %v", dir, err)
		}
	}
}
