package orch

import (
	"context"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSandboxLaunchSpecNeverCarriesTaskLocalArtifactRef(t *testing.T) {
	cfg := &config.Config{}
	cfg.ManifestConfig = "/tmp/manifest.yaml"
	cfg.Paths.RunRoot = "/tmp/run"
	cfg.Paths.BaseRoot = "/tmp/base"

	o := testOrchCfg(t, cfg)
	o.vs = stubVS{}

	sid := "sbx-trust"
	key := strings.Repeat("b", 64)
	manifestKey := strings.Repeat("a", 64)
	sb := &types.Sandbox{
		ID:          sid,
		Profile:     types.ProfileBare,
		TemplateID:  types.TemplateID{Profile: types.ProfileBare, Kind: types.KindSnp, Ref: "manifest://" + key}.String(),
		State:       types.StateRunning,
		RunDir:      nodepath.SandboxRunDir(cfg.Paths.RunRoot, sid),
		BaseDir:     nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, sid),
		APISecret:   deriveTestAPISecret(t, manifestKey),
		ManifestKey: manifestKey,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}

	spec, _, ok, err := o.LaunchSpecFor(context.Background(), "sandbox:"+sid)
	if err != nil || !ok {
		t.Fatalf("LaunchSpecFor: ok=%v err=%v", ok, err)
	}
	if hasArg(spec.Args, "--restore") || hasArg(spec.Args, "--from") {
		t.Fatalf("conductor launch spec leaked task-local Artifact selection: %v", spec.Args)
	}
	assertSandboxPathArgs(t, cfg, sb, spec)
	assertNoCgroupArgs(t, spec.Args)
}

func TestSandboxLaunchSpecColdBootHasNoRestoreArg(t *testing.T) {
	cfg := &config.Config{}
	cfg.ManifestConfig = "/tmp/manifest.yaml"
	cfg.Paths.RunRoot = "/tmp/run"
	cfg.Paths.BaseRoot = "/tmp/base"

	o := testOrchCfg(t, cfg)
	o.vs = stubVS{}

	sid := "sbx-cold"
	manifestKey := strings.Repeat("a", 64)
	sb := &types.Sandbox{
		ID:          sid,
		Profile:     types.ProfileBare,
		TemplateID:  types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(),
		State:       types.StateRunning,
		RunDir:      nodepath.SandboxRunDir(cfg.Paths.RunRoot, sid),
		BaseDir:     nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, sid),
		APISecret:   deriveTestAPISecret(t, manifestKey),
		ManifestKey: manifestKey,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}

	spec, _, ok, err := o.LaunchSpecFor(context.Background(), "sandbox:"+sid)
	if err != nil || !ok {
		t.Fatalf("LaunchSpecFor: ok=%v err=%v", ok, err)
	}
	if hasArg(spec.Args, "--restore") {
		t.Fatalf("cold boot should not carry restore args: %v", spec.Args)
	}
	assertSandboxPathArgs(t, cfg, sb, spec)
	assertNoCgroupArgs(t, spec.Args)
}

func assertSandboxPathArgs(t *testing.T, cfg *config.Config, sb *types.Sandbox, spec *configsock.LaunchSpec) {
	t.Helper()
	if spec.Workdir != sb.RunDir ||
		!hasArgPair(spec.Args, "--path-id", sb.ID) ||
		!hasArgPair(spec.Args, "--run-root", nodepath.SandboxRunRoot(cfg.Paths.RunRoot)) ||
		!hasArgPair(spec.Args, "--base-root", nodepath.SandboxBaseRoot(cfg.Paths.BaseRoot)) {
		t.Fatalf("sandbox directory launch contract = workdir %q args %v", spec.Workdir, spec.Args)
	}
}

func assertNoCgroupArgs(t *testing.T, args []string) {
	t.Helper()
	for _, arg := range args {
		if strings.HasPrefix(arg, "--cgroup-path") || strings.HasPrefix(arg, "--cgroup-adopt") {
			t.Fatalf("LaunchSpec contains node-owned cgroup argument %q: %v", arg, args)
		}
	}
}

func hasArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func hasArgPair(args []string, key, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == key && args[i+1] == value {
			return true
		}
	}
	return false
}
