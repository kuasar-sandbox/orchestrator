package orch

import (
	"context"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSandboxLaunchSpecCarriesRestoreRef(t *testing.T) {
	cfg := &config.Config{}
	cfg.ManifestConfig = "/tmp/manifest.yaml"
	cfg.Paths.RunRoot = "/tmp/run"

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
		RunDir:      "/tmp/run/" + sid,
		BaseDir:     "/tmp/base/" + sid,
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
	if !hasArgPair(spec.Args, "--restore", "manifest://"+key) {
		t.Fatalf("restore arg missing from %v", spec.Args)
	}
	assertNoCgroupArgs(t, spec.Args)
}

func TestSandboxLaunchSpecColdBootHasNoRestoreArg(t *testing.T) {
	cfg := &config.Config{}
	cfg.ManifestConfig = "/tmp/manifest.yaml"
	cfg.Paths.RunRoot = "/tmp/run"

	o := testOrchCfg(t, cfg)
	o.vs = stubVS{}

	sid := "sbx-cold"
	manifestKey := strings.Repeat("a", 64)
	sb := &types.Sandbox{
		ID:          sid,
		Profile:     types.ProfileBare,
		TemplateID:  types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(),
		State:       types.StateRunning,
		RunDir:      "/tmp/run/" + sid,
		BaseDir:     "/tmp/base/" + sid,
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
	assertNoCgroupArgs(t, spec.Args)
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
