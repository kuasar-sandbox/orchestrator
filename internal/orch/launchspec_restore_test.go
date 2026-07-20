package orch

import (
	"context"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSandboxLaunchSpecRestoreFileRefsTrust(t *testing.T) {
	cfg := &config.Config{}
	cfg.ManifestConfig = "/tmp/manifest.yaml"
	cfg.Paths.RunRoot = "/tmp/run"
	cfg.Sandbox.Restore.FileRefs = config.RestoreFileRefsTrust

	o := testOrchCfg(t, cfg)
	o.vs = stubVS{}

	sid := "sbx-trust"
	key := strings.Repeat("b", 64)
	sb := &types.Sandbox{
		ID:          sid,
		TemplateID:  "bare-snp-" + key,
		State:       types.StateRunning,
		RunDir:      "/tmp/run/" + sid,
		BaseDir:     "/tmp/base/" + sid,
		AuthKey:     strings.Repeat("c", 64),
		ManifestKey: strings.Repeat("a", 64),
	}
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
	if !hasArgPair(spec.Args, "--restore-file-refs", config.RestoreFileRefsTrust) {
		t.Fatalf("restore-file-refs trust missing from %v", spec.Args)
	}
}

func TestSandboxLaunchSpecRestoreFileRefsTrustDoesNotAffectColdBoot(t *testing.T) {
	cfg := &config.Config{}
	cfg.ManifestConfig = "/tmp/manifest.yaml"
	cfg.Paths.RunRoot = "/tmp/run"
	cfg.Sandbox.Restore.FileRefs = config.RestoreFileRefsTrust

	o := testOrchCfg(t, cfg)
	o.vs = stubVS{}

	sid := "sbx-cold"
	sb := &types.Sandbox{
		ID:          sid,
		TemplateID:  "bare-img-" + strings.Repeat("b", 64),
		State:       types.StateRunning,
		RunDir:      "/tmp/run/" + sid,
		BaseDir:     "/tmp/base/" + sid,
		AuthKey:     strings.Repeat("c", 64),
		ManifestKey: strings.Repeat("a", 64),
	}
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
	if hasArg(spec.Args, "--restore-file-refs") {
		t.Fatalf("cold boot should not carry restore-file-refs: %v", spec.Args)
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
