package orch

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestKernelCmdlineReachesSandboxAndBuildWorkOrders(t *testing.T) {
	empty, selected := "", `loglevel=4 example.value="two words"`
	for _, cmdline := range []*string{nil, &empty, &selected} {
		cfg := buildNetworkTestConfig()
		cfg.Sandbox.Boot.Kernel = "/node/new-vmlinux"
		cfg.Sandbox.Boot.Cmdline = cmdline
		o := testOrchCfg(t, cfg)
		o.vs = &capturingNetworkVS{}
		p := o.sandboxParams(&types.Sandbox{}, types.TemplateID{}, sandboxcfg.SandboxSpec{}, sandboxcfg.NetworkSpec{}, rtconfig.ResourcesConfig{})
		if p.Kernel != cfg.Sandbox.Boot.Kernel || !reflect.DeepEqual(p.Cmdline, cmdline) {
			t.Fatalf("sandbox lost kernel parameters: kernel=%q cmdline=%v", p.Kernel, p.Cmdline)
		}
		spec, err := o.buildSpecForPending(context.Background(), &pendingBuild{
			build: &types.Build{BuildID: "cmdline-test", Profile: types.ProfileBare},
		})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}
		var received configsock.BuildSpec
		if err := json.Unmarshal(raw, &received); err != nil {
			t.Fatal(err)
		}
		if received.Paths.Kernel != cfg.Sandbox.Boot.Kernel || !reflect.DeepEqual(received.BootCmdline, cmdline) {
			t.Fatalf("builder lost kernel parameters: kernel=%q cmdline=%v", received.Paths.Kernel, received.BootCmdline)
		}
	}
}
