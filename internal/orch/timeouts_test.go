package orch

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestNodeStartupBudgetReachesSandboxAndBuildWorkOrders(t *testing.T) {
	cfg := buildNetworkTestConfig()
	cfg.Sandbox.Timeouts.AppStart = "3s"
	o := testOrchCfg(t, cfg)
	o.vs = &capturingNetworkVS{}
	p := o.sandboxParams(&types.Sandbox{}, types.TemplateID{}, sandboxcfg.SandboxSpec{}, sandboxcfg.NetworkSpec{}, rtconfig.ResourcesConfig{})
	if p.Timeouts.AppStart != "3s" {
		t.Fatal("sandbox timeout", p.Timeouts)
	}
	spec, err := o.buildSpecForPending(context.Background(), &pendingBuild{build: &types.Build{BuildID: "timeout-test", Profile: types.ProfileBare}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var round configsock.BuildSpec
	if err := json.Unmarshal(body, &round); err != nil || round.RuntimeTimeouts.AppStart != "3s" {
		t.Fatal("builder work order timeout", err, round.RuntimeTimeouts)
	}
}
