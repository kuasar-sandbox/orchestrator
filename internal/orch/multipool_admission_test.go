package orch

import (
	"context"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestMultiPoolBuildAdmissionRemainsGlobal(t *testing.T) {
	cfg := buildNetworkTestConfig()
	cfg.Paths.RunRoot = t.TempDir()
	cfg.Paths.BaseRoot = t.TempDir()
	registered, executing := int64(2), int64(1)
	cfg.Builder.Admission.Registration = &config.BuildAdmissionLimitConfig{MaxBuilds: &registered}
	cfg.Builder.Admission.Execution = &config.BuildAdmissionLimitConfig{MaxBuilds: &executing}
	cfg.Builder.TotalTimeoutSec = 60
	cfg.Units.BuilderPools = []config.RunPoolConfig{{Unit: "one@.service"}, {Unit: "two@.service"}}
	o := testOrchCfg(t, cfg)
	lc := newRunPoolTestLauncher()
	o.lc = lc
	o.builderRunPool = newRunPools(runKindBuild, cfg.Units.BuilderPools, 5*time.Second, cfg.Paths.RunRoot, lc, &o.runs, o.log)
	ctx, cancel := context.WithCancel(context.Background())
	var done chan struct{}
	t.Cleanup(func() {
		cancel()
		if done != nil {
			<-done
		}
		drain, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := o.DrainBuilds(drain); err != nil {
			t.Error(err)
		}
	})
	if err := o.builderRunPool.Start(ctx); err != nil {
		t.Fatal(err)
	}
	key, _, _ := allowlistedBuildIdentity(t, o)
	first := registerTriggerTestBuild(t, o, key)
	second := registerTriggerTestBuild(t, o, key)
	if o.builderRunPool.next != 0 {
		t.Fatal("registration selected a pool")
	}
	if _, err := o.RegisterBuild(ctx, key, api.RegisterSpec{Name: "rejected", Profile: types.ProfileE2B, Resources: types.BuildResources{CPU: 2000, Memory: 2 << 30}}); err == nil {
		t.Fatal("registration quota multiplied by pool count")
	}
	for _, b := range []*types.Build{first, second} {
		if err := o.TriggerBuild(ctx, key, b.TemplateID, b.BuildID, api.TriggerSpec{FromImage: "example.invalid/base:v1"}, api.BuildAuth{}); err != nil {
			t.Fatal(err)
		}
	}
	if o.builderRunPool.next != 0 {
		t.Fatal("trigger selected before execution admission")
	}
	done = make(chan struct{})
	go func() { defer close(done); o.BuildPool(ctx, 10*time.Millisecond) }()
	select {
	case unit := <-lc.started:
		if unitToRunIDFromTemplate("one@.service", unit) == "" {
			t.Fatalf("first pool: %s", unit)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first execution was not admitted")
	}
	// Keep the first execution at the real Assign boundary without a runner callback.
	// Further scheduler passes must retain the second FIFO row and the global claim.
	select {
	case unit := <-lc.started:
		t.Fatalf("second pool bypassed global quota: %s", unit)
	case <-time.After(100 * time.Millisecond):
	}
	usage, err := o.st.BuildUsage(ctx)
	if err != nil || usage.RegistrationBuilds != 2 || usage.ExecutionBuilds != 1 {
		t.Fatalf("global usage %+v %v", usage, err)
	}
	a, err := o.st.GetBuild(ctx, first.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := o.st.GetBuild(ctx, second.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if a.Status != types.BuildBuilding || b.Status != types.BuildWaiting {
		t.Fatalf("FIFO order %s/%s", a.Status, b.Status)
	}
	o.builderRunPool.mu.Lock()
	next := o.builderRunPool.next
	o.builderRunPool.mu.Unlock()
	if next != 1 || o.runnerPool.next != 0 {
		t.Fatalf("rejected execution advanced cursors: builder=%d runner=%d", next, o.runnerPool.next)
	}
}
