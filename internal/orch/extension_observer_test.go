package orch

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	_ "modernc.org/sqlite"
)

func TestBuildObserverFollowsSuccessfulRegistrationAndTriggerOnly(t *testing.T) {
	o := testOrch(t)
	recorder := &objectObserverRecorder{}
	o.SetExtensionObserver(recorder)
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	build := registerTriggerTestBuild(t, o, apiKey)
	if got := recorder.buildStates(); len(got) != 1 || got[0] != types.BuildRegistered {
		t.Fatalf("registration events=%v", got)
	}
	recorder.reset()
	if err := o.TriggerBuild(context.Background(), apiKey, build.TemplateID, build.BuildID, api.TriggerSpec{
		FromImage: "registry.example/build:latest",
		Steps:     []types.TemplateStep{{Type: "RUN", Args: []string{"true"}}},
	}, api.BuildAuth{}); err != nil {
		t.Fatal(err)
	}
	if got := recorder.buildStates(); len(got) != 1 || got[0] != types.BuildWaiting {
		t.Fatalf("trigger events=%v", got)
	}

	second := registerTriggerTestBuild(t, o, apiKey)
	recorder.reset()
	want := errors.New("injected trigger commit failure")
	o.commitBuildTrigger = func(context.Context, *types.Build) (bool, error) { return false, want }
	err := o.TriggerBuild(context.Background(), apiKey, second.TemplateID, second.BuildID, api.TriggerSpec{
		FromImage: "registry.example/build:latest",
	}, api.BuildAuth{})
	if !errors.Is(err, want) {
		t.Fatalf("TriggerBuild error=%v", err)
	}
	if got := recorder.buildStates(); len(got) != 0 {
		t.Fatalf("failed trigger published events=%v", got)
	}
}

func TestTerminalObserverRunsAfterCorePublicationAndNotAfterFailedPersistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "terminal-observer.db")
	o := testOrchCfgAt(t, &config.Config{}, dbPath)
	recorder := &objectObserverRecorder{}
	o.SetExtensionObserver(recorder)
	build := observerBuildingFixture(t, "terminal-observer")
	build.EnforcementStatus = "cpu,memory"
	if err := o.st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	o.completeBuildWithPublisher(context.Background(), build, nil, errors.New("runtime failed"),
		func(_ string, state string, _ string, _ string) { recorder.recordOrder("core:" + state) })
	if got := recorder.orderSnapshot(); len(got) != 2 || got[0] != "core:error" || got[1] != "extension:error" {
		t.Fatalf("terminal publication order=%v", got)
	}
	storedTerminal, err := o.st.GetBuild(context.Background(), build.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	recorder.mu.Lock()
	observedTerminal := cloneBuildForObservation(recorder.builds[0])
	recorder.mu.Unlock()
	if observedTerminal.EnforcementStatus != storedTerminal.EnforcementStatus ||
		observedTerminal.ExecutionClaimed != storedTerminal.ExecutionClaimed ||
		observedTerminal.Phase != storedTerminal.Phase ||
		observedTerminal.RuntimeVswitchPort != storedTerminal.RuntimeVswitchPort {
		t.Fatalf("terminal observer diverged from durable row: observed=%+v stored=%+v", observedTerminal, storedTerminal)
	}

	failed := observerBuildingFixture(t, "terminal-failed-persistence")
	if err := o.st.PutBuild(context.Background(), failed); err != nil {
		t.Fatal(err)
	}
	rawDB, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rawDB.Close()
	if _, err := rawDB.Exec(`CREATE TRIGGER fail_extension_terminal BEFORE UPDATE OF status ON builds
		WHEN OLD.build_id='terminal-failed-persistence'
		BEGIN SELECT RAISE(ABORT, 'injected terminal failure'); END`); err != nil {
		t.Fatal(err)
	}
	recorder.reset()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	o.completeBuildWithPublisher(canceled, failed, nil, errors.New("runtime failed"),
		func(_ string, state string, _ string, _ string) { recorder.recordOrder("core:" + state) })
	if got := recorder.orderSnapshot(); len(got) != 0 {
		t.Fatalf("failed terminal persistence published=%v", got)
	}
	stored, err := o.st.GetBuild(context.Background(), failed.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != types.BuildBuilding || !stored.ExecutionClaimed {
		t.Fatalf("failed terminal persistence changed row=%+v", stored)
	}
}

func TestSandboxObserverPublishesCommittedDeadlineAndDelete(t *testing.T) {
	o := testOrch(t)
	o.cfg.Paths.RunRoot = filepath.Join(t.TempDir(), "run")
	o.cfg.Paths.BaseRoot = filepath.Join(t.TempDir(), "base")
	recorder := &objectObserverRecorder{}
	o.SetExtensionObserver(recorder)
	manifestKey := strings.Repeat("a", 64)
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	serviceSecret := strings.Repeat("b", 64)
	forward, err := keys.MintForwardAccessToken(serviceSecret, "observer-sandbox")
	if err != nil {
		t.Fatal(err)
	}
	sandbox := &types.Sandbox{
		ID: "observer-sandbox", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("c", 64)}.String(),
		State:      types.StatePaused, APISecret: apiSecret, ManifestKey: manifestKey,
		ResumeSource:  types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("d", 64)},
		ServiceSecret: serviceSecret, ForwardAccessToken: forward, CreatedUnix: time.Now().Unix(),
		RunDir:   nodepath.SandboxRunDir(o.cfg.Paths.RunRoot, "observer-sandbox"),
		BaseDir:  nodepath.SandboxBaseDir(o.cfg.Paths.BaseRoot, "observer-sandbox"),
		Metadata: map[string]string{"key": "value"}, Env: map[string]string{"hidden": "value"},
	}
	if err := o.st.Put(context.Background(), sandbox); err != nil {
		t.Fatal(err)
	}
	o.cache(sandbox)
	if changed, err := o.SetTimeout(context.Background(), sandbox.ID, apiKey, 60); err != nil || !changed {
		t.Fatalf("SetTimeout changed=%t err=%v", changed, err)
	}
	if got := recorder.sandboxKinds(); len(got) != 1 || got[0] != "upsert" {
		t.Fatalf("deadline events=%v", got)
	}
	recorder.reset()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if changed, err := o.SetTimeout(canceled, sandbox.ID, apiKey, 120); !errors.Is(err, context.Canceled) || changed {
		t.Fatalf("canceled SetTimeout changed=%t err=%v", changed, err)
	}
	if got := recorder.sandboxKinds(); len(got) != 0 {
		t.Fatalf("failed deadline events=%v", got)
	}
	if killed, err := o.Kill(context.Background(), sandbox.ID, apiKey); err != nil || !killed {
		t.Fatalf("Kill killed=%t err=%v", killed, err)
	}
	if err := o.DrainSandboxDeletes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := recorder.sandboxKinds(); len(got) != 1 || got[0] != "delete" {
		t.Fatalf("delete events=%v", got)
	}
}

func TestBuildEventFencesArePerObjectWithoutExtensionObserver(t *testing.T) {
	var withoutObserver Orchestrator
	unlockBuild := withoutObserver.lockBuildEvent("build")
	if unlockBuild == nil {
		t.Fatal("built-in Registry publication did not create a Build event fence")
	}
	unlockEventFence(unlockBuild)
	if unlock := withoutObserver.lockExtensionSandboxEvent("sandbox"); unlock != nil {
		t.Fatal("nil observer created a Sandbox event fence")
	}
	if len(withoutObserver.buildEventFences.locks) != 0 || withoutObserver.lifecycle.locks != nil {
		t.Fatal("released Build fence or nil-observer Sandbox fence retained keyed lock state")
	}

	o := &Orchestrator{}
	unlockFirst := o.lockBuildEvent("same-build")
	sameAcquired := make(chan struct{})
	sameStarted := make(chan struct{})
	sameDone := make(chan struct{})
	go func() {
		close(sameStarted)
		unlock := o.lockBuildEvent("same-build")
		close(sameAcquired)
		unlockEventFence(unlock)
		close(sameDone)
	}()
	waitObserverSignal(t, sameStarted)
	select {
	case <-sameAcquired:
		t.Fatal("same-Build event fence did not serialize")
	case <-time.After(25 * time.Millisecond):
	}

	differentAcquired := make(chan struct{})
	go func() {
		unlock := o.lockBuildEvent("different-build")
		close(differentAcquired)
		unlockEventFence(unlock)
	}()
	waitObserverSignal(t, differentAcquired)
	unlockEventFence(unlockFirst)
	waitObserverSignal(t, sameDone)
}

func waitObserverSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for observer signal")
	}
}

type objectObserverRecorder struct {
	mu      sync.Mutex
	builds  []*types.Build
	sandbox []string
	order   []string
}

func (r *objectObserverRecorder) SandboxUpsert(sandbox *types.Sandbox) {
	r.mu.Lock()
	r.sandbox = append(r.sandbox, "upsert")
	r.mu.Unlock()
}

func (r *objectObserverRecorder) SandboxDelete(sandbox *types.Sandbox) {
	r.mu.Lock()
	r.sandbox = append(r.sandbox, "delete")
	r.mu.Unlock()
}

func (r *objectObserverRecorder) BuildUpsert(build *types.Build) {
	r.mu.Lock()
	r.builds = append(r.builds, cloneBuildForObservation(build))
	r.order = append(r.order, "extension:"+string(build.Status))
	r.mu.Unlock()
}

func (r *objectObserverRecorder) BuildRemove(build *types.Build) {
	r.mu.Lock()
	r.builds = append(r.builds, cloneBuildForObservation(build))
	r.order = append(r.order, "extension:"+string(build.Status))
	r.mu.Unlock()
}

func (r *objectObserverRecorder) recordOrder(value string) {
	r.mu.Lock()
	r.order = append(r.order, value)
	r.mu.Unlock()
}

func (r *objectObserverRecorder) reset() {
	r.mu.Lock()
	r.builds = nil
	r.sandbox = nil
	r.order = nil
	r.mu.Unlock()
}

func (r *objectObserverRecorder) buildStates() []types.BuildState {
	r.mu.Lock()
	defer r.mu.Unlock()
	states := make([]types.BuildState, len(r.builds))
	for index, build := range r.builds {
		states[index] = build.Status
	}
	return states
}

func (r *objectObserverRecorder) sandboxKinds() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sandbox...)
}

func (r *objectObserverRecorder) orderSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

func observerBuildingFixture(t *testing.T, id string) *types.Build {
	t.Helper()
	manifestKey := strings.Repeat("d", 64)
	return &types.Build{
		BuildID: id, TemplateID: "transient-" + id,
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		Profile: types.ProfileE2B, Kind: types.KindImg, Status: types.BuildBuilding,
		Resources: types.BuildResources{CPU: 1000, Memory: 1 << 30}, ExecutionClaimed: true,
		ExecutionClaimedUnix: time.Now().Unix(), CreatedUnix: time.Now().Unix(),
	}
}
