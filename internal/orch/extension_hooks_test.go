package orch

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/conductorext"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type sandboxHookFunc func(context.Context, *conductorextension.SandboxOperation) error

func (f sandboxHookFunc) PrepareSandbox(ctx context.Context, operation *conductorextension.SandboxOperation) error {
	return f(ctx, operation)
}

type buildHookFunc func(context.Context, *conductorextension.BuildOperation) error

func (f buildHookFunc) PrepareBuild(ctx context.Context, operation *conductorextension.BuildOperation) error {
	return f(ctx, operation)
}

func TestPauseHookRunsOutsideLifecycleLockCanReadHostAndRejectsBeforeSnapshot(t *testing.T) {
	cfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
	o, sandbox, apiKey, launcher, vs, argsPath := newCheckpointPauseFixture(t, cfg, "")
	host, _ := conductorext.New(o.st)
	o.SetExtensionHooks(sandboxHookFunc(func(ctx context.Context, operation *conductorextension.SandboxOperation) error {
		if operation.Kind != conductorextension.SandboxOperationPause || operation.Current == nil {
			t.Fatalf("pause operation = %+v", operation)
		}
		unlock := o.lifecycle.Lock(operation.SandboxID)
		defer unlock()
		view, found, err := host.Sandboxes().Get(ctx, operation.SandboxID)
		if err != nil || !found || view.ID != sandbox.ID {
			t.Fatalf("Host.Get = %+v, %t, %v", view, found, err)
		}
		return fmtRejected("private pause policy")
	}), nil)

	done := make(chan error, 1)
	go func() { done <- o.Pause(context.Background(), sandbox.ID, apiKey, sandboxcfg.CheckpointPolicy{}) }()
	select {
	case err := <-done:
		if !errors.Is(err, conductorextension.ErrRejected) {
			t.Fatalf("Pause error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pause Hook ran under the lifecycle lock")
	}
	if _, err := os.Stat(argsPath); !os.IsNotExist(err) {
		t.Fatalf("rejected Pause invoked snapshot: %v", err)
	}
	if launcher.stops.Load() != 0 || vs.detaches.Load() != 0 {
		t.Fatalf("rejected Pause stop/detach = %d/%d", launcher.stops.Load(), vs.detaches.Load())
	}
}

func TestCreateHookRejectsBeforeDurableOrHostSideEffects(t *testing.T) {
	cfg := &config.Config{}
	launcher := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, launcher)
	vs := &checkpointVS{}
	o.vs = vs
	request := createRequestFixture(t, o, "8")
	var sandboxID string
	o.SetExtensionHooks(sandboxHookFunc(func(_ context.Context, operation *conductorextension.SandboxOperation) error {
		sandboxID = operation.SandboxID
		return fmtRejected("private create policy")
	}), nil)
	events, unsubscribe := o.Subscribe()
	defer unsubscribe()

	created, err := o.Create(ctx, request)
	if !errors.Is(err, conductorextension.ErrRejected) || created != nil || sandboxID == "" {
		t.Fatalf("Create = %+v, %v, sid=%q", created, err, sandboxID)
	}
	stored, getErr := o.st.Get(ctx, sandboxID)
	if getErr != nil || stored != nil {
		t.Fatalf("rejected Create stored = %+v, %v", stored, getErr)
	}
	if _, found := o.launches.Lookup(sandboxID); found {
		t.Fatal("rejected Create claimed a launch")
	}
	if launcher.starts.Load() != 0 || vs.attaches.Load() != 0 {
		t.Fatalf("rejected Create start/attach = %d/%d", launcher.starts.Load(), vs.attaches.Load())
	}
	for _, path := range []string{filepath.Join(cfg.Paths.RunRoot, sandboxID), filepath.Join(cfg.Paths.BaseRoot, sandboxID)} {
		if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
			t.Fatalf("rejected Create materialized %s: %v", path, statErr)
		}
	}
	select {
	case event := <-events:
		t.Fatalf("rejected Create published route event %+v", event)
	default:
	}
}

func TestCreateHookMutationIsRenormalizedAndPersisted(t *testing.T) {
	cfg := &config.Config{}
	launcher := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, launcher)
	blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
	o.vs = blocked
	request := createRequestFixture(t, o, "9")
	o.SetExtensionHooks(sandboxHookFunc(func(_ context.Context, operation *conductorextension.SandboxOperation) error {
		operation.Create.TimeoutSeconds = 120
		operation.Create.Metadata["extension"] = "normalized"
		operation.Create.Env["EXTENSION_ENV"] = "set"
		return nil
	}), nil)

	created, err := o.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := o.st.Get(ctx, created.ID)
	if err != nil || stored == nil {
		t.Fatalf("Get = %+v, %v", stored, err)
	}
	if stored.Metadata["extension"] != "normalized" || stored.Env["EXTENSION_ENV"] != "set" || stored.DeadlineUnix-stored.CreatedUnix != 120 {
		t.Fatalf("Hook mutation was not normalized into durable state: %+v", stored)
	}
	select {
	case <-blocked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("accepted Create did not enter resource preparation")
	}
	close(blocked.gate)
	waitForSandbox(t, o, ctx, created.ID, func(current *types.Sandbox) bool { return current.State == types.StateRunning }, "running after Hook-mutated Create")
}

func TestCreateHookInvalidFinalCandidateIsRejectedBeforeSideEffects(t *testing.T) {
	cfg := &config.Config{}
	launcher := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, launcher)
	vs := &checkpointVS{}
	o.vs = vs
	request := createRequestFixture(t, o, "b")
	var sandboxID string
	o.SetExtensionHooks(sandboxHookFunc(func(_ context.Context, operation *conductorextension.SandboxOperation) error {
		sandboxID = operation.SandboxID
		operation.Create.Metadata[sandboxcfg.NsResource] = `{"startup":{"cpu":"invalid"}}`
		return nil
	}), nil)
	created, err := o.Create(ctx, request)
	if !errors.Is(err, api.ErrBadRequest) || created != nil {
		t.Fatalf("Create = %+v, %v, want final validation rejection", created, err)
	}
	if stored, getErr := o.st.Get(ctx, sandboxID); getErr != nil || stored != nil {
		t.Fatalf("invalid Hook candidate stored = %+v, %v", stored, getErr)
	}
	if launcher.starts.Load() != 0 || vs.attaches.Load() != 0 {
		t.Fatalf("invalid Hook candidate start/attach = %d/%d", launcher.starts.Load(), vs.attaches.Load())
	}
}

func TestResumeHookRejectsBeforeClaimAndIdempotentStatesSkipHook(t *testing.T) {
	fixture := newBlockedResumeFixture(t)
	var calls atomic.Int32
	fixture.o.SetExtensionHooks(sandboxHookFunc(func(_ context.Context, operation *conductorextension.SandboxOperation) error {
		calls.Add(1)
		if operation.Kind != conductorextension.SandboxOperationResume || operation.Origin != conductorextension.SandboxOriginDirect {
			t.Fatalf("resume operation = %+v", operation)
		}
		return fmtRejected("private resume policy")
	}), nil)
	if _, err := fixture.o.Connect(fixture.ctx, fixture.sb.ID, fixture.apiKey, "", 0); !errors.Is(err, conductorextension.ErrRejected) {
		t.Fatalf("Connect error = %v", err)
	}
	if _, found := fixture.o.launches.Lookup(fixture.sb.ID); found {
		t.Fatal("rejected Resume claimed a launch")
	}
	stored, err := fixture.o.st.Get(fixture.ctx, fixture.sb.ID)
	if err != nil || stored.State != types.StatePaused {
		t.Fatalf("rejected Resume state = %+v, %v", stored, err)
	}

	stored.State = types.StateRunning
	stored.RunID = "existing-run"
	if err := fixture.o.st.Put(fixture.ctx, stored); err != nil {
		t.Fatal(err)
	}
	fixture.o.cache(stored)
	if _, err := fixture.o.Connect(fixture.ctx, fixture.sb.ID, fixture.apiKey, "", 0); err != nil {
		t.Fatalf("idempotent running Connect = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("Resume Hook calls = %d, want 1", calls.Load())
	}
}

func TestResumeHookResultIsRejectedAfterIncarnationChanges(t *testing.T) {
	fixture := newBlockedResumeFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	fixture.o.SetExtensionHooks(sandboxHookFunc(func(_ context.Context, _ *conductorextension.SandboxOperation) error {
		close(entered)
		<-release
		return nil
	}), nil)
	done := make(chan error, 1)
	go func() {
		_, _, err := fixture.o.ensureResumeAccepted(fixture.ctx, fixture.sb.ID, nil, nil)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Resume Hook did not start")
	}
	unlock := fixture.o.lifecycle.Lock(fixture.sb.ID)
	if err := fixture.o.st.SetDeadline(fixture.ctx, fixture.sb.ID, 999); err != nil {
		unlock()
		t.Fatal(err)
	}
	unlock()
	close(release)
	if err := <-done; !errors.Is(err, api.ErrSandboxChanged) {
		t.Fatalf("Resume result = %v, want ErrSandboxChanged", err)
	}
	if _, found := fixture.o.launches.Lookup(fixture.sb.ID); found {
		t.Fatal("stale Resume Hook result claimed a launch")
	}
}

func TestExplicitDeleteHookCanRejectAndFailedCreateRollbackBypassesIt(t *testing.T) {
	fixture := newBlockedResumeFixture(t)
	var kinds []conductorextension.SandboxOperationKind
	fixture.o.SetExtensionHooks(sandboxHookFunc(func(_ context.Context, operation *conductorextension.SandboxOperation) error {
		kinds = append(kinds, operation.Kind)
		if operation.Kind == conductorextension.SandboxOperationDelete {
			return fmtRejected("private delete policy")
		}
		return nil
	}), nil)
	if killed, err := fixture.o.Kill(fixture.ctx, fixture.sb.ID, fixture.apiKey); killed || !errors.Is(err, conductorextension.ErrRejected) {
		t.Fatalf("Kill = %t, %v", killed, err)
	}
	if stored, err := fixture.o.st.Get(fixture.ctx, fixture.sb.ID); err != nil || stored == nil {
		t.Fatalf("rejected Delete removed row = %+v, %v", stored, err)
	}

	cfg := &config.Config{}
	launcher := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, launcher)
	o.vs = failingCreateVS{err: errors.New("attach failed")}
	var rollbackKinds []conductorextension.SandboxOperationKind
	o.SetExtensionHooks(sandboxHookFunc(func(_ context.Context, operation *conductorextension.SandboxOperation) error {
		rollbackKinds = append(rollbackKinds, operation.Kind)
		if operation.Kind == conductorextension.SandboxOperationDelete {
			return conductorextension.ErrRejected
		}
		return nil
	}), nil)
	created, err := o.Create(ctx, createRequestFixture(t, o, "a"))
	if err != nil {
		t.Fatal(err)
	}
	waitForSandbox(t, o, ctx, created.ID, func(current *types.Sandbox) bool { return current.State == types.StateDead }, "failed Create rollback")
	if !reflect.DeepEqual(rollbackKinds, []conductorextension.SandboxOperationKind{conductorextension.SandboxOperationCreate}) {
		t.Fatalf("mandatory rollback Hook kinds = %v", rollbackKinds)
	}
}

func TestReaperPausePathBypassesRejectingHook(t *testing.T) {
	cfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
	o, sandbox, _, _, _, _ := newCheckpointPauseFixture(t, cfg, "")
	var calls atomic.Int32
	o.SetExtensionHooks(sandboxHookFunc(func(context.Context, *conductorextension.SandboxOperation) error {
		calls.Add(1)
		return conductorextension.ErrRejected
	}), nil)
	if err := o.pauseSandbox(context.Background(), sandbox); err != nil {
		t.Fatalf("mandatory reaper pause = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("mandatory reaper pause called Hook %d times", calls.Load())
	}
	stored, err := o.st.Get(context.Background(), sandbox.ID)
	if err != nil || stored.State != types.StatePaused {
		t.Fatalf("mandatory reaper pause state = %+v, %v", stored, err)
	}
}

func TestBuildRegisterHookMutatesIndependentCandidateBeforeCapacity(t *testing.T) {
	o := testOrch(t)
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	var retained *conductorextension.BuildRegisterRequest
	o.SetExtensionHooks(nil, buildHookFunc(func(_ context.Context, operation *conductorextension.BuildOperation) error {
		if operation.Kind != conductorextension.BuildOperationRegister || operation.Origin != conductorextension.BuildOriginDirect || operation.Current != nil {
			t.Fatalf("register operation = %+v", operation)
		}
		retained = operation.Register
		operation.Register.Profile = conductorextension.ProfileBare
		operation.Register.Kind = conductorextension.BuildKindSnapshot
		operation.Register.Names = []string{"extension-name"}
		operation.Register.Aliases = []string{"extension-alias"}
		operation.Register.Resources = conductorextension.BuildResources{CPU: 1000, Memory: 1 << 30}
		operation.Register.Metadata["extension"] = "value"
		return nil
	}))
	build, err := o.RegisterBuild(context.Background(), apiKey, api.RegisterSpec{
		Name: "original", Tags: []string{"original-alias"}, Profile: types.ProfileE2B, Resources: testBuildResources(),
		Metadata: map[string]string{"caller": "value"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if build.Profile != types.ProfileBare || build.Kind != types.KindSnp ||
		!reflect.DeepEqual(build.Names, []string{"extension-name"}) ||
		!reflect.DeepEqual(build.Aliases, []string{"extension-alias"}) ||
		build.Resources.CPU != 1000 || build.Metadata["extension"] != "value" {
		t.Fatalf("registered Hook mutation = %+v", build)
	}
	retained.Metadata["extension"] = "late mutation"
	retained.Names[0] = "late mutation"
	stored, err := o.st.GetBuild(context.Background(), build.BuildID)
	if err != nil || stored.Metadata["extension"] != "value" || stored.Names[0] != "extension-name" {
		t.Fatalf("Build candidate aliased durable state = %+v, %v", stored, err)
	}

	capacity := testOrch(t)
	capacityKey, _, _ := allowlistedBuildIdentity(t, capacity)
	registerTriggerTestBuild(t, capacity, capacityKey)
	registerTriggerTestBuild(t, capacity, capacityKey)
	var called atomic.Int32
	capacity.SetExtensionHooks(nil, buildHookFunc(func(context.Context, *conductorextension.BuildOperation) error {
		called.Add(1)
		return fmtRejected("registration policy")
	}))
	_, err = capacity.RegisterBuild(context.Background(), capacityKey, api.RegisterSpec{Profile: types.ProfileE2B, Resources: testBuildResources()})
	if !errors.Is(err, conductorextension.ErrRejected) || called.Load() != 1 {
		t.Fatalf("full-capacity RegisterBuild = %v, Hook calls=%d", err, called.Load())
	}
}

func TestBuildRegisterHookReceivesRoutesOnlyAndCoreRetainsMMDSSecrets(t *testing.T) {
	o := testOrchCfg(t, mmdsFeatureConfig())
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	header := `{"secrets":{"key":"private-build-value"}}`
	routes := `{"routes":[{"path":"/secret","type":"secret","secret":"key"}]}`
	o.SetExtensionHooks(nil, buildHookFunc(func(_ context.Context, operation *conductorextension.BuildOperation) error {
		raw := operation.Register.Metadata[sandboxcfg.NsMMDS]
		if raw != routes || strings.Contains(raw, "private-build-value") || strings.Contains(raw, `"secrets"`) {
			t.Fatalf("Build Hook MMDS metadata = %q, want routes-only projection", raw)
		}
		operation.Register.Metadata["extension"] = "accepted"
		return nil
	}))
	build, err := o.RegisterBuild(context.Background(), apiKey, api.RegisterSpec{
		Profile: types.ProfileE2B, Resources: testBuildResources(),
		Metadata: map[string]string{sandboxcfg.NsMMDS: routes}, MMDSHeader: &header,
	})
	if err != nil {
		t.Fatal(err)
	}
	if build.Metadata["extension"] != "accepted" || build.Metadata[sandboxcfg.NsMMDS] != routes {
		t.Fatalf("registered Build metadata = %+v", build.Metadata)
	}
	values, _, found, err := o.st.GetMMDSRouteSecretValues(
		context.Background(), store.MMDSRouteSecretOwnerBuild, build.BuildID,
		sandboxcfg.MMDSRoutesDigest(routes),
	)
	if err != nil || !found || string(values["key"]) != "private-build-value" {
		t.Fatalf("retained MMDS values = %+v, found=%t, err=%v", values, found, err)
	}
}

func TestClusterBuildRegisterHookRunsOnceAndExactReplayUsesOriginalIdentity(t *testing.T) {
	o := testOrch(t)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	command := clusterBuildRegisterCommand("extension-cluster-replay", fingerprint)
	var calls atomic.Int32
	o.SetExtensionHooks(nil, buildHookFunc(func(_ context.Context, operation *conductorextension.BuildOperation) error {
		if calls.Add(1) != 1 {
			return fmtRejected("Hook must not run for replay")
		}
		if operation.Origin != conductorextension.BuildOriginCluster {
			t.Fatalf("cluster register origin = %q", operation.Origin)
		}
		if operation.Register.Metadata == nil {
			operation.Register.Metadata = make(map[string]string)
		}
		operation.Register.Metadata["extension"] = "cluster"
		operation.Register.Names = []string{"cluster-extension-name"}
		return nil
	}))
	if ack := o.HandleCommand(context.Background(), command); ack.Status != routesync.AckAccepted {
		t.Fatalf("initial ACK = %+v", ack)
	}
	replay := *command
	replay.CmdID = "replay-extension-cluster-replay"
	if ack := o.HandleCommand(context.Background(), &replay); ack.Status != routesync.AckAccepted {
		t.Fatalf("exact replay ACK = %+v", ack)
	}
	if calls.Load() != 1 {
		t.Fatalf("cluster Register Hook calls = %d", calls.Load())
	}
	stored, err := o.st.GetBuild(context.Background(), command.BuildID)
	if err != nil || stored.RegistrationRequestDigest == "" || stored.Metadata["extension"] != "cluster" || stored.Names[0] != "cluster-extension-name" {
		t.Fatalf("cluster Hook build = %+v, %v", stored, err)
	}
	conflict := replay
	resources := *replay.BuildResources
	resources.CPU++
	conflict.BuildResources = &resources
	conflict.CmdID = "conflict-extension-cluster-replay"
	if ack := o.HandleCommand(context.Background(), &conflict); ack.Status != routesync.AckRejected || ack.HTTPStatus != 409 {
		t.Fatalf("changed replay ACK = %+v", ack)
	}
	if calls.Load() != 1 {
		t.Fatalf("conflicting replay reran Hook: %d", calls.Load())
	}
}

func TestClusterSandboxHookOriginAndFixedRejectionACK(t *testing.T) {
	o := testOrch(t)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	var origins []conductorextension.SandboxOperationOrigin
	o.SetExtensionHooks(sandboxHookFunc(func(_ context.Context, operation *conductorextension.SandboxOperation) error {
		origins = append(origins, operation.Origin)
		return fmtRejected("private cluster policy")
	}), nil)
	command := clusterCreateCommand(fingerprint, "extension-cluster-create")
	ack := o.HandleCommand(context.Background(), command)
	if ack.Status != routesync.AckRejected || ack.HTTPStatus != 403 || ack.Reason != conductorextension.ErrRejected.Error() {
		t.Fatalf("cluster Create rejection ACK = %+v", ack)
	}
	if !reflect.DeepEqual(origins, []conductorextension.SandboxOperationOrigin{conductorextension.SandboxOriginCluster}) {
		t.Fatalf("cluster Sandbox Hook origins = %v", origins)
	}
	if stored, err := o.st.Get(context.Background(), command.SID); err != nil || stored != nil {
		t.Fatalf("rejected cluster Create persisted = %+v, %v", stored, err)
	}
}

func TestClusterDeleteHookCanRejectBeforeCleanup(t *testing.T) {
	o := testOrch(t)
	_, manifestKey, fingerprint := allowlistedBuildIdentity(t, o)
	sandbox := &types.Sandbox{
		ID: "cluster-hook-delete", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("d", 64)}.String(),
		State:      types.StateRunning, RunID: "cluster-hook-run",
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		Cluster: &types.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
	}
	materializeTestSandboxCredentials(t, sandbox)
	if err := o.st.Put(context.Background(), sandbox); err != nil {
		t.Fatal(err)
	}
	o.cache(sandbox)
	o.SetExtensionHooks(sandboxHookFunc(func(_ context.Context, operation *conductorextension.SandboxOperation) error {
		if operation.Kind != conductorextension.SandboxOperationDelete || operation.Origin != conductorextension.SandboxOriginCluster || operation.Current == nil {
			t.Fatalf("cluster Delete operation = %+v", operation)
		}
		return fmtRejected("private cluster delete policy")
	}), nil)
	ack := o.HandleCommand(context.Background(), &routesync.Command{
		CmdID: "cluster-hook-delete", Kind: routesync.CmdDelete,
		SID: sandbox.ID, APISecretFingerprint: fingerprint,
	})
	if ack.Status != routesync.AckRejected || ack.HTTPStatus != 403 || ack.Reason != conductorextension.ErrRejected.Error() {
		t.Fatalf("cluster Delete rejection ACK = %+v", ack)
	}
	if stored, err := o.st.Get(context.Background(), sandbox.ID); err != nil || stored == nil {
		t.Fatalf("rejected cluster Delete removed row = %+v, %v", stored, err)
	}
}

func TestBuildTriggerHookMutatesFinalWorkOrderAndRevalidatesConcurrentState(t *testing.T) {
	o := testOrch(t)
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	build := registerTriggerTestBuild(t, o, apiKey)
	o.SetExtensionHooks(nil, buildHookFunc(func(_ context.Context, operation *conductorextension.BuildOperation) error {
		if operation.Kind != conductorextension.BuildOperationTrigger || operation.Current == nil {
			t.Fatalf("trigger operation = %+v", operation)
		}
		operation.Trigger.FromImage = "registry.test/extension-final:latest"
		operation.Trigger.Steps = []conductorextension.BuildStep{{Type: "RUN", Args: []string{"echo", "extension"}}}
		operation.Trigger.StartCommand = "serve-extension"
		return nil
	}))
	if err := o.TriggerBuild(context.Background(), apiKey, build.TemplateID, build.BuildID,
		api.TriggerSpec{FromImage: "registry.test/original:latest"}, api.BuildAuth{}); err != nil {
		t.Fatal(err)
	}
	stored, err := o.st.GetBuild(context.Background(), build.BuildID)
	if err != nil || stored.FromImage != "registry.test/extension-final:latest" || stored.StartCmd != "serve-extension" ||
		len(stored.Steps) != 1 || !reflect.DeepEqual(stored.Steps[0].Args, []string{"echo", "extension"}) {
		t.Fatalf("trigger Hook work order = %+v, %v", stored, err)
	}

	concurrent := testOrch(t)
	concurrentKey, _, _ := allowlistedBuildIdentity(t, concurrent)
	registered := registerTriggerTestBuild(t, concurrent, concurrentKey)
	entered := make(chan struct{})
	release := make(chan struct{})
	concurrent.SetExtensionHooks(nil, buildHookFunc(func(context.Context, *conductorextension.BuildOperation) error {
		close(entered)
		<-release
		return nil
	}))
	done := make(chan error, 1)
	go func() {
		done <- concurrent.TriggerBuild(context.Background(), concurrentKey, registered.TemplateID, registered.BuildID,
			api.TriggerSpec{FromImage: "registry.test/hook-race:latest"}, api.BuildAuth{})
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Trigger Hook did not start")
	}
	winner, err := concurrent.st.GetBuild(context.Background(), registered.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	winner.Status = types.BuildWaiting
	winner.Kind = types.KindImg
	winner.FromImage = "registry.test/concurrent-winner:latest"
	winner.WaitingUnix = time.Now().Unix()
	if committed, err := concurrent.st.CommitBuildTrigger(context.Background(), winner); err != nil || !committed {
		t.Fatalf("concurrent trigger commit = %t, %v", committed, err)
	}
	close(release)
	var conflict *api.BuildStateConflictError
	if err := <-done; !errors.As(err, &conflict) || conflict.State != types.BuildWaiting {
		t.Fatalf("stale Trigger Hook result = %v", err)
	}
	final, err := concurrent.st.GetBuild(context.Background(), registered.BuildID)
	if err != nil || final.FromImage != "registry.test/concurrent-winner:latest" {
		t.Fatalf("stale Hook overwrote winner = %+v, %v", final, err)
	}
}

func TestBuildTriggerResolvesCredentialsFromHookFinalSource(t *testing.T) {
	o := testOrch(t)
	apiKey, manifestKey, _ := allowlistedBuildIdentity(t, o)
	auth := `{"auths":{"original.test":{"username":"original","password":"secret"},"final.test":{"username":"final","password":"secret"}}}`
	if _, err := o.st.AddKeyPair(context.Background(), store.KeyPair{
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
	}, "", 0, auth); err != nil {
		t.Fatal(err)
	}
	build := registerTriggerTestBuild(t, o, apiKey)
	o.SetExtensionHooks(nil, buildHookFunc(func(_ context.Context, operation *conductorextension.BuildOperation) error {
		operation.Trigger.FromImage = "final.test/private/base:latest"
		return nil
	}))
	if err := o.TriggerBuild(context.Background(), apiKey, build.TemplateID, build.BuildID,
		api.TriggerSpec{FromImage: "original.test/private/base:latest"}, api.BuildAuth{}); err != nil {
		t.Fatal(err)
	}
	stored, err := o.st.GetBuild(context.Background(), build.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	var credentials regcreds.Creds
	if err := json.Unmarshal([]byte(stored.RegistryAuth), &credentials); err != nil || credentials.Username != "final" {
		t.Fatalf("final-source credentials = %+v, raw=%q, err=%v", credentials, stored.RegistryAuth, err)
	}
}

func fmtRejected(_ string) error {
	return conductorextension.ErrRejected
}
