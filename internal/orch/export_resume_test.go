package orch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type blockingExportPublisher struct {
	ref         string
	started     chan struct{}
	finished    chan struct{}
	release     chan struct{}
	canceled    chan error
	startOnce   sync.Once
	finishOnce  sync.Once
	releaseOnce sync.Once
}

func newBlockingExportPublisher(ref string) *blockingExportPublisher {
	return &blockingExportPublisher{
		ref:      ref,
		started:  make(chan struct{}),
		finished: make(chan struct{}),
		release:  make(chan struct{}),
		canceled: make(chan error, 1),
	}
}

func (p *blockingExportPublisher) Publish(ctx context.Context, _ *types.Sandbox, source types.ResumeSource) (types.ResumeSource, error) {
	p.startOnce.Do(func() { close(p.started) })
	defer p.finishOnce.Do(func() { close(p.finished) })
	select {
	case <-p.release:
		return types.ResumeSource{Kind: source.Kind, Ref: p.ref}, nil
	case <-ctx.Done():
		cause := context.Cause(ctx)
		select {
		case p.canceled <- cause:
		default:
		}
		return types.ResumeSource{}, cause
	}
}

func (p *blockingExportPublisher) Release() {
	p.releaseOnce.Do(func() { close(p.release) })
}

type asyncExportResult struct {
	result string
	err    error
}

type exportResumeFixture struct {
	o        *Orchestrator
	launcher *countingLauncher
	ctx      context.Context
	sb       *types.Sandbox
	apiKey   string
	localRef string
}

func newExportResumeFixture(t *testing.T) exportResumeFixture {
	t.Helper()
	dir := shortOrchestratorTestDir(t)
	runtimePath := filepath.Join(dir, "runtime.erofs")
	if err := os.WriteFile(runtimePath, []byte("runtime"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Sandbox.Boot.Runtime = runtimePath
	cfg.Sandbox.TimeoutSec = 900
	cfg.Checkpoint.Mode = config.CheckpointLocal
	cfg.Paths.RunRoot = filepath.Join(dir, "run")
	cfg.Paths.BaseRoot = filepath.Join(dir, "lib")
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)

	manifestKey := strings.Repeat("6", 64)
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	sid := "export-resume-source"
	localRef := makeLocalSnapshot(t, dir, sid)
	sb := &types.Sandbox{
		ID:           sid,
		Profile:      types.ProfileBare,
		TemplateID:   types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
		State:        types.StatePaused,
		APISecret:    apiSecret,
		ManifestKey:  manifestKey,
		ResumeSource: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: localRef},
		RunDir:       nodepath.SandboxRunDir(o.cfg.Paths.RunRoot, sid),
		BaseDir:      nodepath.SandboxBaseDir(o.cfg.Paths.BaseRoot, sid),
		CreatedUnix:  1,
		DeadlineUnix: time.Now().Add(time.Hour).Unix(),
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	return exportResumeFixture{o: o, launcher: lc, ctx: ctx, sb: sb, apiKey: apiKey, localRef: localRef}
}

func startExport(o *Orchestrator, ctx context.Context, apiKey, sid string, toTemplate, keepSource bool) <-chan asyncExportResult {
	done := make(chan asyncExportResult, 1)
	go func() {
		result, err := o.ExportSandbox(ctx, apiKey, sid, toTemplate, keepSource)
		done <- asyncExportResult{result: result, err: err}
	}()
	return done
}

func waitPublisherStarted(t *testing.T, publisher *blockingExportPublisher) {
	t.Helper()
	select {
	case <-publisher.started:
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot publisher did not start")
	}
}

func waitExportResult(t *testing.T, done <-chan asyncExportResult) asyncExportResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("ExportSandbox did not return")
		return asyncExportResult{}
	}
}

func assertLocalResumeWon(t *testing.T, fixture exportResumeFixture) {
	t.Helper()
	stored := waitForSandbox(t, fixture.o, fixture.ctx, fixture.sb.ID, func(current *types.Sandbox) bool {
		return current.State == types.StateRunning
	}, "running after export-time resume")
	if stored.ResumeSource != (types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: fixture.localRef}) {
		t.Fatalf("resumed source = %+v, want local snapshot %q", stored.ResumeSource, fixture.localRef)
	}
	if _, err := os.Stat(fixture.localRef); err != nil {
		t.Fatalf("resume-winning export removed local snapshot: %v", err)
	}
}

func TestExportResumeDuringPublish(t *testing.T) {
	for _, toTemplate := range []bool{false, true} {
		for _, keepSource := range []bool{false, true} {
			name := fmt.Sprintf("template=%t/keep=%t", toTemplate, keepSource)
			t.Run(name, func(t *testing.T) {
				fixture := newExportResumeFixture(t)
				portableRef := "manifest://" + strings.Repeat("b", 64)
				publisher := newBlockingExportPublisher(portableRef)
				t.Cleanup(publisher.Release)
				fixture.o.artifactPublisher = publisher.Publish
				done := startExport(fixture.o, fixture.ctx, fixture.apiKey, fixture.sb.ID, toTemplate, keepSource)
				waitPublisherStarted(t, publisher)

				connected, err := fixture.o.Connect(fixture.ctx, fixture.sb.ID, fixture.apiKey, "", api.ConnectOptions{})
				if err != nil || connected == nil || connected.State != types.StateStarting {
					t.Fatalf("Connect during export = %+v, %v", connected, err)
				}

				if toTemplate {
					select {
					case result := <-done:
						t.Fatalf("detached template export returned before publish completed: %+v", result)
					default:
					}
					select {
					case cause := <-publisher.canceled:
						t.Fatalf("template resume canceled publish: %v", cause)
					default:
					}
					publisher.Release()
					result := waitExportResult(t, done)
					if result.err != nil {
						t.Fatalf("detached template export: %v", result.err)
					}
					template, err := types.ParseTemplateID(result.result)
					if err != nil || template.Ref != portableRef || template.Kind != types.KindSnp {
						t.Fatalf("detached template result = %#v, %v", template, err)
					}
				} else {
					result := waitExportResult(t, done)
					if result.result != "" || !errors.Is(result.err, api.ErrExportPreempted) {
						t.Fatalf("preempted KMT export = %q, %v", result.result, result.err)
					}
					select {
					case cause := <-publisher.canceled:
						if !errors.Is(cause, api.ErrExportPreempted) {
							t.Fatalf("KMT publisher cancellation = %v", cause)
						}
					case <-time.After(3 * time.Second):
						t.Fatal("KMT publisher was not canceled")
					}
				}
				assertLocalResumeWon(t, fixture)
			})
		}
	}
}

func TestExportExternalWakePreemptsKMTPublish(t *testing.T) {
	fixture := newExportResumeFixture(t)
	publisher := newBlockingExportPublisher("manifest://" + strings.Repeat("c", 64))
	t.Cleanup(publisher.Release)
	fixture.o.artifactPublisher = publisher.Publish
	done := startExport(fixture.o, fixture.ctx, fixture.apiKey, fixture.sb.ID, false, false)
	waitPublisherStarted(t, publisher)

	fixture.o.OnWake(fixture.ctx, fixture.sb.ID)
	result := waitExportResult(t, done)
	if !errors.Is(result.err, api.ErrExportPreempted) {
		t.Fatalf("Wake-preempted KMT export error = %v", result.err)
	}
	assertLocalResumeWon(t, fixture)
}

func TestExportRejectsPublisherChangingArtifactKind(t *testing.T) {
	fixture := newExportResumeFixture(t)
	fixture.o.artifactPublisher = func(context.Context, *types.Sandbox, types.ResumeSource) (types.ResumeSource, error) {
		return types.ResumeSource{
			Kind: types.ResumeSourceSandbox,
			Ref:  "manifest://" + strings.Repeat("e", 64),
		}, nil
	}
	if _, err := fixture.o.ExportSandbox(fixture.ctx, fixture.apiKey, fixture.sb.ID, true, true); err == nil ||
		!strings.Contains(err.Error(), "publisher changed") {
		t.Fatalf("publisher kind-change error = %v", err)
	}
	stored, err := fixture.o.st.Get(fixture.ctx, fixture.sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.ResumeSource != fixture.sb.ResumeSource {
		t.Fatalf("publisher kind change mutated row = %+v, %v", stored, err)
	}
	if _, err := os.Stat(fixture.localRef); err != nil {
		t.Fatalf("publisher kind change removed local source: %v", err)
	}
}

func TestAcceptedResumePreemptsExportBeforeAsyncSnapshotFailure(t *testing.T) {
	fixture := newExportResumeFixture(t)
	portableRef := "manifest://" + strings.Repeat("d", 64)
	publisher := newBlockingExportPublisher(portableRef)
	t.Cleanup(publisher.Release)
	fixture.o.artifactPublisher = publisher.Publish
	fixture.launcher.artifactSummary = &configsock.ArtifactPrepareSummary{
		SchemaVersion:      configsock.ArtifactPrepareSchemaVersion,
		PreparedSourceKind: string(types.ResumeSourceSnapshot),
		Capacity:           configsock.ArtifactCapacity{}, // invalid, rejected asynchronously
		DiskTopology:       validArtifactDiskTopology(),
		ResolutionDigest:   strings.Repeat("0", 64),
		RequiredRefCount:   1,
	}
	done := startExport(fixture.o, fixture.ctx, fixture.apiKey, fixture.sb.ID, false, true)
	waitPublisherStarted(t, publisher)

	accepted, err := fixture.o.Connect(fixture.ctx, fixture.sb.ID, fixture.apiKey, "", api.ConnectOptions{})
	if err != nil || accepted == nil || accepted.State != types.StateStarting {
		t.Fatalf("Connect async acceptance = %+v, %v", accepted, err)
	}
	select {
	case cause := <-publisher.canceled:
		if !errors.Is(cause, api.ErrExportPreempted) {
			t.Fatalf("resume cancellation cause = %v", cause)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("accepted resume did not preempt export")
	}
	result := waitExportResult(t, done)
	if !errors.Is(result.err, api.ErrExportPreempted) || result.result != "" {
		t.Fatalf("preempted export = %q, %v", result.result, result.err)
	}
	stored := waitForSandbox(t, fixture.o, fixture.ctx, fixture.sb.ID, func(sb *types.Sandbox) bool {
		return sb.State == types.StatePaused
	}, "paused after async snapshot preparation failure")
	if stored.ResumeSource != (types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: fixture.localRef}) {
		t.Fatalf("source after failed Resume = %+v", stored)
	}
	if _, err := os.Stat(fixture.localRef); err != nil {
		t.Fatalf("preempted export removed local snapshot: %v", err)
	}
}

func TestTemplateExportDeletesUnkeptSource(t *testing.T) {
	fixture := newExportResumeFixture(t)
	if err := os.MkdirAll(fixture.sb.RunDir, 0o700); err != nil {
		t.Fatal(err)
	}
	portableRef := "manifest://" + strings.Repeat("1", 64)
	publisher := newBlockingExportPublisher(portableRef)
	publisher.Release()
	fixture.o.artifactPublisher = publisher.Publish
	events, cancelEvents := fixture.o.Subscribe()
	defer cancelEvents()

	result, err := fixture.o.ExportSandbox(fixture.ctx, fixture.apiKey, fixture.sb.ID, true, false)
	if err != nil {
		t.Fatal(err)
	}
	template, err := types.ParseTemplateID(result)
	if err != nil || template.Ref != portableRef || template.Kind != types.KindSnp {
		t.Fatalf("template result = %#v, %v", template, err)
	}
	stored, err := fixture.o.st.Get(fixture.ctx, fixture.sb.ID)
	if err != nil || stored != nil {
		t.Fatalf("unkept template source = %+v, %v", stored, err)
	}
	if cached := fixture.o.lookup(fixture.sb.ID); cached != nil {
		t.Fatalf("unkept template source remained cached: %+v", cached)
	}
	if _, err := os.Stat(filepath.Dir(fixture.localRef)); !os.IsNotExist(err) {
		t.Fatalf("unkept template local snapshot still exists: %v", err)
	}
	if _, err := os.Stat(fixture.sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unkept template source run directory still exists: %v", err)
	}
	select {
	case event := <-events:
		if event.Kind != "delete" || event.SID != fixture.sb.ID {
			t.Fatalf("unkept template route event = %+v", event)
		}
	default:
		t.Fatal("unkept template export did not publish source deletion")
	}
}

func TestTemplateExportDeletesPortableUnkeptSource(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	manifestKey := strings.Repeat("7", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	portableRef := "manifest://" + strings.Repeat("3", 64)
	sb := migrationSandbox(t, dir, "portable-template-source", manifestKey, portableRef)
	if err := os.MkdirAll(sb.RunDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)

	result, err := o.ExportSandbox(context.Background(), apiKey, sb.ID, true, false)
	if err != nil {
		t.Fatal(err)
	}
	template, err := types.ParseTemplateID(result)
	if err != nil || template.Ref != portableRef {
		t.Fatalf("portable template result = %#v, %v", template, err)
	}
	stored, err := o.st.Get(context.Background(), sb.ID)
	if err != nil || stored != nil || o.lookup(sb.ID) != nil {
		t.Fatalf("portable unkept template source = %+v, %v", stored, err)
	}
	if _, err := os.Stat(sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("portable unkept template source run directory still exists: %v", err)
	}
}

func TestExportDeleteTeardownFailurePreservesSourceForRetry(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	o.cfg.Units.Runner = "sandbox-runner@.service"
	stopErr := errors.New("injected source stop failure")
	lc := &orderedCleanupLauncher{stopErr: stopErr}
	vs := &orderedCleanupVS{}
	o.lc = lc
	o.vs = vs

	manifestKey := strings.Repeat("8", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	sb := migrationSandbox(t, dir, "teardown-retry-source", manifestKey, "manifest://"+strings.Repeat("4", 64))
	sb.RunID = "sr-00000000-0000-7000-8000-000000000008"
	sb.VswitchPort = "teardown-retry-port"
	if err := os.MkdirAll(sb.RunDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)

	result, err := o.ExportSandbox(context.Background(), apiKey, sb.ID, true, false)
	if result != "" || !errors.Is(err, stopErr) {
		t.Fatalf("export with failed source teardown = %q, %v", result, err)
	}
	stored, getErr := o.st.Get(context.Background(), sb.ID)
	if getErr != nil || stored == nil || o.lookup(sb.ID) == nil {
		t.Fatalf("failed teardown lost source row/cache = %+v, %v", stored, getErr)
	}
	if _, statErr := os.Stat(sb.RunDir); statErr != nil {
		t.Fatalf("failed teardown removed source run directory: %v", statErr)
	}
	if vs.detachCalls.Load() != 0 {
		t.Fatalf("failed Stop advanced to detach: %d", vs.detachCalls.Load())
	}

	result, err = o.ExportSandbox(context.Background(), apiKey, sb.ID, true, false)
	if err != nil {
		t.Fatalf("retry export: %v", err)
	}
	if template, parseErr := types.ParseTemplateID(result); parseErr != nil || template.Ref != sb.ResumeSource.Ref {
		t.Fatalf("retry template result = %#v, %v", template, parseErr)
	}
	stored, getErr = o.st.Get(context.Background(), sb.ID)
	if getErr != nil || stored != nil || o.lookup(sb.ID) != nil {
		t.Fatalf("retry retained source row/cache = %+v, %v", stored, getErr)
	}
	if _, statErr := os.Stat(sb.RunDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("retry retained source run directory: %v", statErr)
	}
	if lc.stopCalls.Load() != 2 || lc.resetCalls.Load() != 1 || vs.detachCalls.Load() != 1 {
		t.Fatalf("retry cleanup calls stop=%d reset=%d detach=%d, want 2/1/1",
			lc.stopCalls.Load(), lc.resetCalls.Load(), vs.detachCalls.Load())
	}
}

func TestExportKeepSourcePreservesLocalResumeSource(t *testing.T) {
	fixture := newExportResumeFixture(t)
	portableRef := "manifest://" + strings.Repeat("2", 64)
	publisher := newBlockingExportPublisher(portableRef)
	publisher.Release()
	fixture.o.artifactPublisher = publisher.Publish
	localSource := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: fixture.localRef}

	result, err := fixture.o.ExportSandbox(fixture.ctx, fixture.apiKey, fixture.sb.ID, false, true)
	if err != nil || !strings.HasPrefix(result, "kmt1.") {
		t.Fatalf("retained KMT export = %q, %v", result, err)
	}
	// The source row keeps its original local ResumeSource — keep-source
	// produces a result but does not change the source (#336).
	stored, err := fixture.o.st.Get(fixture.ctx, fixture.sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.ResumeSource != localSource {
		t.Fatalf("retained source = %+v, %v", stored, err)
	}
	if cached := fixture.o.lookup(fixture.sb.ID); cached == nil || cached.ResumeSource != localSource {
		t.Fatalf("cached source = %+v, want local ref", cached)
	}
	// The local checkpoint is retained, unchanged.
	if _, err := os.Stat(fixture.localRef); err != nil {
		t.Fatalf("keep-source removed the local snapshot: %v", err)
	}

	// Resume uses the same original local ref — the runner receives it
	// verbatim, proving the export did not alter the restore path.
	restored := make(chan string, 1)
	fixture.launcher.snapshotRoots = restored
	connected, err := fixture.o.Connect(fixture.ctx, fixture.sb.ID, fixture.apiKey, "", api.ConnectOptions{})
	if err != nil || connected == nil || connected.State != types.StateStarting {
		t.Fatalf("Connect after keep-source export = %+v, %v", connected, err)
	}
	select {
	case ref := <-restored:
		if ref != fixture.localRef {
			t.Fatalf("Resume restore ref = %q, want local %q", ref, fixture.localRef)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner task did not receive the local root snapshot")
	}
	waitForSandbox(t, fixture.o, fixture.ctx, fixture.sb.ID, func(current *types.Sandbox) bool {
		return current.State == types.StateRunning
	}, "running after keep-source export")
}

func TestDetachedTemplateUploadDoesNotBlockResumeCommitButFencesKill(t *testing.T) {
	fixture := newExportResumeFixture(t)
	publisher := newBlockingExportPublisher("manifest://" + strings.Repeat("e", 64))
	t.Cleanup(publisher.Release)
	fixture.o.artifactPublisher = publisher.Publish
	exportDone := startExport(fixture.o, fixture.ctx, fixture.apiKey, fixture.sb.ID, true, false)
	waitPublisherStarted(t, publisher)

	connected, err := fixture.o.Connect(fixture.ctx, fixture.sb.ID, fixture.apiKey, "", api.ConnectOptions{})
	if err != nil || connected == nil || connected.State != types.StateStarting {
		t.Fatalf("Connect during template export = %+v, %v", connected, err)
	}
	waitForSandbox(t, fixture.o, fixture.ctx, fixture.sb.ID, func(current *types.Sandbox) bool {
		return current.State == types.StateRunning
	}, "running while template upload remains blocked")

	killDone := make(chan struct {
		killed bool
		err    error
	}, 1)
	go func() {
		killed, err := fixture.o.Kill(fixture.ctx, fixture.sb.ID, fixture.apiKey)
		killDone <- struct {
			killed bool
			err    error
		}{killed: killed, err: err}
	}()
	select {
	case result := <-killDone:
		t.Fatalf("Kill bypassed active template export fence: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}

	publisher.Release()
	if result := waitExportResult(t, exportDone); result.err != nil {
		t.Fatalf("detached template export: %v", result.err)
	}
	select {
	case result := <-killDone:
		if result.err != nil || !result.killed {
			t.Fatalf("Kill after template export = %+v", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Kill remained blocked after template export completed")
	}
}

func TestCanceledExportSkipsSourceFinalization(t *testing.T) {
	fixture := newExportResumeFixture(t)
	publisher := newBlockingExportPublisher("manifest://" + strings.Repeat("f", 64))
	t.Cleanup(publisher.Release)
	fixture.o.artifactPublisher = publisher.Publish
	requestCtx, cancel := context.WithCancel(fixture.ctx)
	done := startExport(fixture.o, requestCtx, fixture.apiKey, fixture.sb.ID, true, false)
	waitPublisherStarted(t, publisher)
	cancel()

	result := waitExportResult(t, done)
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("canceled ExportSandbox error = %v", result.err)
	}
	stored, err := fixture.o.st.Get(fixture.ctx, fixture.sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.ResumeSource != (types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: fixture.localRef}) {
		t.Fatalf("source after canceled export = %+v, %v", stored, err)
	}
	if _, err := os.Stat(fixture.localRef); err != nil {
		t.Fatalf("canceled export removed local snapshot: %v", err)
	}
}

func TestExportRejectsAfterLifecycleCancellation(t *testing.T) {
	fixture := newExportResumeFixture(t)
	serviceCtx, stopService := context.WithCancel(context.Background())
	fixture.o.SetLifecycleContext(serviceCtx)
	stopService()

	result, err := fixture.o.ExportSandbox(context.Background(), fixture.apiKey, fixture.sb.ID, true, false)
	if result != "" || !errors.Is(err, context.Canceled) {
		t.Fatalf("ExportSandbox after lifecycle cancellation = %q, %v", result, err)
	}
	stored, getErr := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if getErr != nil || stored == nil || stored.State != types.StatePaused || stored.ResumeSource != (types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: fixture.localRef}) {
		t.Fatalf("rejected export changed source = %+v, %v", stored, getErr)
	}
	if _, statErr := os.Stat(fixture.localRef); statErr != nil {
		t.Fatalf("rejected export removed local snapshot: %v", statErr)
	}
}

func TestExportLifecycleCancellationBeforeFinalizerPreservesSource(t *testing.T) {
	fixture := newExportResumeFixture(t)
	serviceCtx, stopService := context.WithCancel(context.Background())
	fixture.o.SetLifecycleContext(serviceCtx)
	publisher := newBlockingExportPublisher("manifest://" + strings.Repeat("8", 64))
	t.Cleanup(publisher.Release)
	fixture.o.artifactPublisher = publisher.Publish
	done := startExport(fixture.o, context.Background(), fixture.apiKey, fixture.sb.ID, true, false)
	waitPublisherStarted(t, publisher)

	unlock := fixture.o.lifecycle.Lock(fixture.sb.ID)
	publisher.Release()
	select {
	case <-publisher.finished:
	case <-time.After(3 * time.Second):
		unlock()
		t.Fatal("snapshot publisher did not finish")
	}
	stopService()
	unlock()

	result := waitExportResult(t, done)
	if result.result != "" || !errors.Is(result.err, context.Canceled) {
		t.Fatalf("shutdown-fenced export = %q, %v", result.result, result.err)
	}
	stored, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.ResumeSource != (types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: fixture.localRef}) {
		t.Fatalf("shutdown-fenced export changed source = %+v, %v", stored, err)
	}
	if _, err := os.Stat(fixture.localRef); err != nil {
		t.Fatalf("shutdown-fenced export removed local snapshot: %v", err)
	}
	if err := fixture.o.DrainPauses(context.Background()); err != nil {
		t.Fatalf("drain shutdown-fenced export: %v", err)
	}
}

func TestDrainPausesWaitsForAcceptedExport(t *testing.T) {
	fixture := newExportResumeFixture(t)
	publisher := newBlockingExportPublisher("manifest://" + strings.Repeat("9", 64))
	t.Cleanup(publisher.Release)
	fixture.o.artifactPublisher = publisher.Publish
	done := startExport(fixture.o, context.Background(), fixture.apiKey, fixture.sb.ID, true, true)
	waitPublisherStarted(t, publisher)

	drainDone := make(chan error, 1)
	go func() { drainDone <- fixture.o.DrainPauses(context.Background()) }()
	select {
	case err := <-drainDone:
		t.Fatalf("DrainPauses returned before accepted export completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if result, err := fixture.o.ExportSandbox(context.Background(), fixture.apiKey, fixture.sb.ID, true, true); result != "" || !errors.Is(err, context.Canceled) {
		t.Fatalf("ExportSandbox after drain admission closed = %q, %v", result, err)
	}

	publisher.Release()
	if result := waitExportResult(t, done); result.err != nil {
		t.Fatalf("accepted export after release: %v", result.err)
	}
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("DrainPauses: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("DrainPauses did not observe accepted export completion")
	}
}
