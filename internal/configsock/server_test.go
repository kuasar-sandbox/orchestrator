package configsock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

// --- plane stubs ---

type stubProvider struct {
	pidFile         string
	assignmentErr   error
	buildSpecErr    error
	buildPrepErr    error
	resultErr       error
	phaseErr        error
	sandboxAuth     SandboxTaskAuth
	sandboxAuthHits *atomic.Int32
	bootstrapHits   *atomic.Int32
	prepareHits     *atomic.Int32
	prepareErr      error
	buildAuthHits   *atomic.Int32
	buildSpecHits   *atomic.Int32
	buildPrepHits   *atomic.Int32
	sessions        *stubRunSessionRegistry
}

type stubRunSessionRegistry struct {
	mu         sync.Mutex
	registered []RunSessionRequest
	closed     []bool
	closeCh    chan bool
}

type stubRunSession struct {
	registry *stubRunSessionRegistry
}

func (s *stubRunSession) Close(shutdown bool) {
	if s == nil || s.registry == nil {
		return
	}
	s.registry.mu.Lock()
	s.registry.closed = append(s.registry.closed, shutdown)
	ch := s.registry.closeCh
	s.registry.mu.Unlock()
	if ch != nil {
		select {
		case ch <- shutdown:
		default:
		}
	}
}

func (s stubProvider) SandboxTaskAuth(_ context.Context, sandboxID, runID string) (SandboxTaskAuth, bool, error) {
	if s.sandboxAuthHits != nil {
		s.sandboxAuthHits.Add(1)
	}
	if sandboxID != "x" || runID != "sr-test" {
		return SandboxTaskAuth{}, false, nil
	}
	auth := s.sandboxAuth
	if auth.PidFile == "" {
		auth.PidFile = s.pidFile
	}
	return auth, true, nil
}

func (s stubProvider) SandboxTaskSpecFor(_ context.Context, sandboxID, runID string) (*SandboxTaskSpec, bool, error) {
	if s.bootstrapHits != nil {
		s.bootstrapHits.Add(1)
	}
	if sandboxID != "x" || runID != "sr-test" {
		return nil, false, nil
	}
	return &SandboxTaskSpec{
		SandboxID: sandboxID, RunID: runID,
		Final: &LaunchSpec{Exec: "/bin/true", Args: []string{"run"}},
	}, true, nil
}

func (s stubProvider) CompleteSandboxPrepare(_ context.Context, sandboxID, runID string, _ ArtifactPrepareSummary) (*LaunchSpec, error) {
	if s.prepareHits != nil {
		s.prepareHits.Add(1)
	}
	if s.prepareErr != nil {
		return nil, s.prepareErr
	}
	if sandboxID != "x" || runID != "sr-test" {
		return nil, RejectArtifactPrepare(os.ErrNotExist)
	}
	return &LaunchSpec{Exec: "/bin/true", Args: []string{"run"}}, nil
}

func (s stubProvider) BuildTaskAuth(_ context.Context, buildID, runID string) (BuildTaskAuth, bool, error) {
	if s.buildAuthHits != nil {
		s.buildAuthHits.Add(1)
	}
	if buildID != "x" || runID != "br-test" {
		return BuildTaskAuth{}, false, nil
	}
	return BuildTaskAuth{PidFile: s.pidFile}, true, nil
}

func (s stubProvider) BuildTaskSpecFor(_ context.Context, buildID, runID string) (*BuildTaskSpec, bool, error) {
	if s.buildSpecHits != nil {
		s.buildSpecHits.Add(1)
	}
	if s.buildSpecErr != nil {
		return nil, false, s.buildSpecErr
	}
	if buildID != "x" || runID != "br-test" {
		return nil, false, nil
	}
	return &BuildTaskSpec{
		BuildID: buildID, RunID: runID,
		Final: &BuildSpec{BuildID: buildID, RunID: runID, RunDir: "/run/build", BaseDir: "/base/build"},
	}, true, nil
}

func (s stubProvider) CompleteBuildPrepare(_ context.Context, buildID, runID string, _ ArtifactPrepareSummary) (*BuildSpec, error) {
	if s.buildPrepHits != nil {
		s.buildPrepHits.Add(1)
	}
	if s.buildPrepErr != nil {
		return nil, s.buildPrepErr
	}
	if buildID != "x" || runID != "br-test" {
		return nil, RejectBuildPrepare(os.ErrNotExist)
	}
	return &BuildSpec{BuildID: buildID, RunID: runID, RunDir: "/run/build", BaseDir: "/base/build"}, nil
}

func (s stubProvider) RunPidFile(kind, runID string) (string, bool) {
	if kind == "sandbox" && runID == "sr-test" {
		return s.pidFile, true
	}
	if kind == "build" && runID == "br-test" {
		return s.pidFile, true
	}
	return "", false
}

func (s stubProvider) RegisterRunSession(_ context.Context, kind, runID string) (RunSessionRegistration, bool, error) {
	if kind != "sandbox" && kind != "build" {
		return nil, false, nil
	}
	if (kind == "sandbox" && runID != "sr-test") || (kind == "build" && runID != "br-test") {
		return nil, false, nil
	}
	if s.sessions != nil {
		s.sessions.mu.Lock()
		s.sessions.registered = append(s.sessions.registered, RunSessionRequest{Kind: kind, RunID: runID})
		s.sessions.mu.Unlock()
	}
	return &stubRunSession{registry: s.sessions}, true, nil
}

func (s stubProvider) WaitAssignment(_ context.Context, kind, runID string) (string, bool, error) {
	if s.assignmentErr != nil {
		return "", false, s.assignmentErr
	}
	if kind == "sandbox" && runID == "sr-test" {
		return "x", true, nil
	}
	if kind == "build" && runID == "br-test" {
		return "x", true, nil
	}
	return "", false, nil
}

func (s stubProvider) PostSandboxResult(_ context.Context, runID, sandboxID string, _ SandboxExecutionResult) error {
	if s.resultErr != nil {
		return s.resultErr
	}
	if runID == "sr-test" && sandboxID == "x" {
		return nil
	}
	return os.ErrNotExist
}

func (s stubProvider) PostBuildResult(_ context.Context, runID, buildID string, _ BuildResult) error {
	if s.resultErr != nil {
		return s.resultErr
	}
	if runID == "br-test" && buildID == "x" {
		return nil
	}
	return os.ErrNotExist
}

func (s stubProvider) PostBuildPhase(_ context.Context, runID, buildID, phase, sandboxID, state string) error {
	if s.phaseErr != nil {
		return s.phaseErr
	}
	if runID == "br-test" && buildID == "x" && phase == "a" && sandboxID == "bp-a-x" && state == "starting" {
		return nil
	}
	return os.ErrNotExist
}

type stubAdmin struct{ pairs map[string]AdminKeyInfo }

type stubBuilderAdmissionAdmin struct{}

func (stubBuilderAdmissionAdmin) BuilderAdmissionStatus(context.Context) (BuilderAdmissionStatus, error) {
	available := int64(3)
	return BuilderAdmissionStatus{
		Registration: BuildAdmissionLevelStatus{
			Configured: types.BuildAdmissionLimit{MaxBuilds: 4}, UsedBuilds: 1,
			Used:      types.BuildResources{CPU: 1500, Memory: 2 << 30},
			Available: BuildAdmissionHeadroom{MaxBuilds: &available},
		},
		WaitingBuilds: 1,
	}, nil
}

func stubKeyPair(manifestKey, apiSecret string) (string, string, string) {
	if apiSecret == "" {
		apiSecret = "derived-" + manifestKey
	}
	apiFP := "api-fp-" + apiSecret
	manifestFP := "manifest-fp-" + manifestKey
	return apiFP + "\x00" + manifestFP, apiFP, manifestFP
}

func (a *stubAdmin) AddKeyPair(_ context.Context, manifestKey, apiSecret, label string, _ int64, _ string) (bool, string, string, error) {
	key, apiFP, manifestFP := stubKeyPair(manifestKey, apiSecret)
	if _, ok := a.pairs[key]; ok {
		return false, apiFP, manifestFP, nil
	}
	a.pairs[key] = AdminKeyInfo{APISecretFingerprint: apiFP, ManifestKeyFingerprint: manifestFP, Label: label}
	return true, apiFP, manifestFP, nil
}

func (a *stubAdmin) RemoveKeyPair(_ context.Context, manifestKey, apiSecret string) (bool, string, string, error) {
	key, apiFP, manifestFP := stubKeyPair(manifestKey, apiSecret)
	if _, ok := a.pairs[key]; ok {
		delete(a.pairs, key)
		return true, apiFP, manifestFP, nil
	}
	return false, apiFP, manifestFP, nil
}

func (a *stubAdmin) HasKeyPair(_ context.Context, manifestKey, apiSecret string) (bool, string, string, error) {
	key, apiFP, manifestFP := stubKeyPair(manifestKey, apiSecret)
	_, ok := a.pairs[key]
	return ok, apiFP, manifestFP, nil
}

func (a *stubAdmin) ListKeyPairs(_ context.Context) ([]AdminKeyInfo, error) {
	out := make([]AdminKeyInfo, 0, len(a.pairs))
	for _, pair := range a.pairs {
		out = append(out, pair)
	}
	return out, nil
}

// --- harness ---

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func startTestServer(t *testing.T, deps Deps) (string, *http.Client) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sock, client, _ := startTestServerWithContext(t, ctx, deps)
	return sock, client
}

func startTestServerWithContext(t *testing.T, ctx context.Context, deps Deps) (string, *http.Client, <-chan error) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "ctl.sock")
	done := make(chan error, 1)
	go func() { done <- New(sock, deps, discardLogger()).Serve(ctx) }()
	for range 400 { // wait for ListenUnix + chmod to publish the socket
		if _, err := os.Stat(sock); err == nil {
			return sock, HTTPClient(sock), done
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("server did not bind")
	return "", nil, done
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func rawPost(t *testing.T, client *http.Client, path string, body any) (int, []byte) {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := client.Post("http://localhost"+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, rb
}

func rawPostBytes(t *testing.T, client *http.Client, path string, body []byte) (int, []byte) {
	t.Helper()
	resp, err := client.Post("http://localhost"+path, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, rb
}

func adminPost(t *testing.T, client *http.Client, req AdminKeyRequest) AdminKeyResponse {
	t.Helper()
	_, body := rawPost(t, client, PathAdminManifestKey, req)
	var r AdminKeyResponse
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("decode admin resp: %v (%s)", err, body)
	}
	return r
}

// --- tests ---

// TestTaskPlane: peer pid (== this test's pid, both ends same process) must match the
// id's pidfile.
func TestTaskPlane(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "id.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	sock, _ := startTestServer(t, Deps{Provider: stubProvider{pidFile: pf}})

	spec, err := FetchSandboxTaskSpec(context.Background(), sock, "x", "sr-test")
	if err != nil || spec.Final == nil || spec.Final.Exec != "/bin/true" {
		t.Fatalf("happy path: spec=%+v err=%v", spec, err)
	}
	if _, err := FetchSandboxTaskSpec(context.Background(), sock, "nope", "sr-test"); err == nil {
		t.Fatal("unknown id should error")
	}
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1)) // pidfile no longer matches peer
	if _, err := FetchSandboxTaskSpec(context.Background(), sock, "x", "sr-test"); err == nil || err.Error() != "not authorized" {
		t.Fatalf("wrong pid should be not authorized, got %v", err)
	}
}

func TestSandboxBootstrapAuthenticatesBeforeSecretProvider(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "task.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1))
	var secretCalls atomic.Int32
	sock, _ := startTestServer(t, Deps{Provider: stubProvider{pidFile: pf, bootstrapHits: &secretCalls}})
	if _, err := FetchSandboxTaskSpec(context.Background(), sock, "x", "sr-test"); err == nil || err.Error() != "not authorized" {
		t.Fatalf("unauthorized bootstrap error = %v", err)
	}
	if got := secretCalls.Load(); got != 0 {
		t.Fatalf("secret-bearing provider calls = %d, want 0", got)
	}
}

func TestSandboxBootstrapRejectsPreV3SchemasBeforeProviders(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "task.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			var authCalls, secretCalls atomic.Int32
			_, client := startTestServer(t, Deps{Provider: stubProvider{
				pidFile: pf, sandboxAuthHits: &authCalls, bootstrapHits: &secretCalls,
			}})
			status, _ := rawPost(t, client, PathTaskSandboxBootstrap, SandboxTaskRequest{
				SandboxID: "x", RunID: "sr-test", Version: version,
			})
			if status != http.StatusBadRequest {
				t.Fatalf("unsupported sandbox bootstrap status = %d, want %d", status, http.StatusBadRequest)
			}
			if authCalls.Load() != 0 || secretCalls.Load() != 0 {
				t.Fatalf("unsupported schema reached providers: auth=%d secret=%d", authCalls.Load(), secretCalls.Load())
			}
		})
	}
}

func TestTaskRequestsRejectAmbiguousOrOversizedBodiesBeforeProviders(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "task.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	var sandboxAuthCalls, buildAuthCalls atomic.Int32
	_, client := startTestServer(t, Deps{Provider: stubProvider{
		pidFile: pf, sandboxAuthHits: &sandboxAuthCalls, buildAuthHits: &buildAuthCalls,
	}})

	tests := []struct {
		name       string
		path       string
		body       string
		wantStatus int
	}{
		{
			name: "sandbox bootstrap duplicate", path: PathTaskSandboxBootstrap,
			body:       `{"sandbox_id":"x","sandbox_id":"other","run_id":"sr-test","version":1}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "sandbox prepare trailing", path: PathTaskSandboxPrepare,
			body:       `{"sandbox_id":"x","run_id":"sr-test","summary":{}} {}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "build bootstrap unknown", path: PathTaskBuildBootstrap,
			body:       `{"build_id":"x","run_id":"br-test","version":2,"extra":true}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "build prepare duplicate nested", path: PathTaskBuildPrepare,
			body:       `{"build_id":"x","run_id":"br-test","version":2,"summary":{"schema_version":1,"schema_version":1}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "sandbox bootstrap oversized", path: PathTaskSandboxBootstrap,
			body:       `{"sandbox_id":"x","run_id":"sr-test","version":1,"padding":"` + strings.Repeat("x", int(taskBootstrapRequestMaxBytes)) + `"}`,
			wantStatus: http.StatusRequestEntityTooLarge,
		},
		{
			name: "build prepare oversized", path: PathTaskBuildPrepare,
			body:       strings.Repeat(" ", int(taskPrepareRequestMaxBytes)+1),
			wantStatus: http.StatusRequestEntityTooLarge,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, _ := rawPostBytes(t, client, tt.path, []byte(tt.body))
			if status != tt.wantStatus {
				t.Fatalf("status = %d, want %d", status, tt.wantStatus)
			}
		})
	}
	if got := sandboxAuthCalls.Load(); got != 0 {
		t.Fatalf("malformed sandbox requests reached auth provider %d times", got)
	}
	if got := buildAuthCalls.Load(); got != 0 {
		t.Fatalf("malformed build requests reached auth provider %d times", got)
	}
}

func TestSandboxPrepareAuthenticatesBeforeCompletionProvider(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "task.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1))
	var completionCalls atomic.Int32
	sock, _ := startTestServer(t, Deps{Provider: stubProvider{pidFile: pf, prepareHits: &completionCalls}})
	summary := ArtifactPrepareSummary{SchemaVersion: ArtifactPrepareSchemaVersion, ResolutionDigest: strings.Repeat("0", 64), RequiredRefCount: 1}
	if _, err := CompleteSandboxPrepare(context.Background(), sock, "x", "sr-test", summary); err == nil || err.Error() != "not authorized" {
		t.Fatalf("unauthorized completion error = %v", err)
	}
	if got := completionCalls.Load(); got != 0 {
		t.Fatalf("completion provider calls = %d, want 0", got)
	}

	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	final, err := CompleteSandboxPrepare(context.Background(), sock, "x", "sr-test", summary)
	if err != nil || final == nil || final.Exec != "/bin/true" {
		t.Fatalf("authorized completion = %+v, %v", final, err)
	}
	if got := completionCalls.Load(); got != 1 {
		t.Fatalf("completion provider calls = %d, want 1", got)
	}
}

func TestSandboxPrepareConflictIsDefinitive(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "task.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	sock, _ := startTestServer(t, Deps{Provider: stubProvider{
		pidFile: pf, prepareErr: RejectArtifactPrepare(errors.New("conflicting replay")),
	}})
	_, err := CompleteSandboxPrepare(context.Background(), sock, "x", "sr-test", ArtifactPrepareSummary{})
	if err == nil || IsRetryableError(err) || err.Error() != "conflicting replay" {
		t.Fatalf("conflict error = %v, retryable=%t", err, IsRetryableError(err))
	}
}

func TestBuildBootstrapAuthenticatesBeforeSecretProvider(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "builder.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1))
	var secretCalls atomic.Int32
	sock, _ := startTestServer(t, Deps{Provider: stubProvider{pidFile: pf, buildSpecHits: &secretCalls}})
	if _, err := FetchBuildTaskSpec(context.Background(), sock, "x", "br-test"); err == nil || err.Error() != "not authorized" {
		t.Fatalf("unauthorized build bootstrap error = %v", err)
	}
	if got := secretCalls.Load(); got != 0 {
		t.Fatalf("secret-bearing build provider calls = %d, want 0", got)
	}
}

func TestBuildBootstrapRejectsPreviousVersionsBeforeProviders(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "builder.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	for version := 1; version < BuildTaskSchemaVersion; version++ {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			var authCalls, secretCalls atomic.Int32
			_, client := startTestServer(t, Deps{Provider: stubProvider{
				pidFile: pf, buildAuthHits: &authCalls, buildSpecHits: &secretCalls,
			}})
			status, _ := rawPost(t, client, PathTaskBuildBootstrap, BuildTaskRequest{
				BuildID: "x", RunID: "br-test", Version: version,
			})
			if status != http.StatusBadRequest {
				t.Fatalf("unsupported build bootstrap status = %d, want %d", status, http.StatusBadRequest)
			}
			if authCalls.Load() != 0 || secretCalls.Load() != 0 {
				t.Fatalf("unsupported schema reached providers: auth=%d secret=%d", authCalls.Load(), secretCalls.Load())
			}
		})
	}
}

func TestBuildPrepareAuthenticatesBeforeCompletionProvider(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "builder.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1))
	var completionCalls atomic.Int32
	provider := stubProvider{pidFile: pf, buildPrepHits: &completionCalls}
	sock, _ := startTestServer(t, Deps{Provider: provider})
	summary := ArtifactPrepareSummary{SchemaVersion: ArtifactPrepareSchemaVersion, ResolutionDigest: strings.Repeat("0", 64), RequiredRefCount: 1}
	if _, err := CompleteBuildPrepare(context.Background(), sock, "x", "br-test", summary); err == nil || err.Error() != "not authorized" {
		t.Fatalf("unauthorized build completion error = %v", err)
	}
	if got := completionCalls.Load(); got != 0 {
		t.Fatalf("build completion provider calls = %d, want 0", got)
	}

	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	final, err := CompleteBuildPrepare(context.Background(), sock, "x", "br-test", summary)
	if err != nil || final == nil || final.BuildID != "x" {
		t.Fatalf("authorized build completion = %+v, %v", final, err)
	}
	if got := completionCalls.Load(); got != 1 {
		t.Fatalf("build completion provider calls = %d, want 1", got)
	}
}

func TestBuildPrepareRejectsUnversionedV1BeforeProviders(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "builder.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	var authCalls, completionCalls atomic.Int32
	_, client := startTestServer(t, Deps{Provider: stubProvider{
		pidFile: pf, buildAuthHits: &authCalls, buildPrepHits: &completionCalls,
	}})
	status, _ := rawPost(t, client, PathTaskBuildPrepare, BuildPrepareRequest{
		BuildID: "x", RunID: "br-test", Summary: ArtifactPrepareSummary{
			SchemaVersion: ArtifactPrepareSchemaVersion,
		},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("unversioned build prepare status = %d, want %d", status, http.StatusBadRequest)
	}
	if authCalls.Load() != 0 || completionCalls.Load() != 0 {
		t.Fatalf("unversioned build prepare reached providers: auth=%d completion=%d",
			authCalls.Load(), completionCalls.Load())
	}
}

func TestBuildPrepareRejectsPreviousVersionsBeforeProviders(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "builder.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	for version := 1; version < BuildTaskSchemaVersion; version++ {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			var authCalls, completionCalls atomic.Int32
			_, client := startTestServer(t, Deps{Provider: stubProvider{
				pidFile: pf, buildAuthHits: &authCalls, buildPrepHits: &completionCalls,
			}})
			status, _ := rawPost(t, client, PathTaskBuildPrepare, BuildPrepareRequest{
				BuildID: "x", RunID: "br-test", Version: version, Summary: ArtifactPrepareSummary{
					SchemaVersion: ArtifactPrepareSchemaVersion,
				},
			})
			if status != http.StatusBadRequest {
				t.Fatalf("v%d build prepare status = %d, want %d", version, status, http.StatusBadRequest)
			}
			if authCalls.Load() != 0 || completionCalls.Load() != 0 {
				t.Fatalf("v%d build prepare reached providers: auth=%d completion=%d",
					version, authCalls.Load(), completionCalls.Load())
			}
		})
	}
}

func TestBuildSourceDocumentsDoNotCrossTaskWire(t *testing.T) {
	body, err := json.Marshal(BuildSpec{
		BuildID: "build", SourceSandboxRef: "manifest://task-local-only",
		SourceSandboxConfig: &rtconfig.PortableSandboxConfig{
			Metadata: map[string]string{"command": "secret-local-metadata"},
		},
		SourceImageConfig: []byte(`{"Env":["SECRET=value"]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "task-local-only") || strings.Contains(string(body), "secret-local-metadata") ||
		strings.Contains(string(body), "SECRET=value") {
		t.Fatalf("task-local source documents crossed wire: %s", body)
	}
}

func TestBuildPublicationPolicyWireUsesOnlyCheckpointSpecificFields(t *testing.T) {
	body, err := json.Marshal(BuildSpec{
		CheckpointRefLocationParent: "file:///mnt/checkpoints",
		CheckpointRemoteManifest:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := string(body)
	for _, want := range []string{
		`"checkpoint_ref_location_parent":"file:///mnt/checkpoints"`,
		`"checkpoint_remote_manifest":true`,
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("BuildSpec wire %s missing %s", raw, want)
		}
	}
	if strings.Contains(raw, "publish_location_parent") {
		t.Fatalf("BuildSpec wire retained removed field: %s", raw)
	}
}

func TestBuildClientClassifiesOnlyTransportInterruptionsAsRetryable(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.sock")
	if _, err := WaitAssignment(context.Background(), missing, "build", "br-test"); !IsTransportError(err) {
		t.Fatalf("assignment transport error = %v, retryable=%t", err, IsTransportError(err))
	}
	if _, err := FetchBuildTaskSpec(context.Background(), missing, "test", "br-test"); !IsTransportError(err) {
		t.Fatalf("build-spec transport error = %v, retryable=%t", err, IsTransportError(err))
	}

	pf := filepath.Join(t.TempDir(), "id.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	sock, _ := startTestServer(t, Deps{Provider: stubProvider{pidFile: pf}})
	if _, err := FetchBuildTaskSpec(context.Background(), sock, "unknown", "br-test"); err == nil || IsTransportError(err) {
		t.Fatalf("provider rejection = %v, retryable=%t", err, IsTransportError(err))
	}
}

func TestConfigSocketClientsDoNotRetainIdleConnections(t *testing.T) {
	client := HTTPClient(filepath.Join(t.TempDir(), "ctl.sock"))
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("HTTPClient transport = %T, want *http.Transport", client.Transport)
	}
	if !transport.DisableKeepAlives {
		t.Fatal("HTTPClient retains idle UDS connections across short-lived retry clients")
	}
}

func TestBuildRetryClassification(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "id.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	transient := errors.New("temporary sqlite write failure")

	for name, call := range map[string]func(string) error{
		"assignment": func(sock string) error {
			_, err := WaitAssignment(context.Background(), sock, "build", "br-test")
			return err
		},
		"build bootstrap": func(sock string) error {
			_, err := FetchBuildTaskSpec(context.Background(), sock, "x", "br-test")
			return err
		},
		"build prepare": func(sock string) error {
			_, err := CompleteBuildPrepare(context.Background(), sock, "x", "br-test", ArtifactPrepareSummary{})
			return err
		},
		"result": func(sock string) error {
			return PostBuildResultContext(context.Background(), sock, "br-test", "x", BuildResult{ImageRef: "image"})
		},
		"phase": func(sock string) error {
			return PostBuildPhaseContext(context.Background(), sock, "br-test", "x", "a", "bp-a-x", "starting")
		},
	} {
		t.Run(name, func(t *testing.T) {
			provider := stubProvider{pidFile: pf}
			switch name {
			case "assignment":
				provider.assignmentErr = transient
			case "build bootstrap":
				provider.buildSpecErr = transient
			case "build prepare":
				provider.buildPrepErr = transient
			case "result":
				provider.resultErr = transient
			case "phase":
				provider.phaseErr = transient
			}
			sock, _ := startTestServer(t, Deps{Provider: provider})
			err := call(sock)
			if err == nil || !IsRetryableError(err) {
				t.Fatalf("5xx error = %v, retryable=%t", err, IsRetryableError(err))
			}
			var response *retryableResponseError
			if !errors.As(err, &response) || response.status != http.StatusInternalServerError {
				t.Fatalf("retryable response = %#v, want preserved status 500", err)
			}
		})
	}

	t.Run("reject build prepare", func(t *testing.T) {
		provider := stubProvider{pidFile: pf, buildPrepErr: RejectBuildPrepare(errors.New("prepare digest conflict"))}
		sock, _ := startTestServer(t, Deps{Provider: provider})
		_, err := CompleteBuildPrepare(context.Background(), sock, "x", "br-test", ArtifactPrepareSummary{})
		if err == nil || IsRetryableError(err) || err.Error() != "prepare digest conflict" {
			t.Fatalf("prepare rejection = %v, retryable=%t", err, IsRetryableError(err))
		}
	})

	for name, provider := range map[string]stubProvider{
		"result": {pidFile: pf, resultErr: RejectBuildReport(errors.New("result ownership lost"))},
		"phase":  {pidFile: pf, phaseErr: RejectBuildReport(errors.New("phase ownership lost"))},
	} {
		t.Run("reject "+name, func(t *testing.T) {
			sock, _ := startTestServer(t, Deps{Provider: provider})
			var err error
			if name == "result" {
				err = PostBuildResultContext(context.Background(), sock, "br-test", "x", BuildResult{})
			} else {
				err = PostBuildPhaseContext(context.Background(), sock, "br-test", "x", "a", "bp-a-x", "starting")
			}
			if err == nil || IsRetryableError(err) {
				t.Fatalf("definitive error = %v, retryable=%t", err, IsRetryableError(err))
			}
		})
	}
}

func TestRunSessionFlushesAckAndClosesOnClientDisconnect(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "run.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	registry := &stubRunSessionRegistry{closeCh: make(chan bool, 1)}
	sock, _ := startTestServer(t, Deps{Provider: stubProvider{pidFile: pf, sessions: registry}})
	session, err := OpenRunSession(context.Background(), sock, "sandbox", "sr-test")
	if err != nil {
		t.Fatalf("OpenRunSession: %v", err)
	}
	registry.mu.Lock()
	registered := append([]RunSessionRequest(nil), registry.registered...)
	registry.mu.Unlock()
	if len(registered) != 1 || registered[0].Kind != "sandbox" || registered[0].RunID != "sr-test" {
		t.Fatalf("registered sessions = %+v", registered)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Close session: %v", err)
	}
	select {
	case shutdown := <-registry.closeCh:
		if shutdown {
			t.Fatal("client disconnect was reported as shutdown")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session close did not reach provider")
	}
}

func TestRunSessionAuthenticatesBeforeRegistration(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "run.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1))
	registry := &stubRunSessionRegistry{}
	sock, _ := startTestServer(t, Deps{Provider: stubProvider{pidFile: pf, sessions: registry}})
	if _, err := OpenRunSession(context.Background(), sock, "sandbox", "sr-test"); err == nil || err.Error() != "not authorized" {
		t.Fatalf("unauthorized session error = %v", err)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.registered) != 0 {
		t.Fatalf("unauthorized session reached provider: %+v", registry.registered)
	}
}

func TestRunSessionShutdownCloseIsSuppressed(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "run.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	registry := &stubRunSessionRegistry{closeCh: make(chan bool, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	sock, _, done := startTestServerWithContext(t, ctx, Deps{Provider: stubProvider{pidFile: pf, sessions: registry}})
	session, err := OpenRunSession(context.Background(), sock, "sandbox", "sr-test")
	if err != nil {
		t.Fatalf("OpenRunSession: %v", err)
	}
	cancel()
	select {
	case shutdown := <-registry.closeCh:
		if !shutdown {
			t.Fatal("server shutdown was reported as ordinary disconnect")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not close long run session")
	}
	select {
	case <-session.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("client session did not observe shutdown")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop after cancel")
	}
}

func TestRunPlane(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "run.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	sock, _ := startTestServer(t, Deps{Provider: stubProvider{pidFile: pf}})

	taskID, err := WaitAssignment(context.Background(), sock, "sandbox", "sr-test")
	if err != nil || taskID != "x" {
		t.Fatalf("WaitAssignment = %q, %v; want x, nil", taskID, err)
	}
	if err := PostBuildResult(sock, "br-test", "x", BuildResult{ImageRef: "image"}); err != nil {
		t.Fatalf("PostBuildResult: %v", err)
	}
	if err := PostBuildPhase(sock, "br-test", "x", "a", "bp-a-x", "starting"); err != nil {
		t.Fatalf("PostBuildPhase: %v", err)
	}
	if _, err := WaitAssignment(context.Background(), sock, "sandbox", "sr-missing"); err == nil {
		t.Fatal("unknown run should fail assignment")
	}

	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1))
	if _, err := WaitAssignment(context.Background(), sock, "sandbox", "sr-test"); err == nil || err.Error() != "not authorized" {
		t.Fatalf("wrong pid assignment error = %v, want not authorized", err)
	}
	if err := PostBuildResult(sock, "br-test", "x", BuildResult{}); err == nil || err.Error() != "not authorized" {
		t.Fatalf("wrong pid build-result error = %v, want not authorized", err)
	}
	if err := PostBuildPhase(sock, "br-test", "x", "a", "bp-a-x", "starting"); err == nil || err.Error() != "not authorized" {
		t.Fatalf("wrong pid build-phase error = %v, want not authorized", err)
	}
}

// TestAdminPlane: with admin_pidfile unset, the admin plane is reachable (socket
// perms gate) and add/check/list/remove round-trip.
func TestAdminPlane(t *testing.T) {
	adm := &stubAdmin{pairs: map[string]AdminKeyInfo{}}
	_, client := startTestServer(t, Deps{Admin: adm})

	req := AdminKeyRequest{ManifestKey: "k1", APISecret: "a1"}
	if r := adminPost(t, client, AdminKeyRequest{Op: "add", ManifestKey: req.ManifestKey, APISecret: req.APISecret, Label: "L"}); r.Status != "added" || r.APISecretFingerprint != "api-fp-a1" || r.ManifestKeyFingerprint != "manifest-fp-k1" {
		t.Fatalf("add: %+v", r)
	}
	if r := adminPost(t, client, AdminKeyRequest{Op: "add", ManifestKey: req.ManifestKey, APISecret: req.APISecret}); r.Status != "refreshed" {
		t.Fatalf("re-add: %+v", r)
	}
	if r := adminPost(t, client, AdminKeyRequest{Op: "check", ManifestKey: req.ManifestKey, APISecret: req.APISecret}); r.Status != "present" {
		t.Fatalf("check: %+v", r)
	}
	_, body := rawGet(t, client, PathAdminManifestKey)
	var infos []AdminKeyInfo
	if err := json.Unmarshal(body, &infos); err != nil || len(infos) != 1 || infos[0].APISecretFingerprint != "api-fp-a1" || infos[0].ManifestKeyFingerprint != "manifest-fp-k1" {
		t.Fatalf("list: %s (err %v)", body, err)
	}
	if r := adminPost(t, client, AdminKeyRequest{Op: "remove", ManifestKey: req.ManifestKey, APISecret: req.APISecret}); r.Status != "removed" {
		t.Fatalf("remove: %+v", r)
	}
}

func TestBuilderAdmissionAdminStatus(t *testing.T) {
	_, client := startTestServer(t, Deps{BuilderAdmissionAdmin: stubBuilderAdmissionAdmin{}})
	code, body := rawGet(t, client, PathAdminBuilderAdmission)
	if code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", code, body)
	}
	var status BuilderAdmissionStatus
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatal(err)
	}
	if status.Registration.Configured.MaxBuilds != 4 || status.Registration.UsedBuilds != 1 ||
		status.Registration.Used.CPU != 1500 || status.Registration.Available.MaxBuilds == nil ||
		*status.Registration.Available.MaxBuilds != 3 || status.WaitingBuilds != 1 {
		t.Fatalf("builder admission status = %+v", status)
	}
}

// TestAdminPidfileGate: a populated admin_pidfile that excludes our pid denies the
// admin plane (403); adding our pid then allows it.
func TestAdminPidfileGate(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "admin.pids")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1)+"\n# a comment\n")
	adm := &stubAdmin{pairs: map[string]AdminKeyInfo{}}
	_, client := startTestServer(t, Deps{Admin: adm, AdminPidfile: pf})

	if code, _ := rawPost(t, client, PathAdminManifestKey, AdminKeyRequest{Op: "add", ManifestKey: "k1"}); code != http.StatusForbidden {
		t.Fatalf("excluded pid should be 403, got %d", code)
	}
	mustWrite(t, pf, strconv.Itoa(os.Getpid())+"\n")
	if r := adminPost(t, client, AdminKeyRequest{Op: "add", ManifestKey: "k1"}); r.Status != "added" {
		t.Fatalf("allowlisted pid should add: %+v", r)
	}
}

// TestAPIPlaneFallback: any non-/internal path falls through to the injected api handler.
func TestAPIPlaneFallback(t *testing.T) {
	apiH := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("api:" + r.URL.Path))
	})
	_, client := startTestServer(t, Deps{API: apiH})
	resp, err := client.Get("http://localhost/sandboxes")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if string(b) != "api:/sandboxes" {
		t.Fatalf("fallback: %q", b)
	}
}

func rawGet(t *testing.T, client *http.Client, path string) (int, []byte) {
	t.Helper()
	resp, err := client.Get("http://localhost" + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, rb
}

func startTestServerAt(t *testing.T, ctx context.Context, sock string, deps Deps) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- New(sock, deps, discardLogger()).Serve(ctx) }()
	for range 400 {
		if _, err := os.Stat(sock); err == nil {
			return done
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("server did not bind")
	return done
}

func waitRunSessionRegistrations(t *testing.T, registry *stubRunSessionRegistry, n int) []RunSessionRequest {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		registry.mu.Lock()
		registered := append([]RunSessionRequest(nil), registry.registered...)
		registry.mu.Unlock()
		if len(registered) >= n {
			return registered
		}
		time.Sleep(5 * time.Millisecond)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	t.Fatalf("session registrations = %+v, want at least %d", registry.registered, n)
	return nil
}

func TestRunSessionKeeperReconnectsAfterServerRestart(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "ctl.sock")
	pf := filepath.Join(dir, "run.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	registry := &stubRunSessionRegistry{closeCh: make(chan bool, 4)}
	deps := Deps{Provider: stubProvider{pidFile: pf, sessions: registry}}

	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := startTestServerAt(t, ctx1, sock, deps)
	keeper, err := OpenRunSessionKeeper(context.Background(), sock, "sandbox", "sr-test")
	if err != nil {
		t.Fatalf("OpenRunSessionKeeper: %v", err)
	}
	defer keeper.Close()
	registered := waitRunSessionRegistrations(t, registry, 1)
	if registered[0].Kind != "sandbox" || registered[0].RunID != "sr-test" {
		t.Fatalf("first registration = %+v", registered[0])
	}

	cancel1()
	select {
	case shutdown := <-registry.closeCh:
		if !shutdown {
			t.Fatal("first server shutdown was reported as ordinary disconnect")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first server did not close session on shutdown")
	}
	select {
	case err := <-done1:
		if err != nil {
			t.Fatalf("first Serve returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first server did not stop")
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done2 := startTestServerAt(t, ctx2, sock, deps)
	registered = waitRunSessionRegistrations(t, registry, 2)
	if registered[1].Kind != "sandbox" || registered[1].RunID != "sr-test" {
		t.Fatalf("reconnect registration = %+v", registered[1])
	}
	if err := keeper.Close(); err != nil {
		t.Fatalf("Close keeper: %v", err)
	}
	select {
	case shutdown := <-registry.closeCh:
		if shutdown {
			t.Fatal("keeper close was reported as server shutdown")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("keeper close did not close second server session")
	}
	cancel2()
	select {
	case err := <-done2:
		if err != nil {
			t.Fatalf("second Serve returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second server did not stop")
	}
}
