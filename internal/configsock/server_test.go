package configsock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// --- plane stubs ---

type stubProvider struct {
	pidFile       string
	assignmentErr error
	buildSpecErr  error
	resultErr     error
	phaseErr      error
	sandboxAuth   SandboxTaskAuth
	bootstrapHits *atomic.Int32
	prepareHits   *atomic.Int32
	prepareErr    error
}

func (s stubProvider) SandboxTaskAuth(_ context.Context, sandboxID, runID string) (SandboxTaskAuth, bool, error) {
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

func (s stubProvider) CompleteSandboxPrepare(_ context.Context, sandboxID, runID string, _ SnapshotPrepareSummary) (*LaunchSpec, error) {
	if s.prepareHits != nil {
		s.prepareHits.Add(1)
	}
	if s.prepareErr != nil {
		return nil, s.prepareErr
	}
	if sandboxID != "x" || runID != "sr-test" {
		return nil, RejectSnapshotPrepare(os.ErrNotExist)
	}
	return &LaunchSpec{Exec: "/bin/true", Args: []string{"run"}}, nil
}

func (s stubProvider) BuildSpecFor(_ context.Context, id string) (*BuildSpec, string, bool, error) {
	if s.buildSpecErr != nil {
		return nil, s.pidFile, false, s.buildSpecErr
	}
	if id != "build:x" {
		return nil, "", false, nil
	}
	return &BuildSpec{BuildID: "x", Workdir: "/tmp"}, s.pidFile, true, nil
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
	sock := filepath.Join(t.TempDir(), "ctl.sock")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = New(sock, deps, discardLogger()).Serve(ctx) }()
	for range 400 { // wait for ListenUnix + chmod to publish the socket
		if _, err := os.Stat(sock); err == nil {
			return sock, HTTPClient(sock)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("server did not bind")
	return "", nil
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

func TestSandboxPrepareAuthenticatesBeforeCompletionProvider(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "task.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1))
	var completionCalls atomic.Int32
	sock, _ := startTestServer(t, Deps{Provider: stubProvider{pidFile: pf, prepareHits: &completionCalls}})
	summary := SnapshotPrepareSummary{SchemaVersion: 1, ResolutionDigest: strings.Repeat("0", 64), RequiredRefCount: 1}
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
		pidFile: pf, prepareErr: RejectSnapshotPrepare(errors.New("conflicting replay")),
	}})
	_, err := CompleteSandboxPrepare(context.Background(), sock, "x", "sr-test", SnapshotPrepareSummary{})
	if err == nil || IsRetryableError(err) || err.Error() != "conflicting replay" {
		t.Fatalf("conflict error = %v, retryable=%t", err, IsRetryableError(err))
	}
}

func TestBuildClientClassifiesOnlyTransportInterruptionsAsRetryable(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.sock")
	if _, err := WaitAssignment(context.Background(), missing, "build", "br-test"); !IsTransportError(err) {
		t.Fatalf("assignment transport error = %v, retryable=%t", err, IsTransportError(err))
	}
	if _, err := FetchBuildSpecContext(context.Background(), missing, "build:test"); !IsTransportError(err) {
		t.Fatalf("build-spec transport error = %v, retryable=%t", err, IsTransportError(err))
	}

	pf := filepath.Join(t.TempDir(), "id.pid")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()))
	sock, _ := startTestServer(t, Deps{Provider: stubProvider{pidFile: pf}})
	if _, err := FetchBuildSpecContext(context.Background(), sock, "unknown"); err == nil || IsTransportError(err) {
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
		"build spec": func(sock string) error {
			_, err := FetchBuildSpecContext(context.Background(), sock, "build:x")
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
			case "build spec":
				provider.buildSpecErr = transient
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
