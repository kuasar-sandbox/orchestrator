package configsock

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// --- plane stubs ---

type stubProvider struct{ pidFile string }

func (s stubProvider) LaunchSpecFor(_ context.Context, id string) (*LaunchSpec, string, bool, error) {
	if id != "sandbox:x" {
		return nil, "", false, nil
	}
	return &LaunchSpec{Exec: "/bin/true", Args: []string{"a"}}, s.pidFile, true, nil
}

func (s stubProvider) BuildSpecFor(_ context.Context, id string) (*BuildSpec, string, bool, error) {
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
	if kind == "sandbox" && runID == "sr-test" {
		return "x", true, nil
	}
	if kind == "build" && runID == "br-test" {
		return "x", true, nil
	}
	return "", false, nil
}

func (s stubProvider) PostBuildResult(_ context.Context, runID, buildID string, _ BuildResult) error {
	if runID == "br-test" && buildID == "x" {
		return nil
	}
	return os.ErrNotExist
}

type stubAdmin struct{ pairs map[string]AdminKeyInfo }

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

	spec, err := FetchLaunchSpec(sock, "sandbox:x")
	if err != nil || spec.Exec != "/bin/true" {
		t.Fatalf("happy path: spec=%+v err=%v", spec, err)
	}
	if _, err := FetchLaunchSpec(sock, "nope"); err == nil {
		t.Fatal("unknown id should error")
	}
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1)) // pidfile no longer matches peer
	if _, err := FetchLaunchSpec(sock, "sandbox:x"); err == nil || err.Error() != "not authorized" {
		t.Fatalf("wrong pid should be not authorized, got %v", err)
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
	if err := PostBuildResult(sock, "br-test", "x", BuildResult{ImageKey: "image"}); err != nil {
		t.Fatalf("PostBuildResult: %v", err)
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
