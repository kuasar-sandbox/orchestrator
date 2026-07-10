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

// --- stubs for the three planes ---

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

type stubAdmin struct{ keys map[string]string }

func (a *stubAdmin) AddManifestKey(_ context.Context, key, label string, _ int64, _ string) (bool, string, error) {
	if _, ok := a.keys[key]; ok {
		return false, "fp-" + key, nil
	}
	a.keys[key] = label
	return true, "fp-" + key, nil
}

func (a *stubAdmin) RemoveManifestKey(_ context.Context, key string) (bool, string, error) {
	if _, ok := a.keys[key]; ok {
		delete(a.keys, key)
		return true, "fp-" + key, nil
	}
	return false, "fp-" + key, nil
}

func (a *stubAdmin) HasManifestKey(_ context.Context, key string) (bool, string, error) {
	_, ok := a.keys[key]
	return ok, "fp-" + key, nil
}

func (a *stubAdmin) ListManifestKeys(_ context.Context) ([]AdminKeyInfo, error) {
	out := make([]AdminKeyInfo, 0, len(a.keys))
	for k, label := range a.keys {
		out = append(out, AdminKeyInfo{Fingerprint: "fp-" + k, Label: label})
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

// TestAdminPlane: with admin_pidfile unset, the admin plane is reachable (socket
// perms gate) and add/check/list/remove round-trip.
func TestAdminPlane(t *testing.T) {
	adm := &stubAdmin{keys: map[string]string{}}
	_, client := startTestServer(t, Deps{Admin: adm})

	if r := adminPost(t, client, AdminKeyRequest{Op: "add", Key: "k1", Label: "L"}); r.Status != "added" || r.Fingerprint != "fp-k1" {
		t.Fatalf("add: %+v", r)
	}
	if r := adminPost(t, client, AdminKeyRequest{Op: "add", Key: "k1"}); r.Status != "refreshed" {
		t.Fatalf("re-add: %+v", r)
	}
	if r := adminPost(t, client, AdminKeyRequest{Op: "check", Key: "k1"}); r.Status != "present" {
		t.Fatalf("check: %+v", r)
	}
	_, body := rawGet(t, client, PathAdminManifestKey)
	var infos []AdminKeyInfo
	if err := json.Unmarshal(body, &infos); err != nil || len(infos) != 1 || infos[0].Fingerprint != "fp-k1" {
		t.Fatalf("list: %s (err %v)", body, err)
	}
	if r := adminPost(t, client, AdminKeyRequest{Op: "remove", Key: "k1"}); r.Status != "removed" {
		t.Fatalf("remove: %+v", r)
	}
}

// TestAdminPidfileGate: a populated admin_pidfile that excludes our pid denies the
// admin plane (403); adding our pid then allows it.
func TestAdminPidfileGate(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "admin.pids")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1)+"\n# a comment\n")
	adm := &stubAdmin{keys: map[string]string{}}
	_, client := startTestServer(t, Deps{Admin: adm, AdminPidfile: pf})

	if code, _ := rawPost(t, client, PathAdminManifestKey, AdminKeyRequest{Op: "add", Key: "k1"}); code != http.StatusForbidden {
		t.Fatalf("excluded pid should be 403, got %d", code)
	}
	mustWrite(t, pf, strconv.Itoa(os.Getpid())+"\n")
	if r := adminPost(t, client, AdminKeyRequest{Op: "add", Key: "k1"}); r.Status != "added" {
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
