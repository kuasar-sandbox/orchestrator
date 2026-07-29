package configsock

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
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

// stubMMDSSecretsAdmin fakes internal/orch's PutMMDSSecret/DeleteMMDSSecret
// contract: sandbox "missing" is unknown (404), name "unspecified" fails
// specification validation (400), everything else round-trips through an
// in-memory per-(sid,name) revision counter.
type stubMMDSSecretsAdmin struct {
	mu   sync.Mutex
	rev  map[string]int64
	last struct {
		sid, name, contentType string
		body                   []byte
		expiresUnix            int64
	}
}

func (a *stubMMDSSecretsAdmin) PutMMDSSecret(_ context.Context, sid, name string, body []byte, contentType string, expiresUnix int64) (int64, error) {
	if sid == "missing" {
		return 0, fmt.Errorf("mmds secret: sandbox %s: %w", sid, api.ErrNotFound)
	}
	if name == "unspecified" {
		return 0, fmt.Errorf("mmds secret: %q is not specified: %w", name, api.ErrBadRequest)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.rev == nil {
		a.rev = map[string]int64{}
	}
	key := sid + "\x00" + name
	a.rev[key]++
	a.last.sid, a.last.name, a.last.contentType, a.last.body, a.last.expiresUnix = sid, name, contentType, body, expiresUnix
	return a.rev[key], nil
}

func (a *stubMMDSSecretsAdmin) DeleteMMDSSecret(_ context.Context, sid, name string) (int64, error) {
	if sid == "missing" {
		return 0, fmt.Errorf("mmds secret: sandbox %s: %w", sid, api.ErrNotFound)
	}
	if name == "unspecified" {
		return 0, fmt.Errorf("mmds secret: %q is not specified: %w", name, api.ErrBadRequest)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	key := sid + "\x00" + name
	rev := a.rev[key] // 0 if never configured -- matches the real idempotent-delete contract
	delete(a.rev, key)
	return rev, nil
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
	if err := PostBuildResult(sock, "br-test", "x", BuildResult{ImageRef: "image"}); err != nil {
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

func rawDo(t *testing.T, client *http.Client, method, path string, body []byte, headers map[string]string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, "http://localhost"+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, rb
}

// rawDoHeader is rawDo plus the response header, for callers that need to
// read X-Kuasar-MMDS-Revision off a 204 (which carries no body).
func rawDoHeader(t *testing.T, client *http.Client, method, path string, body []byte, headers map[string]string) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, "http://localhost"+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, rb
}

// --- MMDS secrets admin plane ---

func TestAdminMMDSSecretPutGetDeleteRoundTrip(t *testing.T) {
	adm := &stubMMDSSecretsAdmin{}
	_, client := startTestServer(t, Deps{MMDSSecretsAdmin: adm})

	code, header, body := rawDoHeader(t, client, http.MethodPut, "/internal/admin/sandboxes/sbx-1/mmds/secrets/key1", []byte("hello"),
		map[string]string{"Content-Type": "text/plain"})
	if code != http.StatusNoContent {
		t.Fatalf("put status = %d, body = %s", code, body)
	}
	if len(body) != 0 {
		t.Fatalf("204 must carry no body, got %q", body)
	}
	if header.Get("X-Kuasar-MMDS-Revision") != "1" {
		t.Fatalf("put X-Kuasar-MMDS-Revision = %q, want 1", header.Get("X-Kuasar-MMDS-Revision"))
	}
	if adm.last.sid != "sbx-1" || adm.last.name != "key1" || string(adm.last.body) != "hello" || adm.last.contentType != "text/plain" {
		t.Fatalf("stub did not receive expected args: %+v", adm.last)
	}

	code, header, body = rawDoHeader(t, client, http.MethodDelete, "/internal/admin/sandboxes/sbx-1/mmds/secrets/key1", nil, nil)
	if code != http.StatusNoContent {
		t.Fatalf("delete status = %d, body = %s", code, body)
	}
	if len(body) != 0 {
		t.Fatalf("204 must carry no body, got %q", body)
	}
	if header.Get("X-Kuasar-MMDS-Revision") != "1" {
		t.Fatalf("delete X-Kuasar-MMDS-Revision = %q, want 1", header.Get("X-Kuasar-MMDS-Revision"))
	}
}

// TestAdminMMDSSecretPutEnforcesBodyLimit proves the handler's
// maxMMDSSecretBodyBytes memory-safety backstop actually bounds the request
// body reader -- distinct from (and larger than) the operator-configured
// mmds.routes.secret.max_value_bytes policy limit, which internal/orch
// enforces separately and which this stub doesn't model.
func TestAdminMMDSSecretPutEnforcesBodyLimit(t *testing.T) {
	adm := &stubMMDSSecretsAdmin{}
	_, client := startTestServer(t, Deps{MMDSSecretsAdmin: adm})

	oversized := make([]byte, maxMMDSSecretBodyBytes+1)
	code, _ := rawDo(t, client, http.MethodPut, "/internal/admin/sandboxes/sbx-1/mmds/secrets/key1", oversized, nil)
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized put: status = %d, want %d", code, http.StatusRequestEntityTooLarge)
	}
	if adm.last.sid != "" {
		t.Fatalf("stub should not have been called for a rejected oversized body: %+v", adm.last)
	}
}

func TestAdminMMDSSecretPutParsesExpiresHeader(t *testing.T) {
	adm := &stubMMDSSecretsAdmin{}
	_, client := startTestServer(t, Deps{MMDSSecretsAdmin: adm})

	code, _ := rawDo(t, client, http.MethodPut, "/internal/admin/sandboxes/sbx-1/mmds/secrets/key1", []byte("v"),
		map[string]string{"X-Kuasar-MMDS-Expires-Unix": "1234567890"})
	if code != http.StatusNoContent {
		t.Fatalf("put status = %d", code)
	}
	if adm.last.expiresUnix != 1234567890 {
		t.Fatalf("expiresUnix = %d, want 1234567890", adm.last.expiresUnix)
	}

	code, body := rawDo(t, client, http.MethodPut, "/internal/admin/sandboxes/sbx-1/mmds/secrets/key1", []byte("v"),
		map[string]string{"X-Kuasar-MMDS-Expires-Unix": "not-a-number"})
	if code != http.StatusBadRequest {
		t.Fatalf("malformed expires header: status = %d, body = %s", code, body)
	}
}

// TestAdminMMDSSecretPutRejectsNegativeExpires guards against a regression
// where strconv.ParseInt silently accepted a negative expires_unix: since
// MMDSSecretExpired only treats ExpiresUnix>0 as bounded, a negative value
// would have been misread as "never expires" rather than rejected as
// malformed input.
func TestAdminMMDSSecretPutRejectsNegativeExpires(t *testing.T) {
	adm := &stubMMDSSecretsAdmin{}
	_, client := startTestServer(t, Deps{MMDSSecretsAdmin: adm})

	code, body := rawDo(t, client, http.MethodPut, "/internal/admin/sandboxes/sbx-1/mmds/secrets/key1", []byte("v"),
		map[string]string{"X-Kuasar-MMDS-Expires-Unix": "-1"})
	if code != http.StatusBadRequest {
		t.Fatalf("negative expires header: status = %d, body = %s", code, body)
	}
	if adm.last.sid != "" {
		t.Fatalf("stub should not have been called for a rejected negative expiry: %+v", adm.last)
	}
}

func TestAdminMMDSSecretPutRejectsMalformedContentType(t *testing.T) {
	adm := &stubMMDSSecretsAdmin{}
	_, client := startTestServer(t, Deps{MMDSSecretsAdmin: adm})

	code, body := rawDo(t, client, http.MethodPut, "/internal/admin/sandboxes/sbx-1/mmds/secrets/key1", []byte("v"),
		map[string]string{"Content-Type": "not a valid media type"})
	if code != http.StatusBadRequest {
		t.Fatalf("malformed content-type: status = %d, body = %s", code, body)
	}
	if adm.last.sid != "" {
		t.Fatalf("stub should not have been called for a rejected content-type: %+v", adm.last)
	}
}

func TestAdminMMDSSecretPutRejectsOversizeContentType(t *testing.T) {
	adm := &stubMMDSSecretsAdmin{}
	_, client := startTestServer(t, Deps{MMDSSecretsAdmin: adm})

	code, body := rawDo(t, client, http.MethodPut, "/internal/admin/sandboxes/sbx-1/mmds/secrets/key1", []byte("v"),
		map[string]string{"Content-Type": "text/plain; x=" + strings.Repeat("a", maxMMDSSecretContentTypeBytes)})
	if code != http.StatusBadRequest {
		t.Fatalf("oversize content-type: status = %d, body = %s", code, body)
	}
	if adm.last.sid != "" {
		t.Fatalf("stub should not have been called for a rejected content-type: %+v", adm.last)
	}
}

func TestAdminMMDSSecretPutAcceptsValidContentType(t *testing.T) {
	adm := &stubMMDSSecretsAdmin{}
	_, client := startTestServer(t, Deps{MMDSSecretsAdmin: adm})

	code, body := rawDo(t, client, http.MethodPut, "/internal/admin/sandboxes/sbx-1/mmds/secrets/key1", []byte("v"),
		map[string]string{"Content-Type": "application/json; charset=utf-8"})
	if code != http.StatusNoContent {
		t.Fatalf("valid content-type: status = %d, body = %s", code, body)
	}
	if adm.last.contentType != "application/json; charset=utf-8" {
		t.Fatalf("contentType = %q", adm.last.contentType)
	}
}

func TestAdminMMDSSecretPutAcceptsAbsentContentType(t *testing.T) {
	adm := &stubMMDSSecretsAdmin{}
	_, client := startTestServer(t, Deps{MMDSSecretsAdmin: adm})

	code, body := rawDo(t, client, http.MethodPut, "/internal/admin/sandboxes/sbx-1/mmds/secrets/key1", []byte("v"), nil)
	if code != http.StatusNoContent {
		t.Fatalf("absent content-type: status = %d, body = %s", code, body)
	}
}

func TestAdminMMDSSecretUnknownSandboxIs404(t *testing.T) {
	adm := &stubMMDSSecretsAdmin{}
	_, client := startTestServer(t, Deps{MMDSSecretsAdmin: adm})

	code, _ := rawDo(t, client, http.MethodPut, "/internal/admin/sandboxes/missing/mmds/secrets/key1", []byte("v"), nil)
	if code != http.StatusNotFound {
		t.Fatalf("put on unknown sandbox: status = %d, want 404", code)
	}
	code, _ = rawDo(t, client, http.MethodDelete, "/internal/admin/sandboxes/missing/mmds/secrets/key1", nil, nil)
	if code != http.StatusNotFound {
		t.Fatalf("delete on unknown sandbox: status = %d, want 404", code)
	}
}

func TestAdminMMDSSecretUnspecifiedNameIs400(t *testing.T) {
	adm := &stubMMDSSecretsAdmin{}
	_, client := startTestServer(t, Deps{MMDSSecretsAdmin: adm})

	code, _ := rawDo(t, client, http.MethodPut, "/internal/admin/sandboxes/sbx-1/mmds/secrets/unspecified", []byte("v"), nil)
	if code != http.StatusBadRequest {
		t.Fatalf("put on unspecified name: status = %d, want 400", code)
	}
}

func TestAdminMMDSSecretRoutesOffWhenDepsNil(t *testing.T) {
	_, client := startTestServer(t, Deps{})
	code, _ := rawDo(t, client, http.MethodPut, "/internal/admin/sandboxes/sbx-1/mmds/secrets/key1", []byte("v"), nil)
	if code != http.StatusNotFound {
		t.Fatalf("PUT with MMDSSecretsAdmin nil: status = %d, want 404 (route not registered)", code)
	}
}

func TestAdminMMDSSecretPidfileGate(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "admin.pids")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1)+"\n")
	adm := &stubMMDSSecretsAdmin{}
	_, client := startTestServer(t, Deps{MMDSSecretsAdmin: adm, AdminPidfile: pf})

	code, _ := rawDo(t, client, http.MethodPut, "/internal/admin/sandboxes/sbx-1/mmds/secrets/key1", []byte("v"), nil)
	if code != http.StatusForbidden {
		t.Fatalf("excluded pid should be 403, got %d", code)
	}
	mustWrite(t, pf, strconv.Itoa(os.Getpid())+"\n")
	code, _ = rawDo(t, client, http.MethodPut, "/internal/admin/sandboxes/sbx-1/mmds/secrets/key1", []byte("v"), nil)
	if code != http.StatusNoContent {
		t.Fatalf("allowlisted pid should succeed, got %d", code)
	}
}
