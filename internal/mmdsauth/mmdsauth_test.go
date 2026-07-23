package mmdsauth

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrelay"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func testSandbox(id string) *types.Sandbox {
	return &types.Sandbox{
		ID:          id,
		TemplateID:  "e2b-img-" + strings.Repeat("1", 64),
		State:       types.StateRunning,
		RunDir:      "/run/sandbox/" + id,
		BaseDir:     "/var/lib/sandbox/" + id,
		ManifestKey: strings.Repeat("2", 64),
		CreatedUnix: time.Now().Unix(),
	}
}

func TestLookupResolvesExactPath(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-1")
	if err := st.PutWithMMDSEndpoints(ctx, sb, []store.MMDSEndpoint{
		{Name: "creds", Path: "/latest/meta-data/credentials", BackendType: store.MMDSBackendRelay},
	}); err != nil {
		t.Fatal(err)
	}
	a := New(st, nil, time.Second, nil)
	name, backend, found, err := a.Lookup(ctx, sb.ID, "/latest/meta-data/credentials")
	if err != nil || !found || name != "creds" || backend != store.MMDSBackendRelay {
		t.Fatalf("Lookup = name=%q backend=%q found=%t err=%v", name, backend, found, err)
	}
	if _, _, found, err := a.Lookup(ctx, sb.ID, "/latest/nope"); found || err != nil {
		t.Fatalf("Lookup found an undeclared path or errored: found=%t err=%v", found, err)
	}
}

func TestServeStoreNeverConfiguredWaitsThenTimesOut(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-timeout")
	if err := st.PutWithMMDSEndpoints(ctx, sb, []store.MMDSEndpoint{
		{Name: "a", Path: "/latest/a", BackendType: store.MMDSBackendStore},
	}); err != nil {
		t.Fatal(err)
	}
	a := New(st, nil, 50*time.Millisecond, nil)
	start := time.Now()
	_, _, _, present, err := a.ServeStore(ctx, sb.ID, "a")
	if err != nil || present {
		t.Fatalf("ServeStore returned present=%t err=%v for a never-configured endpoint", present, err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("ServeStore returned too fast (%v), want it to have waited out the timeout", elapsed)
	}
}

func TestServeStorePutDuringWaitWakesImmediately(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-wake")
	if err := st.PutWithMMDSEndpoints(ctx, sb, []store.MMDSEndpoint{
		{Name: "a", Path: "/latest/a", BackendType: store.MMDSBackendStore},
	}); err != nil {
		t.Fatal(err)
	}
	a := New(st, nil, 5*time.Second, nil) // long timeout: the test fails if it's actually waited out

	done := make(chan struct{})
	var value []byte
	var present bool
	var serveErr error
	go func() {
		value, _, _, present, serveErr = a.ServeStore(ctx, sb.ID, "a")
		close(done)
	}()

	time.Sleep(20 * time.Millisecond) // let ServeStore park before the PUT lands
	if _, err := st.SetMMDSStoreValue(ctx, sb.ID, "a", []byte("hello"), "", 0); err != nil {
		t.Fatal(err)
	}
	a.Notify(sb.ID, "a")

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeStore did not wake up after Notify")
	}
	if serveErr != nil || !present || string(value) != "hello" {
		t.Fatalf("ServeStore after wake = present=%t value=%q err=%v", present, value, serveErr)
	}
}

func TestServeStoreDeletedReturnsImmediately(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-deleted")
	if err := st.PutWithMMDSEndpoints(ctx, sb, []store.MMDSEndpoint{
		{Name: "a", Path: "/latest/a", BackendType: store.MMDSBackendStore},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SetMMDSStoreValue(ctx, sb.ID, "a", []byte("v"), "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClearMMDSStoreValue(ctx, sb.ID, "a"); err != nil {
		t.Fatal(err)
	}

	a := New(st, nil, 2*time.Second, nil)
	start := time.Now()
	_, _, _, present, err := a.ServeStore(ctx, sb.ID, "a")
	if err != nil || present {
		t.Fatalf("ServeStore returned present=%t err=%v for a deleted value", present, err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("ServeStore waited for a deleted (revision>0) value: %v", elapsed)
	}
}

func TestServeStoreExpiredReturnsImmediately(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-expired")
	if err := st.PutWithMMDSEndpoints(ctx, sb, []store.MMDSEndpoint{
		{Name: "a", Path: "/latest/a", BackendType: store.MMDSBackendStore},
	}); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Hour).Unix()
	if _, err := st.SetMMDSStoreValue(ctx, sb.ID, "a", []byte("v"), "", past); err != nil {
		t.Fatal(err)
	}

	a := New(st, nil, 2*time.Second, nil)
	start := time.Now()
	_, _, _, present, err := a.ServeStore(ctx, sb.ID, "a")
	if err != nil || present {
		t.Fatalf("ServeStore returned present=%t err=%v for an expired value", present, err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("ServeStore waited for an expired value: %v", elapsed)
	}
}

type fakeRelayFetcher struct {
	mu     sync.Mutex
	calls  []fakeRelayCall
	block  chan struct{} // if non-nil, Fetch blocks on ctx.Done() or this channel
	result mmdsrelay.Result
}

type fakeRelayCall struct {
	key, url, headerName, headerValue string
}

func (f *fakeRelayFetcher) Fetch(ctx context.Context, key, rawURL, headerName, headerValue string) mmdsrelay.Result {
	f.mu.Lock()
	f.calls = append(f.calls, fakeRelayCall{key, rawURL, headerName, headerValue})
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return mmdsrelay.Result{Status: 499} // sentinel: caller was cancelled mid-flight
		}
	}
	return f.result
}

func (f *fakeRelayFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func endpointWithRelayConfig(t *testing.T, st *store.Store, sb *types.Sandbox, name, url, headerName string) {
	t.Helper()
	cfg := `{"url":"` + url + `","auth_header_name":"` + headerName + `"}`
	if err := st.PutWithMMDSEndpoints(context.Background(), sb, []store.MMDSEndpoint{
		{Name: name, Path: "/latest/" + name, BackendType: store.MMDSBackendRelay, PublicConfigJSON: cfg},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestServeRelayReturns503WhenRelayClientNil(t *testing.T) {
	st := testStore(t)
	sb := testSandbox("sb-relay-nil")
	endpointWithRelayConfig(t, st, sb, "a", "https://example.com/", "X-Auth")

	a := New(st, nil, time.Second, nil)
	status, _, _, ok, err := a.ServeRelay(context.Background(), sb.ID, "a")
	if err != nil || !ok || status != http.StatusServiceUnavailable {
		t.Fatalf("ServeRelay with nil relay = status=%d ok=%t err=%v, want 503/true/nil", status, ok, err)
	}
}

func TestServeRelayNeverConfiguredWaitsThenNotFound(t *testing.T) {
	st := testStore(t)
	sb := testSandbox("sb-relay-wait")
	endpointWithRelayConfig(t, st, sb, "a", "https://example.com/", "X-Auth")

	fake := &fakeRelayFetcher{}
	a := New(st, fake, 50*time.Millisecond, nil)
	_, _, _, ok, err := a.ServeRelay(context.Background(), sb.ID, "a")
	if err != nil || ok {
		t.Fatalf("ServeRelay returned ok=%t err=%v for a never-configured auth", ok, err)
	}
	if fake.callCount() != 0 {
		t.Fatal("ServeRelay contacted the upstream for a never-configured auth")
	}
}

func TestServeRelayRevokedReturnsImmediately(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-relay-revoked")
	endpointWithRelayConfig(t, st, sb, "a", "https://example.com/", "X-Auth")
	if _, err := st.SetMMDSRelayAuth(ctx, sb.ID, "a", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClearMMDSRelayAuth(ctx, sb.ID, "a"); err != nil {
		t.Fatal(err)
	}

	fake := &fakeRelayFetcher{}
	a := New(st, fake, 2*time.Second, nil)
	start := time.Now()
	_, _, _, ok, err := a.ServeRelay(ctx, sb.ID, "a")
	if err != nil || ok {
		t.Fatalf("ServeRelay returned ok=%t err=%v for revoked auth", ok, err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("ServeRelay waited for revoked (revision>0) auth: %v", elapsed)
	}
	if fake.callCount() != 0 {
		t.Fatal("ServeRelay contacted the upstream for revoked auth")
	}
}

func TestServeRelayConfiguredDispatchesFetchWithCorrectArgs(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-relay-ok")
	endpointWithRelayConfig(t, st, sb, "creds", "https://identity.example.com/creds", "X-Upstream-Assertion")
	if _, err := st.SetMMDSRelayAuth(ctx, sb.ID, "creds", []byte("secret-token")); err != nil {
		t.Fatal(err)
	}

	fake := &fakeRelayFetcher{result: mmdsrelay.Result{Status: 200, ContentType: "application/json", Body: []byte(`{"ok":true}`)}}
	a := New(st, fake, 2*time.Second, nil)
	status, contentType, body, ok, err := a.ServeRelay(ctx, sb.ID, "creds")
	if err != nil || !ok || status != 200 || contentType != "application/json" || string(body) != `{"ok":true}` {
		t.Fatalf("ServeRelay = status=%d contentType=%q body=%q ok=%t err=%v", status, contentType, body, ok, err)
	}
	if len(fake.calls) != 1 {
		t.Fatalf("got %d Fetch calls, want 1", len(fake.calls))
	}
	call := fake.calls[0]
	if call.key != sb.ID+"\x00creds" || call.url != "https://identity.example.com/creds" ||
		call.headerName != "X-Upstream-Assertion" || call.headerValue != "secret-token" {
		t.Fatalf("Fetch called with %+v", call)
	}
}

func TestServeRelayCancelsInFlightFetchOnAuthNotify(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := testSandbox("sb-relay-cancel")
	endpointWithRelayConfig(t, st, sb, "a", "https://example.com/", "X-Auth")
	if _, err := st.SetMMDSRelayAuth(ctx, sb.ID, "a", []byte("old-secret")); err != nil {
		t.Fatal(err)
	}

	fake := &fakeRelayFetcher{block: make(chan struct{})} // Fetch blocks until ctx is cancelled
	a := New(st, fake, 2*time.Second, nil)

	done := make(chan mmdsrelay.Result, 1)
	go func() {
		status, _, _, _, _ := a.ServeRelay(ctx, sb.ID, "a")
		done <- mmdsrelay.Result{Status: status}
	}()

	time.Sleep(50 * time.Millisecond) // let ServeRelay reach the blocked Fetch call
	if _, err := st.SetMMDSRelayAuth(ctx, sb.ID, "a", []byte("new-secret")); err != nil {
		t.Fatal(err)
	}
	a.Notify(sb.ID, "a") // simulate what orch's admin mutation path does after a successful write

	select {
	case res := <-done:
		if res.Status != 499 {
			t.Fatalf("ServeRelay result status = %d, want 499 (fake's cancellation sentinel)", res.Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeRelay did not cancel its in-flight Fetch after Notify")
	}
}
