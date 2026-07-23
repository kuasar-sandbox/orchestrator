package mmds

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeSource struct {
	fip   map[string]string    // floatingip -> sid
	info  map[string][2]string // sid -> {tid, token}
	runID map[string]string    // sid -> current run id; mutate in place to simulate resume
}

func (f fakeSource) ByFloatingIP(ip string) (string, bool) { sid, ok := f.fip[ip]; return sid, ok }
func (f fakeSource) SandboxInfo(sid string) (string, string, bool) {
	v, ok := f.info[sid]
	return v[0], v[1], ok
}

// MmdsSecret returns a deterministic per-sandbox secret for known sandboxes (a
// stand-in for keys.MmdsSecret), and ok=false for unknown ids so a forged token's
// sid fails to verify.
func (f fakeSource) MmdsSecret(sid string) ([]byte, bool) {
	if _, ok := f.info[sid]; !ok {
		return nil, false
	}
	return []byte("secret-for-" + sid), true
}

// CurrentRunID returns the sid's current run id from the (mutable, shared-by-
// reference) runID map — tests simulate a pause/resume by overwriting the
// map entry mid-test and checking that a token minted before the change no
// longer verifies.
func (f fakeSource) CurrentRunID(sid string) (string, bool) {
	v, ok := f.runID[sid]
	return v, ok
}

func testSourceWithRunID() fakeSource {
	return fakeSource{
		fip:   map[string]string{"100.100.96.5": "sbx-1"},
		info:  map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		runID: map[string]string{"sbx-1": "sr-run-1"},
	}
}

func putTokenReq(sourceAddr, ttlSeconds string) *http.Request {
	req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = sourceAddr
	if ttlSeconds != "" {
		req.Header.Set(TTLHeader, ttlSeconds)
	}
	return req
}

func TestPutGetFlow(t *testing.T) {
	src := testSourceWithRunID()
	h := New(src, nil, 50*time.Millisecond, nil, nil).Handler()

	// PUT from the sandbox's floating IP, with a valid TTL header -> a session token.
	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:34567", "60"))
	if w.Code != 200 || w.Body.Len() == 0 {
		t.Fatalf("PUT: code=%d body=%q", w.Code, w.Body.String())
	}
	if got := w.Header().Get(TTLHeader); got != "60" {
		t.Errorf("PUT response %s = %q, want 60 (echoed)", TTLHeader, got)
	}
	token := w.Body.String()

	// GET with the token from the SAME source IP -> the sandbox metadata.
	req := httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:34567"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("GET: code=%d", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{`"instanceID":"sbx-1"`, `"envID":"tmpl-1"`, `"accessTokenHash":"` + HashToken("tok-abc") + `"`} {
		if !strings.Contains(body, want) {
			t.Errorf("GET body missing %q: %s", want, body)
		}
	}

	// A forged/tampered token -> 401 (unforgeable without the per-sandbox secret).
	req = httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", "sbx-evil.deadbeef")
	req.RemoteAddr = "100.100.96.5:34567"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("GET forged token: code=%d (want 401)", w.Code)
	}

	// PUT from an unregistered floating IP -> 503 after the park (envd's poll retries).
	w = httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.9:1", "60"))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("PUT unknown ip: code=%d (want 503)", w.Code)
	}
}

func TestPutTokenRequiresTTLHeader(t *testing.T) {
	src := testSourceWithRunID()
	h := New(src, nil, 50*time.Millisecond, nil, nil).Handler()

	cases := map[string]string{
		"missing":                              "",
		"malformed":                            "soon",
		"zero":                                 "0",
		"negative":                             "-1",
		"too-large":                            "21601",
		"exactly-one-over-lower-bound-is-fine": "1", // sanity check: NOT a rejection case
	}
	for name, ttl := range cases {
		wantOK := name == "exactly-one-over-lower-bound-is-fine"
		t.Run(name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, putTokenReq("100.100.96.5:1", ttl))
			if wantOK && w.Code != http.StatusOK {
				t.Fatalf("PUT ttl=%q: code=%d, want 200", ttl, w.Code)
			}
			if !wantOK && w.Code != http.StatusBadRequest {
				t.Fatalf("PUT ttl=%q: code=%d, want 400", ttl, w.Code)
			}
		})
	}

	// Duplicate header (two values for the same key) -> 400.
	req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = "100.100.96.5:1"
	req.Header.Add(TTLHeader, "60")
	req.Header.Add(TTLHeader, "120")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("PUT duplicate ttl header: code=%d, want 400", w.Code)
	}

	// Upper bound (21600) is accepted.
	w = httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "21600"))
	if w.Code != http.StatusOK {
		t.Fatalf("PUT ttl=21600: code=%d, want 200", w.Code)
	}
}

func TestGetMetaRejectsCrossSourceReplay(t *testing.T) {
	src := testSourceWithRunID()
	h := New(src, nil, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "60"))
	token := w.Body.String()

	// GET from a DIFFERENT source IP than the PUT -> 401 (the token is bound
	// to the source IP it was minted for, re-checked against the live request).
	req := httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "9.9.9.9:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("GET cross-source: code=%d, want 401", w.Code)
	}
}

func TestGetMetaRejectsTokenAfterResumeChangesRunID(t *testing.T) {
	src := testSourceWithRunID()
	h := New(src, nil, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "60"))
	token := w.Body.String()

	// Sanity: the token verifies before any resume.
	req := httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET before resume: code=%d, want 200", w.Code)
	}

	// Simulate pause/resume: the sandbox's current incarnation changes.
	src.runID["sbx-1"] = "sr-run-2"

	req = httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("GET after resume: code=%d, want 401 (stale incarnation)", w.Code)
	}
}

func TestGetMetaRejectsExpiredToken(t *testing.T) {
	src := testSourceWithRunID()
	h := New(src, nil, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "1")) // 1-second TTL
	token := w.Body.String()

	time.Sleep(1100 * time.Millisecond)

	req := httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("GET expired token: code=%d, want 401", w.Code)
	}
}

func TestGetMetaAcceptsTokenReuseWithinTTLFromSameSource(t *testing.T) {
	src := testSourceWithRunID()
	h := New(src, nil, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "60"))
	token := w.Body.String()

	// The doc is explicit that tokens are reusable, not single-use: two GETs
	// with the same token from the same source within TTL both succeed.
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "http://169.254.169.254/", nil)
		req.Header.Set("X-metadata-token", token)
		req.RemoteAddr = "100.100.96.5:1"
		w = httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("GET reuse #%d: code=%d, want 200", i, w.Code)
		}
	}
}

// fakeAuthority is a minimal EndpointAuthority stub for dispatch tests,
// mirroring fakeSource's style: plain maps, no concurrency concerns. The
// wait/wake semantics themselves are exercised at the real implementation
// (internal/mmdsauth) rather than re-stubbed here.
type fakeAuthority struct {
	byPath      map[string][2]string // sid+"\x00"+path -> {name, backendType}
	values      map[string]fakeStoreValue
	relayValues map[string]fakeRelayResult

	// lookupErr/serveStoreErr/serveRelayErr, when set, make the
	// corresponding method return this error unconditionally instead of
	// consulting the maps above — simulating an authority failure (store
	// read error, worker RPC timeout/close/backpressure) distinct from a
	// legitimate not-found/not-present/not-ok result.
	lookupErr     error
	serveStoreErr error
	serveRelayErr error
}

type fakeStoreValue struct {
	value       []byte
	contentType string
	revision    int64
	present     bool
}

type fakeRelayResult struct {
	status      int
	contentType string
	body        []byte
	ok          bool
}

func (f *fakeAuthority) Lookup(_ context.Context, sid, path string) (string, string, bool, error) {
	if f.lookupErr != nil {
		return "", "", false, f.lookupErr
	}
	v, ok := f.byPath[sid+"\x00"+path]
	if !ok {
		return "", "", false, nil
	}
	return v[0], v[1], true, nil
}

func (f *fakeAuthority) ServeStore(_ context.Context, sid, name string) ([]byte, string, int64, bool, error) {
	if f.serveStoreErr != nil {
		return nil, "", 0, false, f.serveStoreErr
	}
	v := f.values[sid+"\x00"+name]
	return v.value, v.contentType, v.revision, v.present, nil
}

func (f *fakeAuthority) ServeRelay(_ context.Context, sid, name string) (int, string, []byte, bool, error) {
	if f.serveRelayErr != nil {
		return 0, "", nil, false, f.serveRelayErr
	}
	v := f.relayValues[sid+"\x00"+name]
	return v.status, v.contentType, v.body, v.ok, nil
}

func TestGetMetaDispatchesDeclaredStoreEndpoint(t *testing.T) {
	src := testSourceWithRunID()
	auth := &fakeAuthority{
		byPath: map[string][2]string{"sbx-1\x00/latest/user-data": {"user-data", BackendStore}},
		values: map[string]fakeStoreValue{
			"sbx-1\x00user-data": {value: []byte("hello"), contentType: "text/plain", revision: 3, present: true},
		},
	}
	h := New(src, auth, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "60"))
	token := w.Body.String()

	req := httptest.NewRequest("GET", "http://169.254.169.254/latest/user-data", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET declared endpoint: code=%d body=%q", w.Code, w.Body.String())
	}
	if w.Body.String() != "hello" {
		t.Fatalf("GET declared endpoint body = %q, want %q", w.Body.String(), "hello")
	}
	if got := w.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("Content-Type = %q, want text/plain", got)
	}
	if got := w.Header().Get(MMDSRevisionHeader); got != "3" {
		t.Fatalf("%s = %q, want 3", MMDSRevisionHeader, got)
	}
	if got := w.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	if got := w.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
	}
}

func TestGetMetaFallsThroughWhenNoEndpointMatches(t *testing.T) {
	src := testSourceWithRunID()
	auth := &fakeAuthority{byPath: map[string][2]string{}}
	h := New(src, auth, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "60"))
	token := w.Body.String()

	req := httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"instanceID":"sbx-1"`) {
		t.Fatalf("fallback GET: code=%d body=%q, want the built-in envd response", w.Code, w.Body.String())
	}
}

// TestGetMetaLookupFailureReturns503 covers a worker RPC
// close/timeout/backpressure (and its internal-mode analogue, a store read
// failure), which must return 503/504 as classified: an EndpointAuthority
// failure during Lookup must never be treated the same as "no such
// endpoint" — the guest must get 503, not the built-in envd fallback.
func TestGetMetaLookupFailureReturns503(t *testing.T) {
	src := testSourceWithRunID()
	auth := &fakeAuthority{lookupErr: errors.New("store unavailable")}
	h := New(src, auth, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "60"))
	token := w.Body.String()

	req := httptest.NewRequest("GET", "http://169.254.169.254/latest/user-data", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET after a Lookup failure: code=%d, want 503 (never the built-in fallback)", w.Code)
	}
}

// TestGetMetaLookupTimeoutReturns504 is TestGetMetaLookupFailureReturns503's
// timeout variant: a context.DeadlineExceeded-classified authority failure
// (worker RPC deadline, relay-style timeout) must map to 504, not 503.
func TestGetMetaLookupTimeoutReturns504(t *testing.T) {
	src := testSourceWithRunID()
	auth := &fakeAuthority{lookupErr: context.DeadlineExceeded}
	h := New(src, auth, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "60"))
	token := w.Body.String()

	req := httptest.NewRequest("GET", "http://169.254.169.254/latest/user-data", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("GET after a Lookup timeout: code=%d, want 504", w.Code)
	}
}

// TestGetMetaServeStoreFailureReturns503 is TestGetMetaLookupFailureReturns503
// for the second dispatch hop: Lookup succeeds (the endpoint exists) but
// ServeStore itself fails.
func TestGetMetaServeStoreFailureReturns503(t *testing.T) {
	src := testSourceWithRunID()
	auth := &fakeAuthority{
		byPath:        map[string][2]string{"sbx-1\x00/latest/user-data": {"user-data", BackendStore}},
		serveStoreErr: errors.New("store unavailable"),
	}
	h := New(src, auth, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "60"))
	token := w.Body.String()

	req := httptest.NewRequest("GET", "http://169.254.169.254/latest/user-data", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET after a ServeStore failure: code=%d, want 503", w.Code)
	}
}

// TestGetMetaServeRelayFailureReturns503 is the relay-backend analogue of
// TestGetMetaServeStoreFailureReturns503.
func TestGetMetaServeRelayFailureReturns503(t *testing.T) {
	src := testSourceWithRunID()
	auth := &fakeAuthority{
		byPath:        map[string][2]string{"sbx-1\x00/latest/creds": {"creds", BackendRelay}},
		serveRelayErr: errors.New("relay unavailable"),
	}
	h := New(src, auth, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "60"))
	token := w.Body.String()

	req := httptest.NewRequest("GET", "http://169.254.169.254/latest/creds", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET after a ServeRelay failure: code=%d, want 503", w.Code)
	}
}

func TestGetMetaStoreEndpointNeverConfiguredReturns404(t *testing.T) {
	src := testSourceWithRunID()
	auth := &fakeAuthority{
		byPath: map[string][2]string{"sbx-1\x00/latest/user-data": {"user-data", BackendStore}},
		values: map[string]fakeStoreValue{"sbx-1\x00user-data": {present: false}},
	}
	h := New(src, auth, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "60"))
	token := w.Body.String()

	req := httptest.NewRequest("GET", "http://169.254.169.254/latest/user-data", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("GET never-configured endpoint: code=%d, want 404", w.Code)
	}
}

func TestGetMetaRelayEndpointUnavailable(t *testing.T) {
	src := testSourceWithRunID()
	auth := &fakeAuthority{
		byPath:      map[string][2]string{"sbx-1\x00/latest/meta-data/credentials": {"credentials", BackendRelay}},
		relayValues: map[string]fakeRelayResult{"sbx-1\x00credentials": {status: http.StatusServiceUnavailable, ok: true}},
	}
	h := New(src, auth, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "60"))
	token := w.Body.String()

	req := httptest.NewRequest("GET", "http://169.254.169.254/latest/meta-data/credentials", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET declared relay endpoint (relay unavailable): code=%d, want 503", w.Code)
	}
}

func TestGetMetaRelayEndpointDispatchesSuccessfulFetch(t *testing.T) {
	src := testSourceWithRunID()
	auth := &fakeAuthority{
		byPath: map[string][2]string{"sbx-1\x00/latest/meta-data/credentials": {"credentials", BackendRelay}},
		relayValues: map[string]fakeRelayResult{
			"sbx-1\x00credentials": {status: http.StatusOK, contentType: "application/json", body: []byte(`{"token":"upstream-value"}`), ok: true},
		},
	}
	h := New(src, auth, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "60"))
	token := w.Body.String()

	req := httptest.NewRequest("GET", "http://169.254.169.254/latest/meta-data/credentials", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET declared relay endpoint: code=%d body=%q", w.Code, w.Body.String())
	}
	if w.Body.String() != `{"token":"upstream-value"}` {
		t.Fatalf("GET declared relay endpoint body = %q", w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
}

func TestGetMetaRelayEndpointNeverConfiguredReturns404(t *testing.T) {
	src := testSourceWithRunID()
	auth := &fakeAuthority{
		byPath:      map[string][2]string{"sbx-1\x00/latest/meta-data/credentials": {"credentials", BackendRelay}},
		relayValues: map[string]fakeRelayResult{"sbx-1\x00credentials": {ok: false}},
	}
	h := New(src, auth, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, putTokenReq("100.100.96.5:1", "60"))
	token := w.Body.String()

	req := httptest.NewRequest("GET", "http://169.254.169.254/latest/meta-data/credentials", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "100.100.96.5:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("GET never-configured relay endpoint: code=%d, want 404", w.Code)
	}
}

func TestGuardRejectsQueryString(t *testing.T) {
	src := testSourceWithRunID()
	h := New(src, nil, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "http://169.254.169.254/?x=1", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("GET with a query string: code=%d, want 400", w.Code)
	}
}

func TestGuardRejectsPercentEncodedPath(t *testing.T) {
	src := testSourceWithRunID()
	h := New(src, nil, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "http://169.254.169.254/latest%2Fuser-data", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("GET with a percent-escaped path: code=%d, want 400", w.Code)
	}
}

func TestGuardRejectsNonCanonicalPath(t *testing.T) {
	src := testSourceWithRunID()
	h := New(src, nil, 50*time.Millisecond, nil, nil).Handler()

	for _, target := range []string{
		"http://169.254.169.254/../latest",
		"http://169.254.169.254//latest",
		"http://169.254.169.254/latest/",
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", target, nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("GET %s: code=%d, want 400 (and no redirect)", target, w.Code)
		}
		if loc := w.Header().Get("Location"); loc != "" {
			t.Fatalf("GET %s: got a redirect Location=%q, want none (no automatic redirect)", target, loc)
		}
	}
}

func TestGuardRejectsRequestBody(t *testing.T) {
	src := testSourceWithRunID()
	h := New(src, nil, 50*time.Millisecond, nil, nil).Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "http://169.254.169.254/", strings.NewReader("x")))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("GET with a body: code=%d, want 400", w.Code)
	}

	w = httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", strings.NewReader("x"))
	req.RemoteAddr = "100.100.96.5:1"
	req.Header.Set(TTLHeader, "60")
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("PUT token with a body: code=%d, want 400", w.Code)
	}
}

// TestGuardRejectsStalledChunkedBodyWithoutBlocking is the regression case
// for the bug this guard used to have: a body of unknown length (no
// Content-Length, e.g. a real chunked request) whose sender never delivers
// any bytes must be rejected from headers alone. An io.Pipe with no writer
// stands in for a guest that sends body-bearing headers and then stalls —
// if the guard ever goes back to reading r.Body to decide this, this test
// hangs instead of failing fast.
func TestGuardRejectsStalledChunkedBodyWithoutBlocking(t *testing.T) {
	src := testSourceWithRunID()
	h := New(src, nil, 50*time.Millisecond, nil, nil).Handler()

	pr, pw := io.Pipe()
	t.Cleanup(func() { _ = pw.Close() })

	done := make(chan int, 1)
	go func() {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "http://169.254.169.254/", pr))
		done <- w.Code
	}()

	select {
	case code := <-done:
		if code != http.StatusBadRequest {
			t.Fatalf("GET with a stalled unknown-length body: code=%d, want 400", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("guard blocked on a stalled body instead of rejecting it from headers")
	}
}

func TestHashToken(t *testing.T) {
	h := HashToken("hello")
	if len(h) != 128 { // keys.HashAccessTokenBytes = hex(sha512) = 64 bytes = 128 hex chars
		t.Fatalf("hash not 128-char hex sha512: %q (len %d)", h, len(h))
	}
	if h != HashToken("hello") {
		t.Fatal("hash not deterministic")
	}
	if HashToken("a") == HashToken("b") {
		t.Fatal("distinct tokens hashed equal")
	}
}
