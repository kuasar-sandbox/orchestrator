package mmds

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeSource struct {
	fip      map[string]string               // floatingip -> sid
	info     map[string][2]string            // sid -> {tid, token}
	routes   map[string]map[string]MMDSRoute // sid -> path -> route
	routeErr error                           // if set, MMDSRoute always fails with this error
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

func (f fakeSource) MMDSRoute(sid, path string) (MMDSRoute, bool, error) {
	if f.routeErr != nil {
		return MMDSRoute{}, false, f.routeErr
	}
	byPath, ok := f.routes[sid]
	if !ok {
		return MMDSRoute{}, false, nil
	}
	route, ok := byPath[path]
	return route, ok, nil
}

func TestPutGetFlow(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()

	// PUT from the sandbox's floating IP -> a session token.
	req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = "100.100.96.5:34567"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 || w.Body.Len() == 0 {
		t.Fatalf("PUT: code=%d body=%q", w.Code, w.Body.String())
	}
	token := w.Body.String()

	// GET with the token -> the sandbox metadata; source IP is deliberately NOT the
	// floating IP, proving the (untrusted) source is not re-read — the token is authoritative.
	req = httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "9.9.9.9:1"
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
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("GET forged token: code=%d (want 401)", w.Code)
	}

	// PUT from an unregistered floating IP -> 503 after the park (envd's poll retries).
	req = httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = "100.100.96.9:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("PUT unknown ip: code=%d (want 503)", w.Code)
	}
}

func TestPutTokenRejectsRequestBody(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", strings.NewReader("unexpected"))
	req.RemoteAddr = "100.100.96.5:1"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("PUT with body: code=%d body=%q, want 400", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
}

// TestPutTokenAllowsBodylessChunkedRequest proves a bodyless request whose
// Body is a real (non-http.NoBody) reader -- what a chunked
// Transfer-Encoding request always gets, since its length is never known in
// advance -- is not rejected just because it isn't the NoBody sentinel:
// rejectRequestBody must actually peek it and observe it's empty.
// strings.NewReader("") reproduces the same shape httptest.NewRequest gives
// any non-nil body reader (a real io.ReadCloser distinct from NoBody),
// regardless of whether real chunked framing is involved.
func TestPutTokenAllowsBodylessChunkedRequest(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", strings.NewReader(""))
	if req.Body == http.NoBody {
		t.Fatal("test setup: strings.NewReader(\"\") body was normalized to http.NoBody")
	}
	req.RemoteAddr = "100.100.96.5:1"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT with empty non-NoBody body: code=%d body=%q, want 200", w.Code, w.Body.String())
	}
}

// mintedToken returns a valid session token for sid via the real PUT flow.
func mintedToken(t *testing.T, h http.Handler, floatingIP, sid string) string {
	t.Helper()
	req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = floatingIP + ":1"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("PUT: code=%d body=%q", w.Code, w.Body.String())
	}
	return w.Body.String()
}

func TestGetMetaServesSpecifiedStaticRoute(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {
				"/static-path": {Type: "static", ContentType: "application/json", Data: `{"key":"value"}`},
			},
		},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "100.100.96.5", "sbx-1")

	req := httptest.NewRequest("GET", "http://169.254.169.254/static-path", nil)
	req.Header.Set("X-metadata-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("GET specified static route: code=%d body=%q", w.Code, w.Body.String())
	}
	if got, want := w.Body.String(), `{"key":"value"}`; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
	if got, want := w.Header().Get("Content-Type"), "application/json"; got != want {
		t.Fatalf("Content-Type = %q, want %q", got, want)
	}
}

func TestGetMetaRejectsRequestBody(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "100.100.96.5", "sbx-1")
	req := httptest.NewRequest("GET", "http://169.254.169.254/", strings.NewReader("unexpected"))
	req.Header.Set("X-metadata-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("GET with body: code=%d body=%q, want 400", w.Code, w.Body.String())
	}
	if got := w.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
	}

	// Authentication remains the outer boundary: an invalid token is still
	// classified as unauthorized even when the request also carries a body.
	req = httptest.NewRequest("GET", "http://169.254.169.254/", strings.NewReader("unexpected"))
	req.Header.Set("X-metadata-token", "sbx-evil.deadbeef")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized GET with body: code=%d, want 401", w.Code)
	}
}

func TestGetMetaReturns503ForSpecifiedSecretOrServiceRoute(t *testing.T) {
	for _, routeType := range []string{"secret", "service"} {
		t.Run(routeType, func(t *testing.T) {
			src := fakeSource{
				fip:  map[string]string{"100.100.96.5": "sbx-1"},
				info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
				routes: map[string]map[string]MMDSRoute{
					"sbx-1": {"/backend-path": {Type: routeType}},
				},
			}
			h := New(src, 50*time.Millisecond, nil).Handler()
			token := mintedToken(t, h, "100.100.96.5", "sbx-1")

			req := httptest.NewRequest("GET", "http://169.254.169.254/backend-path", nil)
			req.Header.Set("X-metadata-token", token)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("GET specified %s route: code=%d (want 503)", routeType, w.Code)
			}
		})
	}
}

func TestGetMetaReturns503OnRouteResolutionError(t *testing.T) {
	src := fakeSource{
		fip:      map[string]string{"100.100.96.5": "sbx-1"},
		info:     map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routeErr: errors.New("rpc timeout"),
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "100.100.96.5", "sbx-1")

	req := httptest.NewRequest("GET", "http://169.254.169.254/some-path", nil)
	req.Header.Set("X-metadata-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET with a resolution error: code=%d (want 503, must NOT silently fall through)", w.Code)
	}
}

func TestGetMetaRootStillServesFixedInstanceInfo(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "100.100.96.5", "sbx-1")

	req := httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("GET /: code=%d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"instanceID":"sbx-1"`) {
		t.Fatalf("unexpected root body: %s", w.Body.String())
	}
}

func TestGetMetaUnspecifiedPathReturns404(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "100.100.96.5", "sbx-1")

	req := httptest.NewRequest("GET", "http://169.254.169.254/some/unspecified/path", nil)
	req.Header.Set("X-metadata-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	// An unspecified path must NOT silently alias the root instance-info
	// response (that was the pre-Phase-1 ServeMux subtree-match behavior,
	// which the design's guest HTTP contract explicitly rejects: unknown
	// route -> 404).
	if w.Code != http.StatusNotFound {
		t.Fatalf("GET unspecified path: code=%d, want 404", w.Code)
	}
	if strings.Contains(w.Body.String(), `"instanceID"`) {
		t.Fatalf("unspecified path leaked the root instance-info body: %s", w.Body.String())
	}
}

// TestHeadRequestReturns405 proves HEAD is rejected outright: Go's ServeMux
// implicitly matches "GET " patterns to HEAD requests too, which without an
// explicit guard would dispatch straight into getMeta and return 200,
// violating the exact-method contract that every non-PUT-token request other
// than GET must be 405.
func TestHeadRequestReturns405(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "100.100.96.5", "sbx-1")

	for _, path := range []string{"/", "/some/route"} {
		req := httptest.NewRequest("HEAD", "http://169.254.169.254"+path, nil)
		req.Header.Set("X-metadata-token", token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("HEAD %s: code=%d, want 405", path, w.Code)
		}
	}
}

func TestResponsesCarrySecureHeaders(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {"/x": {Type: "static", ContentType: "text/plain", Data: "hi"}},
		},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "100.100.96.5", "sbx-1")

	cases := []struct {
		name string
		req  func() *http.Request
	}{
		{"root 200", func() *http.Request {
			r := httptest.NewRequest("GET", "http://169.254.169.254/", nil)
			r.Header.Set("X-metadata-token", token)
			return r
		}},
		{"specified static 200", func() *http.Request {
			r := httptest.NewRequest("GET", "http://169.254.169.254/x", nil)
			r.Header.Set("X-metadata-token", token)
			return r
		}},
		{"unspecified 404", func() *http.Request {
			r := httptest.NewRequest("GET", "http://169.254.169.254/nope", nil)
			r.Header.Set("X-metadata-token", token)
			return r
		}},
		{"invalid token 401", func() *http.Request {
			r := httptest.NewRequest("GET", "http://169.254.169.254/", nil)
			r.Header.Set("X-metadata-token", "sbx-evil.deadbeef")
			return r
		}},
		{"unknown floating ip 503 (PUT)", func() *http.Request {
			r := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
			r.RemoteAddr = "9.9.9.9:1"
			return r
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, tt.req())
			if got := w.Header().Get("Cache-Control"); got != "no-store" {
				t.Errorf("Cache-Control = %q, want %q (code=%d)", got, "no-store", w.Code)
			}
			if got := w.Header().Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("X-Content-Type-Options = %q, want %q (code=%d)", got, "nosniff", w.Code)
			}
		})
	}
}

func TestGuardRawRequestRejectsNonCanonicalPathsBeforeServeMuxRedirects(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {"/a/b": {Type: "static", ContentType: "text/plain", Data: "specified"}},
		},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "100.100.96.5", "sbx-1")

	for _, tt := range []struct {
		name string
		raw  string // raw request-target, sent verbatim so it isn't pre-cleaned by httptest/net/url
	}{
		{"double slash", "/a//b"},
		{"dot-dot segment", "/a/../b"},
		{"dot segment", "/a/./b"},
		{"trailing slash", "/a/b/"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "http://169.254.169.254"+tt.raw, nil)
			req.Header.Set("X-metadata-token", token)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			// http.ServeMux would otherwise clean this path and issue a 301/308 to
			// it BEFORE any handler (including our 404 logic) ever runs -- the
			// design forbids any auto-redirect/rewrite of a non-canonical path.
			if w.Code == http.StatusMovedPermanently || w.Code == http.StatusPermanentRedirect {
				t.Fatalf("guard did not intercept before ServeMux's own redirect: code=%d location=%q", w.Code, w.Header().Get("Location"))
			}
			if w.Code != http.StatusBadRequest {
				t.Fatalf("code=%d, want 400", w.Code)
			}
		})
	}
}

func TestGuardRawRequestRejectsPercentEscapedPathEvenWhenSpecified(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {"/a/b": {Type: "static", ContentType: "text/plain", Data: "specified"}},
		},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "100.100.96.5", "sbx-1")

	// "/a%2Fb" decodes to the specified "/a/b" -- it must NOT match via the
	// decoded r.URL.Path; the raw percent-escape is rejected outright.
	req := httptest.NewRequest("GET", "http://169.254.169.254/a%2Fb", nil)
	req.Header.Set("X-metadata-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("percent-escaped path: code=%d body=%q, want 400", w.Code, w.Body.String())
	}
}

func TestGuardRawRequestRejectsQueries(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {"/x": {Type: "static", ContentType: "text/plain", Data: "specified"}},
		},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "100.100.96.5", "sbx-1")

	for _, tt := range []struct {
		name   string
		method string
		target string
	}{
		{"non-empty query", "GET", "/x?q=1"},
		{"empty query", "GET", "/x?"},
		{"token endpoint empty query", "PUT", "/latest/api/token?"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "http://169.254.169.254"+tt.target, nil)
			req.Header.Set("X-metadata-token", token)
			req.RemoteAddr = "100.100.96.5:1"
			if strings.HasSuffix(tt.target, "?") && !req.URL.ForceQuery {
				t.Fatal("test request did not preserve the empty query")
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s %s: code=%d body=%q, want 400", tt.method, tt.target, w.Code, w.Body.String())
			}
		})
	}
}

func TestGuardRawRequestAllowsCanonicalPaths(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"100.100.96.5": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "100.100.96.5", "sbx-1")

	req := httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("canonical root path rejected: code=%d", w.Code)
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
