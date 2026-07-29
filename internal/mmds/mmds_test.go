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
	fip         map[string]string               // floatingip -> sid
	info        map[string][2]string            // sid -> {tid, token}
	routes      map[string]map[string]MMDSRoute // sid -> path -> route
	routeErr    error                           // if set, MMDSRoute always fails with this error
	incarnation map[string]string               // sid -> incarnation, overriding the deterministic default when non-nil
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

// Incarnation returns f.incarnation[sid] when set (letting a test mutate the
// shared map to simulate a pause/resume reassigning the incarnation after a
// token was minted), else a deterministic default (a stand-in for
// types.Sandbox.RunID). ok=false for an unknown sandbox so a token can't be
// minted or verified without one.
func (f fakeSource) Incarnation(sid string) (string, bool) {
	if f.incarnation != nil {
		v, ok := f.incarnation[sid]
		return v, ok
	}
	if _, ok := f.info[sid]; !ok {
		return "", false
	}
	return "run-for-" + sid, true
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
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()

	// PUT from the sandbox's floating IP -> a session token.
	req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = "192.0.2.1:34567"
	req.Header.Set("X-metadata-token-ttl-seconds", "60")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 || w.Body.Len() == 0 {
		t.Fatalf("PUT: code=%d body=%q", w.Code, w.Body.String())
	}
	token := w.Body.String()

	// GET with the token from the SAME source IP the token was minted for -> the
	// sandbox metadata.
	req = httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "192.0.2.1:1"
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

	// GET with the SAME valid token but from a DIFFERENT source IP -> 401 (the
	// token is bound to the source IP it was minted for).
	req = httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "9.9.9.9:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("GET from a different source ip: code=%d (want 401)", w.Code)
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
	req.Header.Set("X-metadata-token-ttl-seconds", "60")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("PUT unknown ip: code=%d (want 503)", w.Code)
	}
}

// mintedToken returns a valid session token for sid via the real PUT flow.
func mintedToken(t *testing.T, h http.Handler, floatingIP, sid string) string {
	t.Helper()
	req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = floatingIP + ":1"
	req.Header.Set("X-metadata-token-ttl-seconds", "60")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("PUT: code=%d body=%q", w.Code, w.Body.String())
	}
	return w.Body.String()
}

func TestGetMetaServesSpecifiedStaticRoute(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {
				"/static-path": {Type: "static", ContentType: "application/json", Data: `{"key":"value"}`},
			},
		},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "192.0.2.1", "sbx-1")

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

func TestGetMetaReturns503ForSpecifiedServiceRoute(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {"/backend-path": {Type: "service"}},
		},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "192.0.2.1", "sbx-1")

	req := httptest.NewRequest("GET", "http://169.254.169.254/backend-path", nil)
	req.Header.Set("X-metadata-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET specified service route: code=%d (want 503)", w.Code)
	}
}

func TestGetMetaReturns404ForSpecifiedButAbsentSecretRoute(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {"/backend-path": {Type: "secret", Present: false}},
		},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "192.0.2.1", "sbx-1")

	req := httptest.NewRequest("GET", "http://169.254.169.254/backend-path", nil)
	req.Header.Set("X-metadata-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("GET specified-but-absent secret route: code=%d (want 404)", w.Code)
	}
}

func TestGetMetaServesConfiguredSecretRoute(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {"/backend-path": {Type: "secret", Present: true, ContentType: "text/plain", Data: "sh-sh-secret"}},
		},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "192.0.2.1", "sbx-1")

	req := httptest.NewRequest("GET", "http://169.254.169.254/backend-path", nil)
	req.Header.Set("X-metadata-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET configured secret route: code=%d body=%q", w.Code, w.Body.String())
	}
	if got, want := w.Body.String(), "sh-sh-secret"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
	if got, want := w.Header().Get("Content-Type"), "text/plain"; got != want {
		t.Fatalf("Content-Type = %q, want %q", got, want)
	}
}

func TestGetMetaReturns503OnRouteResolutionError(t *testing.T) {
	src := fakeSource{
		fip:      map[string]string{"192.0.2.1": "sbx-1"},
		info:     map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routeErr: errors.New("rpc timeout"),
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "192.0.2.1", "sbx-1")

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
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "192.0.2.1", "sbx-1")

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
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "192.0.2.1", "sbx-1")

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

func TestResponsesCarrySecureHeaders(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {"/x": {Type: "static", ContentType: "text/plain", Data: "hi"}},
		},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "192.0.2.1", "sbx-1")

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
			r.Header.Set("X-metadata-token-ttl-seconds", "60")
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
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {"/a/b": {Type: "static", ContentType: "text/plain", Data: "specified"}},
		},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "192.0.2.1", "sbx-1")

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
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {"/a/b": {Type: "static", ContentType: "text/plain", Data: "specified"}},
		},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "192.0.2.1", "sbx-1")

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

func TestGuardRawRequestRejectsQueryOnSpecifiedRoute(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		routes: map[string]map[string]MMDSRoute{
			"sbx-1": {"/x": {Type: "static", ContentType: "text/plain", Data: "specified"}},
		},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "192.0.2.1", "sbx-1")

	req := httptest.NewRequest("GET", "http://169.254.169.254/x?q=1", nil)
	req.Header.Set("X-metadata-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("path with query: code=%d body=%q, want 400", w.Code, w.Body.String())
	}
}

func TestGuardRawRequestAllowsCanonicalPaths(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "192.0.2.1", "sbx-1")

	req := httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("canonical root path rejected: code=%d", w.Code)
	}
}

func TestPutTokenRequiresTTLHeader(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()

	for _, tt := range []struct {
		name string
		set  func(r *http.Request)
	}{
		{"missing", func(r *http.Request) {}},
		{"zero", func(r *http.Request) { r.Header.Set("X-metadata-token-ttl-seconds", "0") }},
		{"negative", func(r *http.Request) { r.Header.Set("X-metadata-token-ttl-seconds", "-1") }},
		{"non-numeric", func(r *http.Request) { r.Header.Set("X-metadata-token-ttl-seconds", "soon") }},
		{"over max", func(r *http.Request) { r.Header.Set("X-metadata-token-ttl-seconds", "21601") }},
		{"duplicate", func(r *http.Request) {
			r.Header.Add("X-metadata-token-ttl-seconds", "60")
			r.Header.Add("X-metadata-token-ttl-seconds", "120")
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
			req.RemoteAddr = "192.0.2.1:1"
			tt.set(req)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("code=%d, want 400", w.Code)
			}
		})
	}
}

func TestPutTokenEchoesAcceptedTTL(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()

	req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = "192.0.2.1:1"
	req.Header.Set("X-metadata-token-ttl-seconds", "300")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("PUT: code=%d", w.Code)
	}
	if got, want := w.Header().Get("X-metadata-token-ttl-seconds"), "300"; got != want {
		t.Fatalf("echoed ttl = %q, want %q", got, want)
	}
}

func TestGetMetaRejectsExpiredToken(t *testing.T) {
	src := fakeSource{
		fip:  map[string]string{"192.0.2.1": "sbx-1"},
		info: map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
	}
	h := New(src, 50*time.Millisecond, nil).Handler()

	req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = "192.0.2.1:1"
	req.Header.Set("X-metadata-token-ttl-seconds", "1")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("PUT: code=%d", w.Code)
	}
	token := w.Body.String()

	time.Sleep(1100 * time.Millisecond)

	req = httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	req.RemoteAddr = "192.0.2.1:1"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("GET with expired token: code=%d (want 401)", w.Code)
	}
}

// TestGetMetaRejectsTokenAfterIncarnationChanges proves a token minted under one
// run incarnation stops verifying once the sandbox's current incarnation changes
// (the pause/resume invalidation the design requires), without any explicit
// revocation list.
func TestGetMetaRejectsTokenAfterIncarnationChanges(t *testing.T) {
	incarnation := map[string]string{"sbx-1": "run-1"}
	src := fakeSource{
		fip:         map[string]string{"192.0.2.1": "sbx-1"},
		info:        map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		incarnation: incarnation,
	}
	h := New(src, 50*time.Millisecond, nil).Handler()
	token := mintedToken(t, h, "192.0.2.1", "sbx-1")

	// Still valid under the same incarnation.
	req := httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("GET before resume: code=%d", w.Code)
	}

	// Simulate pause/resume: the sandbox gets a fresh incarnation.
	incarnation["sbx-1"] = "run-2"

	req = httptest.NewRequest("GET", "http://169.254.169.254/", nil)
	req.Header.Set("X-metadata-token", token)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("GET after resume: code=%d (want 401, token minted under a prior incarnation)", w.Code)
	}
}

func TestPutTokenFailsClosedWhenIncarnationUnknown(t *testing.T) {
	src := fakeSource{
		fip:         map[string]string{"192.0.2.1": "sbx-1"},
		info:        map[string][2]string{"sbx-1": {"tmpl-1", "tok-abc"}},
		incarnation: map[string]string{}, // sbx-1 has no incarnation yet
	}
	h := New(src, 50*time.Millisecond, nil).Handler()

	req := httptest.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = "192.0.2.1:1"
	req.Header.Set("X-metadata-token-ttl-seconds", "60")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("PUT with no incarnation yet: code=%d (want 503)", w.Code)
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
