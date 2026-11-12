package mmds

import (
	"context"
	"errors"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdssvc"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testHandler() (http.Handler, fakeSource) {
	src := fakeSource{
		fip:  map[string]string{"192.0.2.1": "sandbox-1"},
		info: map[string][2]string{"sandbox-1": {"template-1", "access-token"}},
		routes: map[string]map[string]MMDSRoute{
			"sandbox-1": {
				"/static":  {Type: "static", Body: []byte("static-data")},
				"/secret":  {Type: "secret", ContentType: "application/octet-stream", Body: []byte{0, 1, 2}, Present: true},
				"/missing": {Type: "secret"},
				"/service": {Type: "service", ReadService: func(context.Context) mmdssvc.Result {
					return mmdssvc.Result{StatusCode: http.StatusCreated, ContentType: "application/json", Body: []byte(`{"ok":true}`)}
				}},
			},
		},
	}
	return New(src, 10*time.Millisecond, nil).Handler(), src
}

func mintTestToken(t *testing.T, handler http.Handler) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = "192.0.2.1:1234"
	req.Header.Set("X-metadata-token-ttl-seconds", "60")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("PUT token: status=%d body=%q", response.Code, response.Body.String())
	}
	return response.Body.String()
}

func getRoute(t *testing.T, handler http.Handler, token, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://169.254.169.254"+path, nil)
	req.RemoteAddr = "192.0.2.1:4321"
	req.Header.Set("X-metadata-token", token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}

func TestRouteResponses(t *testing.T) {
	handler, _ := testHandler()
	token := mintTestToken(t, handler)

	tests := []struct {
		path        string
		status      int
		contentType string
		body        []byte
	}{
		{path: "/static", status: 200, contentType: "text/plain", body: []byte("static-data")},
		{path: "/secret", status: 200, contentType: "application/octet-stream", body: []byte{0, 1, 2}},
		{path: "/missing", status: 404, contentType: "text/plain; charset=utf-8", body: []byte("\n")},
		{path: "/service", status: 201, contentType: "application/json", body: []byte(`{"ok":true}`)},
		{path: "/undeclared", status: 404, contentType: "text/plain; charset=utf-8", body: []byte("\n")},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			response := getRoute(t, handler, token, tt.path)
			if response.Code != tt.status || response.Header().Get("Content-Type") != tt.contentType || string(response.Body.Bytes()) != string(tt.body) {
				t.Fatalf("response metadata or body mismatch: status=%d content-type=%q", response.Code, response.Header().Get("Content-Type"))
			}
		})
	}
}

func TestRouteResolutionFailureIs503(t *testing.T) {
	_, src := testHandler()
	src.routeErr = errors.New("route view unavailable")
	handler := New(src, 10*time.Millisecond, nil).Handler()
	response := getRoute(t, handler, mintTestToken(t, handler), "/static")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestUnavailableSourceFailsAllMMDSRequestsClosed(t *testing.T) {
	_, src := testHandler()
	handler := New(&src, 10*time.Millisecond, nil).Handler()
	token := mintTestToken(t, handler)
	src.unavailable = true

	req := httptest.NewRequest(http.MethodPut, "http://169.254.169.254/latest/api/token", nil)
	req.RemoteAddr = "192.0.2.1:1234"
	req.Header.Set("X-metadata-token-ttl-seconds", "60")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("token status = %d", response.Code)
	}
	response = getRoute(t, handler, token, "/")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("existing-token status = %d", response.Code)
	}
}

func TestRootOnlyMatchesExactly(t *testing.T) {
	handler, _ := testHandler()
	token := mintTestToken(t, handler)
	root := getRoute(t, handler, token, "/")
	if root.Code != http.StatusOK || !strings.Contains(root.Body.String(), `"instanceID":"sandbox-1"`) {
		t.Fatalf("root response: status=%d body=%q", root.Code, root.Body.String())
	}
	other := getRoute(t, handler, token, "/not-root")
	if other.Code != http.StatusNotFound || strings.Contains(other.Body.String(), "instanceID") {
		t.Fatalf("other response: status=%d body=%q", other.Code, other.Body.String())
	}
}

func TestRequestGuardRejectsNonExactPathsAndQuery(t *testing.T) {
	handler, _ := testHandler()
	tests := []struct {
		name   string
		target string
		force  bool
	}{
		{name: "query", target: "http://169.254.169.254/static?q=1"},
		{name: "empty query", target: "http://169.254.169.254/static", force: true},
		{name: "escape", target: "http://169.254.169.254/a%2Fb"},
		{name: "empty segment", target: "http://169.254.169.254/a//b"},
		{name: "dot segment", target: "http://169.254.169.254/a/./b"},
		{name: "dotdot segment", target: "http://169.254.169.254/a/../b"},
		{name: "trailing slash", target: "http://169.254.169.254/a/"},
		{name: "backslash", target: "http://169.254.169.254/a%5Cb"},
		{name: "wildcard", target: "http://169.254.169.254/a*b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.target, nil)
			req.URL.ForceQuery = tt.force
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != http.StatusBadRequest || response.Header().Get("Location") != "" {
				t.Fatalf("status=%d location=%q", response.Code, response.Header().Get("Location"))
			}
		})
	}
}

func TestGETBodyIsRejectedWithoutReading(t *testing.T) {
	handler, _ := testHandler()
	tests := []struct {
		name   string
		mutate func(*http.Request)
	}{
		{name: "content length", mutate: func(r *http.Request) { r.ContentLength = 1 }},
		{name: "unknown length", mutate: func(r *http.Request) { r.ContentLength = -1 }},
		{name: "transfer encoding", mutate: func(r *http.Request) { r.TransferEncoding = []string{"chunked"} }},
		{name: "opaque body", mutate: func(r *http.Request) { r.Body = io.NopCloser(strings.NewReader("")) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "http://169.254.169.254/", nil)
			tt.mutate(req)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d", response.Code)
			}
		})
	}
}

func TestAllResponsesCarrySecurityHeaders(t *testing.T) {
	handler, _ := testHandler()
	token := mintTestToken(t, handler)
	for _, path := range []string{"/", "/static", "/missing", "/undeclared"} {
		response := getRoute(t, handler, token, path)
		if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("path %s headers = %#v", path, response.Header())
		}
	}
	req := httptest.NewRequest(http.MethodGet, "http://169.254.169.254/a//b", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("guard headers = %#v", response.Header())
	}
}

func TestTokenTTLAndBinding(t *testing.T) {
	handler, src := testHandler()
	for _, value := range []string{"", "0", "-1", "not-a-number", "21601"} {
		req := httptest.NewRequest(http.MethodPut, "http://169.254.169.254/latest/api/token", nil)
		req.RemoteAddr = "192.0.2.1:1"
		if value != "" {
			req.Header.Set("X-metadata-token-ttl-seconds", value)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("TTL %q status = %d", value, response.Code)
		}
	}

	token := mintTestToken(t, handler)
	req := httptest.NewRequest(http.MethodGet, "http://169.254.169.254/", nil)
	req.RemoteAddr = "192.0.2.2:1"
	req.Header.Set("X-metadata-token", token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("different source status = %d", response.Code)
	}

	expired, err := mintToken(tokenPayload{
		SID:         "sandbox-1",
		SourceIP:    "192.0.2.1",
		Incarnation: "run-for-sandbox-1",
		Audience:    mmdsTokenAudience,
		ExpiresUnix: time.Now().Add(-time.Second).Unix(),
	}, []byte("secret-for-sandbox-1"))
	if err != nil {
		t.Fatal(err)
	}
	response = getRoute(t, handler, expired, "/")
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expired status = %d", response.Code)
	}

	src.incarnation = map[string]string{"sandbox-1": "run-1"}
	handler = New(src, 10*time.Millisecond, nil).Handler()
	old := mintTestToken(t, handler)
	src.incarnation["sandbox-1"] = "run-2"
	response = getRoute(t, handler, old, "/")
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("old incarnation status = %d", response.Code)
	}
}

func TestHoldTrafficConditionalGuestProtocol(t *testing.T) {
	_, src := testHandler()
	routes := src.routes["sandbox-1"]
	routes["/jobs"] = MMDSRoute{Type: "hold-traffic"}
	calledService := false
	routes["/service"] = MMDSRoute{Type: "service", ReadService: func(context.Context) mmdssvc.Result { calledService = true; return mmdssvc.Result{StatusCode: 200} }}
	var current int64
	calls := 0
	src.hold = func(_ context.Context, sid, run, path string, value, match *int64) (*int64, int) {
		if sid != "sandbox-1" || run != "run-for-sandbox-1" || path != "/jobs" {
			t.Fatalf("wrong authenticated target %s/%s/%s", sid, run, path)
		}
		calls++
		if match != nil && *match != current {
			return &current, 409
		}
		if value != nil {
			current = *value
		}
		return &current, 200
	}
	h := New(src, time.Second, nil).Handler()
	tok := mintTestToken(t, h)
	exchange := func(method, path, body string, chunked bool) (int, string) {
		var b io.Reader
		if body != "" {
			b = strings.NewReader(body)
		}
		r := httptest.NewRequest(method, "http://169.254.169.254"+path, b)
		r.RemoteAddr = "192.0.2.1:5522"
		r.Header.Set("X-metadata-token", tok)
		if chunked {
			r.ContentLength = -1
			r.TransferEncoding = []string{"chunked"}
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code, w.Body.String()
	}
	for _, tt := range []struct {
		method, path, body string
		chunked            bool
		status             int
		result             string
	}{
		{"GET", "/jobs", "", false, 200, "0"},
		{"PUT", "/jobs?if-match=%30", "1", true, 200, "1"},
		{"PUT", "/jobs?if-match=0", "1", false, 409, "1"},
		{"put", "/jobs?if-match=1", "0", false, 200, "0"},
		{"PUT", "/jobs", "23", false, 200, "23"},
		{"GET", "/jobs", "", false, 200, "23"},
	} {
		status, body := exchange(tt.method, tt.path, tt.body, tt.chunked)
		if status != tt.status || body != tt.result {
			t.Errorf("%s %s => %d %q; want %d %q", tt.method, tt.path, status, body, tt.status, tt.result)
		}
	}
	before := calls
	for _, tt := range []struct {
		method, path, body string
		status             int
	}{
		{"PUT", "/jobs?if-match=0&if-match=0", "0", 400},
		{"PUT", "/jobs?other=0", "0", 400}, {"PUT", "/jobs?", "0", 400},
		{"PUT", "/jobs?if-match=", "0", 400}, {"PUT", "/jobs?if-match=%GG", "0", 400},
		{"PUT", "/jobs?if-match=0&", "0", 400}, {"PUT", "/jobs?if-match=0;other=1", "0", 400},
		{"PUT", "/jobs", "", 400}, {"PUT", "/jobs", "-1", 400}, {"PUT", "/jobs", "9223372036854775808", 400},
		{"PUT", "/jobs", "1=2", 400}, {"PUT", "/service", "1", 404}, {"put", "/service", "1", 404},
		{"PUT", "/absent?if-match=0", "1", 404}, {"PUT", "/latest/api/token?if-match=0", "1", 400},
		{"put", "/latest/api/token", "1", 404}, {"GET", "/jobs?if-match=0", "", 400},
		{"put", "/%6Aobs?if-match=0", "1", 400},
	} {
		if status, _ := exchange(tt.method, tt.path, tt.body, false); status != tt.status {
			t.Errorf("%s %s body=%q => %d; want %d", tt.method, tt.path, tt.body, status, tt.status)
		}
	}
	if calls != before || calledService || current != 23 {
		t.Fatalf("rejected request executed: calls=%d service=%t current=%d", calls, calledService, current)
	}
}
