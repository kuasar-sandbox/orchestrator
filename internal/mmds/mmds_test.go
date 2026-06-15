package mmds

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeSource struct {
	fip  map[string]string    // floatingip -> sid
	info map[string][2]string // sid -> {tid, token}
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
