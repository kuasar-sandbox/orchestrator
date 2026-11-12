package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type holdAPITestCore struct {
	Core
	value  int64
	writes int
	paused bool
}

func (c *holdAPITestCore) GetSandboxHold(context.Context, string, string) (int64, error) {
	return c.value, nil
}
func (c *holdAPITestCore) SetSandboxHold(_ context.Context, id, key string, v int64, m *int64) (int64, error) {
	c.writes++
	if id != "sb" || key == "" {
		return 0, ErrNotFound
	}
	if c.paused {
		return 0, ErrHoldStateConflict
	}
	if m != nil && c.value != *m {
		return c.value, ErrHoldNotMatched
	}
	c.value = v
	return v, nil
}

func TestHoldAPIContract(t *testing.T) {
	c := new(holdAPITestCore)
	h, key := newSandboxContractHandler(t, c, Resources{})
	call := func(method, body string) (int, string) {
		r := httptest.NewRequest(method, "/sandboxes/sb/traffic/hold", strings.NewReader(body))
		r.Header.Set("X-API-KEY", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("cacheable response")
		}
		return w.Code, strings.TrimSpace(w.Body.String())
	}
	for _, raw := range []string{`{}`, `null`, `{"value":null}`, `{"value":2,"ifMatch":null}`, `{"value":"2"}`, `{"value":-1}`, `{"value":2.0}`, `{"value":2,"if-match":0}`, `{"value":2,"ifmatch":0}`, `{"value":2,"ifMatch":1,"ifMatch":0}`, `{"value":1,"ifMatch":0,"other":2}`, `{"value":1}{}`, `{"value":9223372036854775808}`} {
		if code, _ := call("PUT", raw); code != 400 {
			t.Errorf("%s: status %d", raw, code)
		}
	}
	if c.writes != 0 {
		t.Fatalf("forwarded %d malformed writes", c.writes)
	}
	if code, b := call("PUT", `{"value":1,"ifMatch":0}`); code != 200 || b != `{"value":1}` {
		t.Fatal(code, b)
	}
	if code, b := call("PUT", `{"value":1,"ifMatch":0}`); code != 409 || b != `{"value":1}` {
		t.Fatal(code, b)
	}
	if code, b := call("PUT", `{"value":0}`); code != 200 || b != `{"value":0}` {
		t.Fatal(code, b)
	}
	c.paused = true
	if code, b := call("PUT", `{"value":1}`); code != 409 || strings.Contains(b, `"value"`) {
		t.Fatal(code, b)
	}
	if code, b := call(http.MethodGet, ""); code != 200 || b != `{"value":0}` {
		t.Fatal(code, b)
	}
}
