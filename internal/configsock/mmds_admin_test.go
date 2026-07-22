package configsock

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"
)

type mmdsCall struct {
	op          string
	sid, name   string
	value       []byte
	contentType string
	expiresUnix int64
}

type stubMmdsAdmin struct {
	calls []mmdsCall
	// err, if set, is returned by every mutation (used to simulate not-found /
	// wrong-backend / a generic failure).
	err error
}

func (a *stubMmdsAdmin) SetMMDSStoreValue(_ context.Context, sid, name string, value []byte, contentType string, expiresUnix int64) error {
	a.calls = append(a.calls, mmdsCall{op: "set_store", sid: sid, name: name, value: value, contentType: contentType, expiresUnix: expiresUnix})
	return a.err
}

func (a *stubMmdsAdmin) ClearMMDSStoreValue(_ context.Context, sid, name string) error {
	a.calls = append(a.calls, mmdsCall{op: "clear_store", sid: sid, name: name})
	return a.err
}

func (a *stubMmdsAdmin) SetMMDSRelayAuth(_ context.Context, sid, name string, value []byte) error {
	a.calls = append(a.calls, mmdsCall{op: "set_relay_auth", sid: sid, name: name, value: value})
	return a.err
}

func (a *stubMmdsAdmin) ClearMMDSRelayAuth(_ context.Context, sid, name string) error {
	a.calls = append(a.calls, mmdsCall{op: "clear_relay_auth", sid: sid, name: name})
	return a.err
}

func rawPutDelete(t *testing.T, client *http.Client, method, path string, headers map[string]string, body []byte) int {
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
	return resp.StatusCode
}

func TestMmdsAdminStorePutAndDelete(t *testing.T) {
	adm := &stubMmdsAdmin{}
	_, client := startTestServer(t, Deps{MmdsEndpoints: adm})

	path := "/internal/admin/sandboxes/sb-1/mmds/user-data"
	code := rawPutDelete(t, client, http.MethodPut, path, map[string]string{"Content-Type": "text/plain"}, []byte("hello"))
	if code != http.StatusNoContent {
		t.Fatalf("PUT store value = %d, want 204", code)
	}
	if len(adm.calls) != 1 || adm.calls[0].op != "set_store" || adm.calls[0].sid != "sb-1" || adm.calls[0].name != "user-data" ||
		string(adm.calls[0].value) != "hello" || adm.calls[0].contentType != "text/plain" {
		t.Fatalf("unexpected call: %+v", adm.calls)
	}

	code = rawPutDelete(t, client, http.MethodDelete, path, nil, nil)
	if code != http.StatusNoContent {
		t.Fatalf("DELETE store value = %d, want 204", code)
	}
	if len(adm.calls) != 2 || adm.calls[1].op != "clear_store" {
		t.Fatalf("unexpected calls after delete: %+v", adm.calls)
	}
}

func TestMmdsAdminStorePutWithExpires(t *testing.T) {
	adm := &stubMmdsAdmin{}
	_, client := startTestServer(t, Deps{MmdsEndpoints: adm})

	code := rawPutDelete(t, client, http.MethodPut, "/internal/admin/sandboxes/sb-1/mmds/a",
		map[string]string{MMDSExpiresHeader: "12345"}, []byte("v"))
	if code != http.StatusNoContent {
		t.Fatalf("PUT with expires = %d, want 204", code)
	}
	if adm.calls[0].expiresUnix != 12345 {
		t.Fatalf("expiresUnix = %d, want 12345", adm.calls[0].expiresUnix)
	}

	code = rawPutDelete(t, client, http.MethodPut, "/internal/admin/sandboxes/sb-1/mmds/a",
		map[string]string{MMDSExpiresHeader: "not-a-number"}, []byte("v"))
	if code != http.StatusBadRequest {
		t.Fatalf("PUT with malformed expires = %d, want 400", code)
	}
}

func TestMmdsAdminRelayAuthPutAndDelete(t *testing.T) {
	adm := &stubMmdsAdmin{}
	_, client := startTestServer(t, Deps{MmdsEndpoints: adm})

	path := "/internal/admin/sandboxes/sb-1/mmds/credentials/auth"
	code := rawPutDelete(t, client, http.MethodPut, path, nil, []byte("secret-token"))
	if code != http.StatusNoContent {
		t.Fatalf("PUT relay auth = %d, want 204", code)
	}
	if len(adm.calls) != 1 || adm.calls[0].op != "set_relay_auth" || string(adm.calls[0].value) != "secret-token" {
		t.Fatalf("unexpected call: %+v", adm.calls)
	}

	code = rawPutDelete(t, client, http.MethodDelete, path, nil, nil)
	if code != http.StatusNoContent {
		t.Fatalf("DELETE relay auth = %d, want 204", code)
	}
	if len(adm.calls) != 2 || adm.calls[1].op != "clear_relay_auth" {
		t.Fatalf("unexpected calls after delete: %+v", adm.calls)
	}
}

func TestMmdsAdminNotFoundAndWrongBackend(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"not-found", ErrMMDSEndpointNotFound},
		{"wrong-backend", ErrMMDSEndpointWrongBackend},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adm := &stubMmdsAdmin{err: tc.err}
			_, client := startTestServer(t, Deps{MmdsEndpoints: adm})
			code := rawPutDelete(t, client, http.MethodPut, "/internal/admin/sandboxes/sb-1/mmds/a", nil, []byte("v"))
			if code != http.StatusNotFound {
				t.Fatalf("PUT with %v = %d, want 404", tc.err, code)
			}
		})
	}
}

func TestMmdsAdminGenericErrorIsInternalError(t *testing.T) {
	adm := &stubMmdsAdmin{err: errors.New("boom")}
	_, client := startTestServer(t, Deps{MmdsEndpoints: adm})
	code := rawPutDelete(t, client, http.MethodPut, "/internal/admin/sandboxes/sb-1/mmds/a", nil, []byte("v"))
	if code != http.StatusInternalServerError {
		t.Fatalf("PUT with a generic error = %d, want 500", code)
	}
}

func TestMmdsAdminPidfileGate(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "admin.pids")
	mustWrite(t, pf, strconv.Itoa(-1)+"\n") // excludes this test's own pid
	adm := &stubMmdsAdmin{}
	_, client := startTestServer(t, Deps{MmdsEndpoints: adm, AdminPidfile: pf})

	code := rawPutDelete(t, client, http.MethodPut, "/internal/admin/sandboxes/sb-1/mmds/a", nil, []byte("v"))
	if code != http.StatusForbidden {
		t.Fatalf("excluded pid PUT = %d, want 403", code)
	}
	if len(adm.calls) != 0 {
		t.Fatalf("admin was called despite failed auth: %+v", adm.calls)
	}
}

func TestMmdsAdminOversizedBodyRejected(t *testing.T) {
	adm := &stubMmdsAdmin{}
	_, client := startTestServer(t, Deps{MmdsEndpoints: adm, MMDSMaxStoreValueBytes: 4})

	code := rawPutDelete(t, client, http.MethodPut, "/internal/admin/sandboxes/sb-1/mmds/a", nil, []byte("this-is-too-long"))
	if code != http.StatusBadRequest {
		t.Fatalf("oversized PUT = %d, want 400", code)
	}
}

func TestMmdsAdminRoutesNotRegisteredWhenNilDeps(t *testing.T) {
	_, client := startTestServer(t, Deps{})
	code := rawPutDelete(t, client, http.MethodPut, "/internal/admin/sandboxes/sb-1/mmds/a", nil, []byte("v"))
	// No API handler and no MmdsEndpoints => falls through to a 404 from the
	// bare ServeMux (no handler registered for this path at all).
	if code != http.StatusNotFound {
		t.Fatalf("PUT with mmds admin disabled = %d, want 404 (route not registered)", code)
	}
}

func TestMmdsAdminStoreAndAuthPathsDoNotCollide(t *testing.T) {
	adm := &stubMmdsAdmin{}
	_, client := startTestServer(t, Deps{MmdsEndpoints: adm})

	// An endpoint literally named "auth" must still hit the store pattern, not
	// the .../{name}/auth pattern (which requires a further path segment).
	code := rawPutDelete(t, client, http.MethodPut, "/internal/admin/sandboxes/sb-1/mmds/auth", nil, []byte("v"))
	if code != http.StatusNoContent {
		t.Fatalf("PUT to an endpoint literally named 'auth' = %d, want 204", code)
	}
	if len(adm.calls) != 1 || adm.calls[0].op != "set_store" || adm.calls[0].name != "auth" {
		t.Fatalf("unexpected call: %+v", adm.calls)
	}
}
