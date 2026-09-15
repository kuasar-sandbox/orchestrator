package configsock

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
)

type actionAdminStub struct {
	calls  int
	cancel bool
	result api.BuildActionResult
	err    error
}

func (s *actionAdminStub) CancelBuildAdmin(_ context.Context, bid string) (api.BuildActionResult, error) {
	s.calls++
	if bid != "b1" {
		return api.BuildActionResult{}, api.ErrNotFound
	}
	return s.result, s.err
}
func (s *actionAdminStub) DeleteBuildAdmin(_ context.Context, tid string, opts api.DeleteBuildOptions) (api.BuildActionResult, error) {
	s.calls++
	s.cancel = opts.Cancel
	if tid != "transient-b1" {
		return api.BuildActionResult{}, api.ErrNotFound
	}
	return s.result, s.err
}

func TestBuildActionAdminUsesPeerGateAndSharedActionParser(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "admin.pids")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1)+"\n")
	admin := &actionAdminStub{result: api.BuildActionResult{BuildID: "b1", TemplateID: "transient-b1", Pending: true}}
	_, client := startTestServer(t, Deps{BuilderActionAdmin: admin, AdminPidfile: pf})
	call := func(method, path, header string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(method, "http://localhost"+path, nil)
		if header != "" {
			req.Header.Set("X-Kuasar-Sandbox-Builder", header)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	cancelPath := strings.Replace(PathAdminBuildCancel, "{bid}", "b1", 1)
	deletePath := strings.Replace(PathAdminBuildDelete, "{tid}", "transient-b1", 1)
	for _, op := range []struct{ method, path string }{{"POST", cancelPath}, {"DELETE", deletePath}} {
		if resp := call(op.method, op.path, ""); resp.StatusCode != 403 || admin.calls != 0 {
			t.Fatalf("admin gate: %d calls=%d", resp.StatusCode, admin.calls)
		}
	}
	mustWrite(t, pf, strconv.Itoa(os.Getpid())+"\n")
	for _, tc := range []struct {
		query  string
		header []string
	}{
		{query: "cancel=true"}, {query: "cancel=false"}, {query: "cancel=%ZZ"}, {query: "delete=true"},
		{header: []string{""}}, {header: []string{`{}`}}, {header: []string{`null`}},
		{header: []string{`{"cancel":false}`}}, {header: []string{`{"cancel":true}`, `{}`}},
	} {
		before := admin.calls
		req, err := http.NewRequest(http.MethodPost, "http://localhost"+cancelPath+"?"+tc.query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if tc.header != nil {
			req.Header[http.CanonicalHeaderKey("X-Kuasar-Sandbox-Builder")] = tc.header
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || admin.calls != before {
			t.Fatalf("cancel options %+v: status=%d calls=%d -> %d", tc, resp.StatusCode, before, admin.calls)
		}
	}
	for _, tc := range []struct {
		query, header string
		cancel        bool
		code          int
	}{{"cancel=true", `{}`, true, 202}, {"cancel=true", `{"cancel":false}`, false, 202}, {"cancel=false", `{"cancel":true}`, true, 202}, {"cancel=1", `{"cancel":true}`, false, 400}, {"", `{"cancel":null}`, false, 400}} {
		before := admin.calls
		resp := call("DELETE", deletePath+"?"+tc.query, tc.header)
		if resp.StatusCode != tc.code {
			t.Fatalf("%+v status=%d", tc, resp.StatusCode)
		}
		if tc.code == 400 {
			if before != admin.calls {
				t.Fatal("invalid action called core")
			}
		} else if admin.cancel != tc.cancel || resp.Header.Get("Location") != "/templates/transient-b1/builds/b1/status" {
			t.Fatalf("action changed: cancel=%v Location=%s", admin.cancel, resp.Header.Get("Location"))
		}
	}
	for _, tc := range []struct {
		err     error
		pending bool
		code    int
	}{{nil, true, 202}, {nil, false, 204}, {api.ErrNotFound, false, 404}, {api.ErrBuildOwned, false, 409}, {api.ErrBadRequest, false, 400}, {errors.New("write failed"), false, 500}} {
		admin.err = tc.err
		admin.result.Pending = tc.pending
		if resp := call("POST", cancelPath, ""); resp.StatusCode != tc.code {
			t.Fatalf("%v: status=%d", tc.err, resp.StatusCode)
		}
	}
}
