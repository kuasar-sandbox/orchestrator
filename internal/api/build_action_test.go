package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDeleteBuildOptionsStrictPresence(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		header      []string
		want, bad   bool
	}{
		{name: "default"}, {name: "empty-object", header: []string{`{}`}},
		{name: "query-true", query: "cancel=true", want: true}, {name: "query-false", query: "cancel=false"},
		{name: "header-true", header: []string{`{"cancel":true}`}, want: true}, {name: "header-false", header: []string{`{"cancel":false}`}},
		{name: "empty-preserves-query", query: "cancel=true", header: []string{`{}`}, want: true},
		{name: "explicit-false-overrides", query: "cancel=true", header: []string{`{"cancel":false}`}},
		{name: "explicit-true-overrides", query: "cancel=false", header: []string{`{"cancel":true}`}, want: true},
		{name: "query-empty", query: "cancel=", bad: true}, {name: "query-bare", query: "cancel", bad: true},
		{name: "query-duplicate", query: "cancel=true&cancel=true", bad: true},
		{name: "query-duplicate-overridden", query: "cancel=false&cancel=true", header: []string{`{"cancel":true}`}, bad: true},
		{name: "query-number-overridden", query: "cancel=1", header: []string{`{"cancel":false}`}, bad: true},
		{name: "query-case", query: "cancel=True", bad: true}, {name: "query-escape", query: "cancel=%ZZ", bad: true},
		{name: "force", query: "force=true", bad: true},
		{name: "empty-header", header: []string{""}, bad: true}, {name: "space-header", header: []string{" "}, bad: true},
		{name: "duplicate-header", header: []string{`{}`, `{}`}, bad: true},
		{name: "null", header: []string{`null`}, bad: true}, {name: "array", header: []string{`[]`}, bad: true},
		{name: "bool-object", header: []string{`true`}, bad: true},
		{name: "field-null", header: []string{`{"cancel":null}`}, bad: true},
		{name: "field-string", header: []string{`{"cancel":"true"}`}, bad: true},
		{name: "field-number", header: []string{`{"cancel":1}`}, bad: true},
		{name: "field-array", header: []string{`{"cancel":[]}`}, bad: true},
		{name: "duplicate-key", header: []string{`{"cancel":false,"cancel":true}`}, bad: true},
		{name: "escaped-duplicate", header: []string{`{"cancel":false,"\u0063ancel":true}`}, bad: true},
		{name: "case-field", header: []string{`{"Cancel":true}`}, bad: true},
		{name: "unknown", header: []string{`{"cancelBuild":true}`}, bad: true},
		{name: "target", header: []string{`{"target":{}}`}, bad: true},
		{name: "resources", header: []string{`{"resources":{}}`}, bad: true},
		{name: "referer", header: []string{`{"referer":{}}`}, bad: true},
		{name: "registry", header: []string{`{"registry":{}}`}, bad: true},
		{name: "trailing-value", header: []string{`{} {}`}, bad: true},
		{name: "trailing-garbage", header: []string{`{} x`}, bad: true},
		{name: "trailing-space", header: []string{"{\"cancel\":true} \n"}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodDelete, "/templates/transient-test?"+tc.query, nil)
			if tc.header != nil {
				r.Header[http.CanonicalHeaderKey(builderHeader)] = tc.header
			}
			got, err := ParseDeleteBuildOptions(r)
			if (err != nil) != tc.bad || err == nil && got.Cancel != tc.want {
				t.Fatalf("options=%+v error=%v", got, err)
			}
		})
	}
}

type buildActionCoreStub struct {
	Core
	call func(bool, string, string, DeleteBuildOptions) (BuildActionResult, error)
}

func (c *buildActionCoreStub) CancelBuild(_ context.Context, _ string, tid, bid string) (BuildActionResult, error) {
	return c.call(false, tid, bid, DeleteBuildOptions{})
}
func (c *buildActionCoreStub) DeleteBuild(_ context.Context, _ string, tid string, options DeleteBuildOptions) (BuildActionResult, error) {
	return c.call(true, tid, "", options)
}

func TestBuildActionHTTPCompletionAndLocation(t *testing.T) {
	for _, action := range []string{"cancel", "delete"} {
		for _, tc := range []struct {
			name    string
			pending bool
			err     error
			status  int
		}{
			{"complete", false, nil, 204}, {"accepted", true, nil, 202}, {"owned", false, ErrBuildOwned, 409},
			{"missing", false, ErrNotFound, 404}, {"invalid", false, ErrBadRequest, 400}, {"write-failed", false, errors.New("write failed"), 500},
		} {
			t.Run(action+"/"+tc.name, func(t *testing.T) {
				calls := 0
				core := &buildActionCoreStub{call: func(remove bool, tid, bid string, options DeleteBuildOptions) (BuildActionResult, error) {
					calls++
					if tid != "transient-test" || remove != (action == "delete") || (action == "cancel" && bid != "test") {
						t.Fatalf("unexpected target remove=%v tid=%s bid=%s", remove, tid, bid)
					}
					return BuildActionResult{TemplateID: tid, BuildID: "test", Pending: tc.pending}, tc.err
				}}
				handler, key := newMigrationTestHandler(t, core)
				method, path := http.MethodDelete, "/templates/transient-test"
				if action == "cancel" {
					method, path = http.MethodPost, path+"/builds/test/cancel"
				}
				response := migrationRequest(t, handler, key, method, path, nil, nil)
				if response.Code != tc.status || calls != 1 {
					t.Fatalf("status=%d calls=%d body=%s", response.Code, calls, response.Body.String())
				}
				wantLocation := ""
				if action == "delete" && tc.pending {
					wantLocation = "/templates/transient-test/builds/test/status"
				}
				if response.Header().Get("Location") != wantLocation {
					t.Fatalf("Location=%q", response.Header().Get("Location"))
				}
			})
		}
	}
}

func TestDeleteBuildRejectsInvalidInputBeforeCore(t *testing.T) {
	core := &buildActionCoreStub{call: func(bool, string, string, DeleteBuildOptions) (BuildActionResult, error) {
		t.Fatal("invalid action reached Core")
		return BuildActionResult{}, nil
	}}
	handler, key := newMigrationTestHandler(t, core)
	response := migrationRequest(t, handler, key, http.MethodDelete, "/templates/transient-test?cancel=1", nil, http.Header{builderHeader: []string{`{"cancel":true}`}})
	if response.Code != 400 {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestRegisterAndTriggerRejectCancelActionField(t *testing.T) {
	for _, path := range []string{"/v3/templates", "/v2/templates/transient-test/builds/test"} {
		for _, raw := range []string{`true`, `false`, `null`, `"true"`} {
			t.Run(path+"/"+raw, func(t *testing.T) {
				handler, key := newMigrationTestHandler(t, &buildActionCoreStub{})
				response := migrationRequest(t, handler, key, http.MethodPost, path, strings.NewReader(`{"cpuCount":1,"memoryMB":512,"cancel":`+raw+`}`), nil)
				if response.Code != http.StatusBadRequest {
					t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
				}
			})
		}
		for _, header := range []string{`{"cancel":true}`, `{"cancel":false}`} {
			handler, key := newMigrationTestHandler(t, &buildActionCoreStub{})
			response := migrationRequest(t, handler, key, http.MethodPost, path, strings.NewReader(`{"cpuCount":1,"memoryMB":512}`), http.Header{builderHeader: []string{header}})
			if response.Code != http.StatusBadRequest {
				t.Fatalf("%s action header = %d", path, response.Code)
			}
		}
	}
}
