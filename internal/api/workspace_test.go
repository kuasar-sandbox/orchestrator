package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
)

type workspaceTestCore struct {
	Core
	create CreateReq
	build  RegisterSpec
	calls  int
	disk   string
	opErr  error
}

func (c *workspaceTestCore) Create(_ context.Context, r CreateReq) (*types.Sandbox, error) {
	c.create = r
	return &types.Sandbox{ID: "sb", Profile: types.ProfileBare}, nil
}
func (c *workspaceTestCore) RegisterBuild(_ context.Context, _ string, r RegisterSpec) (*types.Build, error) {
	c.build = r
	return &types.Build{TemplateID: "transient-1", BuildID: "b"}, nil
}
func (c *workspaceTestCore) ListWorkspaces(context.Context, string, string) (WorkspaceList, error) {
	return WorkspaceList{}, c.opErr
}
func (c *workspaceTestCore) AttachWorkspace(_ context.Context, id, key, name string, disk json.RawMessage) error {
	if id != "sb" || key == "" || name != "w" {
		return ErrNotFound
	}
	c.calls++
	c.disk = string(disk)
	return c.opErr
}
func (c *workspaceTestCore) ExportWorkspace(_ context.Context, id, key, name string) (types.WorkspaceExportResult, error) {
	if id != "sb" || key == "" || name != "w" {
		return types.WorkspaceExportResult{}, ErrNotFound
	}
	return types.WorkspaceExportResult{Result: "kwt1.token", DiskReport: artifact.DiskReport{
		Refs: true, DiskRefs: []string{"manifest://" + strings.Repeat("a", 64)}, RemovedRefs: []string{},
	}}, c.opErr
}

func TestWorkspaceCreateAndBuildSeparatePrivateInputs(t *testing.T) {
	const token = "kwt1.private-source"
	const declaration = `[{"name":"w","mounts":[{"path":"/data","subpath":"project"}],"disk":{"migrationToken":"` + token + `"}}]`
	for _, path := range []string{"/sandboxes", "/v3/templates"} {
		for _, useHeader := range []bool{false, true} {
			c := new(workspaceTestCore)
			h, key := newSandboxContractHandler(t, c, Resources{})
			value := declaration
			if useHeader {
				value = "invalid but superseded"
			}
			body, _ := json.Marshal(map[string]any{"cpuCount": 1, "memoryMB": 256, "metadata": map[string]string{workspace.Namespace: value, "plain": "ok"}})
			r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
			r.Header.Set("X-API-KEY", key)
			if path == "/v3/templates" {
				r.Header.Set("X-Kuasar-Sandbox-Builder", `{"target":{"kind":"sandbox"}}`)
			}
			if useHeader {
				r.Header.Set(workspace.Header, declaration)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			meta, entries := c.create.Metadata, c.create.Workspaces
			want := 201
			if path == "/v3/templates" {
				meta, entries = c.build.Metadata, c.build.Workspaces
				want = 202
			}
			if w.Code != want || strings.Contains(w.Body.String(), token) || meta["plain"] != "ok" || meta[workspace.Namespace] != `[{"name":"w","mounts":[{"path":"/data","subpath":"project"}]}]` || len(entries) != 1 || !strings.Contains(string(entries[0].Disk), token) {
				t.Fatalf("%s header=%t status=%d body=%s metadata=%v", path, useHeader, w.Code, w.Body, meta)
			}
		}
	}
}

func TestWorkspaceRegistrationRejectsIncompatibleTarget(t *testing.T) {
	for _, header := range []string{"", `{"target":{"kind":"image"}}`} {
		core := new(workspaceTestCore)
		handler, key := newSandboxContractHandler(t, core, Resources{})
		req := httptest.NewRequest(http.MethodPost, "/v3/templates", strings.NewReader(`{"cpuCount":1,"memoryMB":256}`))
		req.Header.Set("X-API-KEY", key)
		req.Header.Set(workspace.Header, `[{"name":"work","mounts":[{"path":"/data"}]}]`)
		if header != "" {
			req.Header.Set("X-Kuasar-Sandbox-Builder", header)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "builder.target") || len(core.build.Workspaces) != 0 {
			t.Fatalf("target %q: status %d, body %s", header, w.Code, w.Body)
		}
	}
}

func TestWorkspaceOperations(t *testing.T) {
	core := new(workspaceTestCore)
	h, key := newSandboxContractHandler(t, core, Resources{})
	call := func(method, part, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/sandboxes/sb/workspaces"+part, strings.NewReader(body))
		r.Header.Set("X-API-KEY", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	w := call("GET", "", "")
	if w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"workspaces":[]}` {
		t.Fatal(w)
	}
	for _, body := range []string{`null`, `{}`, `{"Disk":{}}`, `{"disk":null}`, `{"disk":"kwt1"}`, `{"disk":{"migrationToken":"s","size":2}}`, `{"disk":{},"disk":{}}`, `{"diskRef":"foo"}`} {
		if w := call("POST", "/attach?name=w", body); w.Code != 400 {
			t.Errorf("%s: %d", body, w.Code)
		}
	}
	if core.calls != 0 {
		t.Fatal("invalid input dispatched")
	}
	if w := call("POST", "/attach?trace=abc&name=w", `{"disk":{}}`); w.Code != 204 || w.Body.Len() != 0 || core.disk != "{}" {
		t.Fatal(w)
	}
	for err, want := range map[error]int{ErrWorkspaceConflict: 409, ErrWorkspaceUnavailable: 503, ErrNotFound: 404, ErrNotAllowed: 403} {
		core.opErr = err
		if w := call("POST", "/attach?name=w", `{"disk":{}}`); w.Code != want {
			t.Fatalf("%v got %d", err, w.Code)
		}
	}
	core.opErr = nil
	if w := call("POST", "/export?name=w&trace=abc&trace=def", `{}`); w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"result":"kwt1.token","refs":true,"diskRefs":["manifest://`+strings.Repeat("a", 64)+`"],"removedRefs":[]}` {
		t.Fatal(w)
	}
	for _, args := range [][2]string{{"/export?name=w&name=x", "{}"}, {"/export?trace=abc", "{}"}, {"/export?name=&trace=abc", "{}"}, {"/export?name=w", `{"keepSource":false}`}} {
		if w := call("POST", args[0], args[1]); w.Code != 400 {
			t.Fatal(w)
		}
	}
}

func TestWorkspaceLegacyMetadataIsSafe(t *testing.T) {
	meta := map[string]string{"owner": "ok", workspace.Namespace: `[{"name":"w","mounts":[{"path":"/data"}],"disk":{"oldSource":"private-token"}}]`}
	c := &sandboxContractCoreStub{sandbox: &types.Sandbox{ID: "sb", Profile: types.ProfileBare, Metadata: meta}}
	h, key := newSandboxContractHandler(t, c, Resources{})
	for _, withPlan := range []bool{false, true} {
		if withPlan {
			c.sandbox.WorkspacePlan = []workspace.Prepared{{Name: "project", Mounts: []workspace.Mount{{Path: "/actual"}}}}
		}
		for _, p := range []string{"/sandboxes/sb", "/v2/sandboxes"} {
			r := httptest.NewRequest("GET", p, nil)
			r.Header.Set("X-API-KEY", key)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			response := w.Body.String()
			if w.Code != 200 || !strings.Contains(response, `"owner":"ok"`) || strings.Contains(response, "/data") || strings.Contains(response, "private-token") ||
				strings.Contains(response, workspace.Namespace) != withPlan || strings.Contains(response, "/actual") != withPlan {
				t.Fatalf("resolved plan %t, GET %s: %s", withPlan, p, response)
			}
		}
	}
	if !strings.Contains(meta[workspace.Namespace], "private-token") {
		t.Fatal("projection mutated source metadata")
	}
}
