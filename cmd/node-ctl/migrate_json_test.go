package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
)

func TestExportSandboxJSONClient(t *testing.T) {
	t.Setenv("E2B_API_KEY", "test-owner")
	root := "manifest://" + strings.Repeat("a", 64)
	e := "manifest://" + strings.Repeat("b", 64)
	report := types.ExportResult{Result: "kmt1.test", PublishReport: artifact.PublishReport{SnapshotRef: root, SandboxRef: e, RemovedRefs: []string{"file://old.snapshot"}}}
	valid, _ := json.Marshal(report)
	tests := []struct {
		name     string
		body     string
		template bool
		wantErr  bool
	}{
		{"token", string(valid), false, false},
		{"template", `{"result":"` + types.TemplateID{Profile: types.ProfileBare, Kind: types.KindSnp, Ref: root}.String() + `","snapshotRef":"` + root + `","sandboxRef":"` + e + `","removedRefs":[]}`, true, false},
		{"sandbox", `{"result":"kmt1.test","sandboxRef":"` + e + `","removedRefs":[]}`, false, false},
		{"invalid", "{", false, true}, {"trailing", string(valid) + "{}", false, true}, {"pollution", "progress\n" + string(valid), false, true},
		{"no result", `{"sandboxRef":"` + e + `","removedRefs":[]}`, false, true},
		{"no root", `{"result":"kmt1.test","removedRefs":[]}`, false, true},
		{"null removals", `{"result":"kmt1.test","sandboxRef":"` + e + `","removedRefs":null}`, false, true},
		{"path leak", strings.Replace(string(valid), "file://old.snapshot", "file:///private/mount/old.snapshot", 1), false, true},
		{"duplicate", strings.Replace(string(valid), `"result":`, `"result":"pollution","result":`, 1), false, true},
		{"role mismatch", `{"result":"` + types.TemplateID{Profile: types.ProfileBare, Kind: types.KindSbx, Ref: e}.String() + `","snapshotRef":"` + root + `","sandboxRef":"` + e + `","removedRefs":[]}`, true, true},
	}
	for _, tt := range tests {
		for _, keepSource := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/keep=%t", tt.name, keepSource), func(t *testing.T) {
				socket := filepath.Join(t.TempDir(), "ctl.sock")
				listener, err := net.Listen("unix", socket)
				if err != nil {
					t.Fatal(err)
				}
				server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodPost || r.URL.Path != "/sandboxes/source/export" || r.Header.Get("X-API-KEY") != "test-owner" {
						t.Errorf("request=%s %s", r.Method, r.URL.Path)
					}
					var args struct {
						KeepSource bool `json:"keepSource"`
						ToTemplate bool `json:"toTemplate"`
					}
					json.NewDecoder(r.Body).Decode(&args)
					if args.KeepSource != keepSource || args.ToTemplate != tt.template {
						t.Errorf("args=%+v", args)
					}
					io.WriteString(w, tt.body)
				})}
				go server.Serve(listener)
				defer server.Close()
				for _, asJSON := range []bool{false, true} {
					args := []string{"source", "--socket", socket}
					if keepSource {
						args = append(args, "--keep-source")
					}
					if tt.template {
						args = append(args, "--to-template")
					}
					if asJSON {
						args = append(args, "--json")
					}
					old := os.Stdout
					r, w, err := os.Pipe()
					if err != nil {
						t.Fatal(err)
					}
					os.Stdout = w
					callErr := exportSandboxCmd(args, nil)
					w.Close()
					os.Stdout = old
					out, _ := io.ReadAll(r)
					r.Close()
					if (callErr != nil) != tt.wantErr {
						t.Fatalf("err=%v output=%s", callErr, out)
					}
					if tt.wantErr {
						if len(out) != 0 {
							t.Fatalf("failure emitted %s", out)
						}
						continue
					}
					var expected types.ExportResult
					json.Unmarshal([]byte(tt.body), &expected)
					if asJSON {
						var actual types.ExportResult
						if err := json.Unmarshal(out, &actual); err != nil || actual.Result != expected.Result || actual.SandboxRef != expected.SandboxRef {
							t.Fatalf("JSON %s %v", out, err)
						}
					} else if string(out) != expected.Result+"\n" {
						t.Fatalf("default output=%s", out)
					}
				}
			})
		}
	}
	t.Setenv("E2B_API_KEY", "")
	if err := exportSandboxCmd([]string{"source"}, nil); err == nil {
		t.Fatal("missing authorization accepted")
	}
}
