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

func TestWorkspaceCLIUsesControlSocketAndPipesToken(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "ctl.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("NODE_CTL_SOCKET", socket)
	t.Setenv("E2B_API_KEY", "owner")
	const token = "kwt1.source"
	server := http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-KEY") != "owner" {
			t.Error("missing credentials")
		}
		switch r.URL.Path {
		case "/sandboxes/A/workspaces/export":
			if r.Method != "POST" || r.URL.Query().Get("name") != "project" {
				t.Error("bad export")
			}
			out, _ := json.Marshal(types.WorkspaceExportResult{Result: token, DiskReport: artifact.DiskReport{
				Refs: true, DiskRefs: []string{"manifest://" + strings.Repeat("a", 64)}, RemovedRefs: []string{},
			}})
			w.Write(out)
		case "/sandboxes/B/workspaces/attach":
			body, err := io.ReadAll(r.Body)
			if err != nil || string(body) != `{"disk":{"migrationToken":"kwt1.source"}}` || r.URL.Query().Get("name") != "project" {
				t.Errorf("bad attach, %s", body)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unknown path %s", r.URL.Path)
			w.WriteHeader(404)
		}
	})}
	go server.Serve(ln)
	defer server.Close()
	oldInput, oldOutput := os.Stdin, os.Stdout
	defer func() { os.Stdin, os.Stdout = oldInput, oldOutput }()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	os.Stdout = w
	err = workspaceCmd([]string{"export", "A", "--name", "project"})
	w.Close()
	os.Stdout = oldOutput
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil || string(out) != token+"\n" {
		t.Fatalf("unexpected export output %s", out)
	}
	in, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	if _, err := writer.Write(out); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	os.Stdin = in
	if err := workspaceCmd([]string{"attach", "B", "--name", "project", "--token-file", "-"}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCLIRestrictsInput(t *testing.T) {
	for _, test := range []struct {
		disk, tok, content string
		bad                bool
	}{
		{"d", "", "{}", false}, {"d", "", "[]", true}, {"", "t", "", true}, {"", "t", "one\ntwo", true}, {"", "t", "token\n", false}, {"d", "t", "", true},
	} {
		p := filepath.Join(t.TempDir(), "input")
		if err := os.WriteFile(p, []byte(test.content), 0600); err != nil {
			t.Fatal(err)
		}
		disk, file := test.disk, test.tok
		if disk != "" {
			disk = p
		}
		if file != "" {
			file = p
		}
		_, err := workspaceDiskInput(disk, file)
		if (err != nil) != test.bad {
			t.Errorf("disk=%s token=%s: %v", test.disk, test.tok, err)
		}
	}
	t.Setenv("E2B_API_KEY", "owner")
	for _, args := range [][]string{{}, {"list", "."}, {"export", "A", "--timeout", "0"}, {"attach", "B", "--name", "w", "extra"}, {"attach", "B"}, {"list", "B/B"}} {
		if err := workspaceCmd(args); err == nil || strings.Contains(fmt.Sprint(err), "reach orchestrator") {
			t.Errorf("invalid args reached server: %v, %v", args, err)
		}
	}
}

func TestWorkspaceCLIJSONMatchesAPIReport(t *testing.T) {
	t.Setenv("E2B_API_KEY", "owner")
	root := "manifest://" + strings.Repeat("a", 64)
	valid := `{"result":"kwt1.test","refs":true,"diskRefs":["` + root + `"],"removedRefs":[]}`
	for _, test := range []struct {
		name, body string
		bad        bool
	}{
		{"complete", valid, false},
		{"legacy token envelope", `{"token":"kwt1.test"}`, true},
		{"unknown with delta", `{"result":"kwt1.test","refs":false,"removedRefs":[]}`, true},
		{"diskRef", strings.Replace(valid, `"diskRefs":`, `"diskRef":"`+root+`","diskRefs":`, 1), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			socket := filepath.Join(t.TempDir(), "ctl.sock")
			listener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, test.body) })}
			go server.Serve(listener)
			defer server.Close()
			out, err := captureStdout(t, func() error {
				return workspaceCmd([]string{"export", "A", "--name", "project", "--json", "--socket", socket})
			})
			if (err != nil) != test.bad {
				t.Fatalf("output=%q err=%v", out, err)
			}
			if test.bad {
				if out != "" {
					t.Fatalf("failed command emitted output: %s", out)
				}
				return
			}
			if out != valid+"\n" {
				t.Fatalf("CLI changed report: %s", out)
			}
		})
	}
}
