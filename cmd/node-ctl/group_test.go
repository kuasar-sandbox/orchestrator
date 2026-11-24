package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
)

func serveGroupCLI(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "ctl.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/groups" || r.Header.Get("X-API-KEY") != "group-owner" {
			t.Errorf("unexpected request %s, missing business credentials", r.URL.Path)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		handler(w, r)
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	t.Setenv("E2B_API_KEY", "group-owner")
	t.Setenv("NODE_CTL_SOCKET", socket)
	return socket
}

func writeGroupCLIFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestGroupCLIApplyUsesSingleCAS(t *testing.T) {
	for _, test := range []struct {
		name, revision                  string
		getStatus, putStatus, wantReads int
		version                         int64
	}{
		{"existing", "", 200, 200, 1, 5},
		{"new", "", 404, 200, 1, 0},
		{"tombstone", "", 404, 200, 1, 9},
		{"explicit", `,"expected_version":3`, 200, 200, 0, 3},
		{"conflict", "", 200, 409, 1, 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			var mu sync.Mutex
			reads, writes := 0, 0
			socket := serveGroupCLI(t, func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.URL.Query().Get("group") != "/teams/中国 a&b" {
					t.Errorf("group name was not round-tripped: %s", r.URL.RawQuery)
				}
				switch r.Method {
				case http.MethodGet:
					reads++
					w.WriteHeader(test.getStatus)
					fmt.Fprintf(w, `{"version":%d}`, test.version)
				case http.MethodPut:
					writes++
					var req nodegroup.PutRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ExpectedVersion != test.version || req.Context.ProjectID != "project" {
						t.Errorf("incorrect conditional PUT: %+v, %v", req, err)
					}
					w.WriteHeader(test.putStatus)
					if test.putStatus == 409 {
						fmt.Fprint(w, `{"code":409,"message":"Group version conflict","version":6}`)
						return
					}
					json.NewEncoder(w).Encode(nodegroup.Result{Group: r.URL.Query().Get("group"), Version: test.version + 1})
				default:
					t.Errorf("unexpected mutation %s", r.Method)
				}
			})
			file := writeGroupCLIFile(t, t.TempDir(), "group.json", `{"group":"/teams/中国 a&b","context":{"project_id":"project"}`+test.revision+`}`)
			var out bytes.Buffer
			err := groupCmd([]string{"apply", "--socket", socket, "-f", file}, &out)
			if (err != nil) != (test.putStatus == 409) {
				t.Fatalf("apply = %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if reads != test.wantReads || writes != 1 {
				t.Fatalf("GET=%d PUT=%d; no retry/rebase permitted", reads, writes)
			}
			if test.putStatus == 200 && !strings.Contains(out.String(), `"version":`) {
				t.Fatalf("missing result: %s", &out)
			}
		})
	}
}

func TestGroupCLIRejectsInvalidRevisionBeforePut(t *testing.T) {
	for _, response := range []struct {
		status int
		body   string
	}{
		{404, `{"message":"Group not found"}`},
		{403, `{"message":"API key is not allowed"}`},
		{200, `{"version":null}`},
		{200, `{"version":-1}`},
		{200, `{"version":1,"version":2}`},
	} {
		t.Run(response.body, func(t *testing.T) {
			serveGroupCLI(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Error("invalid revision must not authorize a write")
				}
				w.WriteHeader(response.status)
				fmt.Fprint(w, response.body)
			})
			file := writeGroupCLIFile(t, t.TempDir(), "group.json", `{"group":"g","context":{}}`)
			if err := groupCmd([]string{"apply", "-f", file}, &bytes.Buffer{}); err == nil {
				t.Fatal("expected failure")
			}
		})
	}
}

func TestGroupCLIValidatesFilesWithoutInstallingKeys(t *testing.T) {
	t.Setenv("E2B_API_KEY", "group-owner")
	for _, raw := range []string{
		`{"group":"g"}`, `{"group":"g","context":null}`, `{"group":"g","context":{},"expected_version":null}`,
		`{"group":"g","context":{},"expected_version":-1}`, `{"group":"g","context":{},"api_secret":"private"}`,
		`{"group":"g","group":"other","context":{}}`, `{"group":"g","context":{"image_repo":"a","image_repo":"b"}}`,
		`{"group":"g","context":{"selector":{}}}`, `{"group":"g","context":{}} {}`,
		`{"group":"g","context":{"metadata":{"oversized":"` + strings.Repeat("x", 1<<20) + `"}}}`,
	} {
		file := writeGroupCLIFile(t, t.TempDir(), "bad.json", raw)
		if _, err := applyGroupFile("/nonexistent-group-test.sock", "group-owner", file); err == nil || strings.Contains(err.Error(), "reach orchestrator") {
			t.Fatalf("invalid input reached the network: %v", err)
		}
	}
}

func TestGroupCLIDirectoryReportsPartialFailureWithoutRollback(t *testing.T) {
	var mu sync.Mutex
	var groups []string
	serveGroupCLI(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method != http.MethodPut {
			t.Errorf("unexpected %s", r.Method)
		}
		group := r.URL.Query().Get("group")
		groups = append(groups, group)
		if group == "conflict" {
			w.WriteHeader(409)
			fmt.Fprint(w, `{"message":"Group version conflict"}`)
			return
		}
		json.NewEncoder(w).Encode(nodegroup.Result{Group: group, Version: 1})
	})
	dir := t.TempDir()
	writeGroupCLIFile(t, dir, "01.json", `{"group":"first","context":{},"expected_version":0}`)
	writeGroupCLIFile(t, dir, "02.json", `{"group":"conflict","context":{},"expected_version":0}`)
	writeGroupCLIFile(t, dir, "03.json", `{"group":"last","context":{},"expected_version":0}`)
	writeGroupCLIFile(t, dir, "ignore.txt", "ignored")
	var out bytes.Buffer
	err := groupCmd([]string{"apply", "-f", dir}, &out)
	if err == nil || !strings.Contains(err.Error(), "02.json") {
		t.Fatalf("missing per-file failure: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(groups, ",") != "first,conflict,last" || strings.Count(out.String(), "\n") != 2 {
		t.Fatalf("directory result: groups=%v output=%s", groups, &out)
	}
}
