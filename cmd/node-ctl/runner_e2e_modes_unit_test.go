package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxsdk"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRunnerE2ERecordsPreparedSource(t *testing.T) {
	work := t.TempDir()
	for _, source := range []types.ResumeSource{
		{Kind: types.ResumeSourceSandbox, Ref: "file:///prepared/root.sandbox"},
		{Kind: types.ResumeSourceSnapshot, Ref: "file:///prepared/memory.snapshot"},
	} {
		args := []string{"run", "--sandbox-id", "sb"}
		if err := recordRunnerE2ELaunch(work, configsock.LaunchSpec{Args: args}, source, map[string]string{"z": "file:///z", "a": "file:///a"}); err != nil {
			t.Fatal(err)
		}
	}
	f, err := os.Open(filepath.Join(work, "run-argv.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	for _, mode := range []string{"--from", "--restore"} {
		var args []string
		if err := decoder.Decode(&args); err != nil {
			t.Fatal(err)
		}
		if args[3] != mode || !reflect.DeepEqual(args[5:], []string{"--ref-location", "a=file:///a", "--ref-location", "z=file:///z"}) {
			t.Fatalf("incorrect source observation: %v", args)
		}
	}
}

func TestRunnerE2EFaultsUseActualReadinessWire(t *testing.T) {
	for _, mode := range []string{"runtime-wire-failure", "envd-init-failure", "hold", "park"} {
		t.Run(mode, func(t *testing.T) {
			work := t.TempDir()
			if err := os.WriteFile(filepath.Join(work, "inject-sandbox-run"), []byte(mode), 0600); err != nil {
				t.Fatal(err)
			}
			// Keep the Unix socket short even when TMPDIR is a long build path.
			runRoot, err := os.MkdirTemp("/tmp", "e445-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(runRoot) })
			input := &sandboxsdk.Input{RunRoot: runRoot, PathID: "sb"}
			if err := os.Mkdir(filepath.Join(runRoot, "sb"), 0700); err != nil {
				t.Fatal(err)
			}
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			defer w.Close()
			ready := sandboxsdk.NewReadiness(w)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan int, 1)
			go func() {
				code, _ := executeRunnerE2E(ctx, work, input, nil, w, ready)
				_ = ready.Close()
				done <- code
			}()
			if mode == "hold" || mode == "park" {
				cancel()
			}
			_ = r.SetReadDeadline(time.Now().Add(5 * time.Second))
			wire, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			wantCode := 44
			switch mode {
			case "runtime-wire-failure":
				wantCode = 42
				if string(wire) != "control_ready\ninvalid_runtime_event\n" {
					t.Fatalf("wire=%q", wire)
				}
			case "envd-init-failure":
				wantCode = 43
				if string(wire) != "control_ready\nready\n" {
					t.Fatalf("wire=%q", wire)
				}
				transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", filepath.Join(runRoot, "sb", "envd.sock"))
				}}
				defer transport.CloseIdleConnections()
				client := &http.Client{Transport: transport, Timeout: time.Second}
				response, err := client.Post("http://envd/init", "application/json", nil)
				if err != nil {
					t.Fatal(err)
				}
				_ = response.Body.Close()
				if response.StatusCode != 500 {
					t.Fatalf("status=%d", response.StatusCode)
				}
				cancel()
			default:
				if len(wire) != 0 {
					t.Fatalf("held launch emitted readiness: %q", wire)
				}
			}
			select {
			case code := <-done:
				if code != wantCode {
					t.Fatalf("code=%d, want %d", code, wantCode)
				}
			case <-ctx.Done():
				// Cancellation was intentional; still require bounded cleanup.
				select {
				case code := <-done:
					if code != wantCode {
						t.Fatalf("code=%d, want %d", code, wantCode)
					}
				case <-time.After(time.Second):
					t.Fatal("fault owner failed to clean up")
				}
			}
		})
	}
}
