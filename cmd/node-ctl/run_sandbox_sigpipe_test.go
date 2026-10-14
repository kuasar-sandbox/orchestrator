package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxsdk"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxsdk/journalio"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRunAssignedSandboxSIGPIPE(t *testing.T) {
	const childEnv = "KUASAR_RUNNER_SIGPIPE_TEST"
	if os.Getenv(childEnv) == "1" {
		runSandboxSIGPIPEChild(t)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Reset SIGPIPE before exec so an Ignore in an earlier parent test cannot
	// mask the production policy. GNU env also unblocks the specified signal.
	cmd := exec.CommandContext(ctx, "env", "--default-signal=PIPE", os.Args[0], "-test.run=^TestRunAssignedSandboxSIGPIPE$", "-test.count=1")
	cmd.Env = append(os.Environ(), childEnv+"=1")
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = writer // A real fd 2 pipe, with no reader before exec.
	var output bytes.Buffer
	cmd.Stdout = &output
	err = cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("child timed out: %v; stdout=%q", err, output.String())
	}
	if err != nil {
		t.Fatalf("runner policy: %v; stdout=%q", err, output.String())
	}
	if want := "diagnostic\nruntime cleanup\nreported completion\nrunner cleanup\nPASS\n"; output.String() != want {
		t.Fatalf("stdout=%q, want %q", output.String(), want)
	}
}

func runSandboxSIGPIPEChild(t *testing.T) {
	spec := sdkLaunchSpec(t)
	cgroup, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cgroup.Close()
	ready, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ready.Close()
	cleaned := false
	reported := false
	// This is the common runner entry reached by runSandboxWithTask in both
	// node-ctl and runnerE2EProcess. Only host setup and runtime are replaced.
	err = runAssignedSandbox("unused.pid", "unused.sock", "run-1", runSandboxOps{
		lockPidfile:   func(string) error { return nil },
		prepareCgroup: func() (*os.File, error) { return cgroup, nil },
		waitAssignment: func(context.Context, string, string, string) (string, error) {
			return "sid", nil
		},
		connectReady: func(string) (*os.File, error) { return ready, nil },
		launchTask: func(_, sid, runID string, ready, cgroup *os.File) error {
			return executeSandboxAndReportWith(context.Background(), sid, runID, spec, types.ResumeSource{}, nil, map[string]string{"MANIFEST_KEY": ""}, cgroup, ready,
				func(context.Context, *sandboxsdk.Input, *os.File, *sandboxsdk.Readiness) (int, error) {
					defer func() {
						cleaned = true
						fmt.Println("runtime cleanup")
					}()
					// Use the SDK's actual journal writer and process-stderr fallback.
					writer, err := journalio.New(journalio.Target{Tag: "sigpipe-regression"}, os.Stderr, "")
					if err != nil {
						t.Fatal(err)
					}
					defer writer.Close()
					var limit syscall.Rlimit
					if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
						t.Fatal(err)
					}
					// Fresh subprocess: journald's lazy socket has not been opened.
					// Existing stdout/stderr work, but socket creation must fail.
					if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{Cur: 0, Max: limit.Max}); err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
							t.Fatal(err)
						}
					}()
					fmt.Println("diagnostic")
					log.New(writer, "", 0).Print("runtime diagnostic")
					return 0, nil
				}, func(result configsock.SandboxExecutionResult) error {
					if !cleaned || result.SID != "sid" || result.RunID != "run-1" || result.Stage != types.SandboxResultRun || result.ExitCode == nil || *result.ExitCode != 0 || result.Error != "" || result.Signal != "" {
						t.Fatalf("cleanup=%v, completion=%+v", cleaned, result)
					}
					if _, err := ready.Stat(); !errors.Is(err, os.ErrClosed) {
						t.Fatalf("readiness still open at report: %v", err)
					}
					reported = true
					fmt.Println("reported completion")
					return nil
				})
		},
	})
	if err != nil || !reported {
		t.Fatalf("runner err=%v, reported=%v", err, reported)
	}
	for _, file := range []*os.File{ready, cgroup} {
		if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("borrowed descriptor still open: %v", err)
		}
	}
	fmt.Println("runner cleanup")
}
