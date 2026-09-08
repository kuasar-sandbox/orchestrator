package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

func TestPhaseRunTargetsCarryOnlyBuildIdentity(t *testing.T) {
	spec := &configsock.BuildSpec{BuildID: "build,1", RunID: "run/2", Env: map[string]string{"MANIFEST_KEY": "secret-not-for-logs"}}
	args := phaseSandboxRunArgs(spec, "a", "phase-a", "/run/a.yaml", nil, "", false)
	wantFields := "KUASAR_BUILD_ID=build%2C1,KUASAR_RUN_ID=run%2F2"
	for flag, tag := range map[string]string{"--log-to": "sandbox-ctl", "--stdout-to": buildTag, "--stderr-to": buildTag, "--console": consoleTag} {
		found := 0
		for i := 0; i+1 < len(args); i++ {
			if args[i] == flag {
				found++
				if args[i+1] != "journald="+tag+","+wantFields {
					t.Fatalf("%s = %q", flag, args[i+1])
				}
			}
		}
		if found != 1 {
			t.Fatalf("%s occurs %d times", flag, found)
		}
	}
	if strings.Contains(strings.Join(args, " "), "secret-not-for-logs") || strings.Contains(strings.Join(args, " "), "KUASAR_STABLE_ID") || strings.Contains(strings.Join(args, " "), "KUASAR_SANDBOX_ID") {
		t.Fatal("unrelated identity or secret leaked into build targets")
	}
}

func TestBuildExecPreservesTargetsAndDiagnosticCapture(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "sandbox-ctl")
	argsPath := filepath.Join(dir, "args")
	t.Setenv("JOURNAL_TEST_ARGS", argsPath)
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" >\"$JOURNAL_TEST_ARGS\"\nprintf 'expected error tail\\n' >&2\nexit 3\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	spec := &configsock.BuildSpec{BuildID: "build-1", RunID: "run-1"}
	spec.Paths.SandboxCtl = fake
	sb := &phaseSandbox{p: &buildPipeline{spec: spec}, pathID: "a", runRoot: dir}
	for _, target := range []string{buildJournalTarget(spec, buildTag), filepath.Join(dir, "a,b=c%2C"), ""} {
		t.Run(target, func(t *testing.T) {
			out := filepath.Join(dir, "artifact,output%")
			err := sb.exec(context.Background(), execOpts{stdoutTo: out, stderrTo: target, quiet: true}, "/bin/false")
			if err == nil || !strings.Contains(err.Error(), "expected error tail") {
				t.Fatalf("diagnostic capture: %v", err)
			}
			data, err := os.ReadFile(argsPath)
			if err != nil {
				t.Fatal(err)
			}
			args := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
			seen := map[string]string{}
			for i := 0; i+1 < len(args); i++ {
				if args[i] == "--stdout-to" || args[i] == "--stderr-to" || args[i] == "--log-to" {
					seen[args[i]] = args[i+1]
				}
			}
			if seen["--stdout-to"] != out || seen["--stderr-to"] != target {
				t.Fatalf("rewrote output destinations: %#v", seen)
			}
			if _, ok := seen["--log-to"]; ok {
				t.Fatal("exec component stderr was redirected")
			}
			if target == "" && len(seen) != 1 {
				t.Fatal("capture target unexpectedly supplied")
			}
		})
	}
}
