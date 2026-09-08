package orch

import (
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func decodeJournalTarget(t *testing.T, target string) (string, map[string]string) {
	t.Helper()
	parts := strings.Split(target, ",")
	if !strings.HasPrefix(parts[0], "journald=") {
		t.Fatalf("not a journal target: %q", target)
	}
	fields := map[string]string{}
	for _, part := range parts[1:] {
		key, value, ok := strings.Cut(part, "=")
		decoded, err := url.PathUnescape(value)
		if !ok || err != nil {
			t.Fatalf("invalid field %q: %v", part, err)
		}
		if _, exists := fields[key]; exists {
			t.Fatalf("duplicate field %q", key)
		}
		fields[key] = decoded
	}
	return strings.TrimPrefix(parts[0], "journald="), fields
}

func TestSandboxLaunchJournalIdentity(t *testing.T) {
	o := testOrchCfg(t, &config.Config{})
	o.vs = stubVS{}
	for _, tc := range []struct {
		name, id, stable, run string
		cluster               *types.ClusterSandboxContext
	}{
		{name: "standalone fallback", id: "local-1", run: "run-1"},
		{name: "standalone independent", id: "local-1", stable: "logical", run: "run-1"},
		{name: "same node resume", id: "local-1", stable: "logical", run: "run-2"},
		{name: "import new node id", id: "local-2", stable: "logical", run: "run-3"},
		{name: "cluster", id: "worker-42-g1", stable: "worker-42", run: "run-c", cluster: &types.ClusterSandboxContext{}},
		{name: "opaque escaping", id: "local", stable: "a,b%2C+c=d", run: "run-4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sb := &types.Sandbox{ID: tc.id, StableIDValue: tc.stable, RunID: tc.run, Cluster: tc.cluster, RunDir: "/run/test", ManifestKey: "private-not-a-log-field"}
			spec := o.sandboxFinalLaunchSpec(sb, types.TemplateID{}, sandboxcfg.SandboxSpec{}, nil)
			stable := tc.stable
			if stable == "" {
				stable = tc.id
			}
			want := map[string]string{"KUASAR_STABLE_ID": stable, "KUASAR_SANDBOX_ID": tc.id, "KUASAR_RUN_ID": tc.run}
			for flag, tag := range map[string]string{"--log-to": "sandbox-ctl", "--stdout-to": configsock.RunnerLogTag, "--stderr-to": configsock.RunnerLogTag, "--console": configsock.ConsoleTag} {
				count := 0
				for i := 0; i+1 < len(spec.Args); i++ {
					if spec.Args[i] != flag {
						continue
					}
					count++
					gotTag, fields := decodeJournalTarget(t, spec.Args[i+1])
					if gotTag != tag || !reflect.DeepEqual(fields, want) {
						t.Fatalf("%s = %q, fields=%v want=%v", flag, spec.Args[i+1], fields, want)
					}
				}
				if count != 1 {
					t.Fatalf("%s occurs %d times in %v", flag, count, spec.Args)
				}
			}
			if got := sandboxTaskEnv(sb); !reflect.DeepEqual(got, map[string]string{"MANIFEST_KEY": sb.ManifestKey}) {
				t.Fatalf("obsolete log identity environment: %#v", got)
			}
			if strings.Contains(strings.Join(spec.Args, " "), sb.ManifestKey) {
				t.Fatal("secret leaked to launch arguments")
			}
		})
	}
}

func TestSandboxJournalTargetsDoNotInheritAnotherObject(t *testing.T) {
	t.Setenv("KUASAR_STABLE_ID", "ambient")
	first := &types.Sandbox{ID: "first", StableIDValue: "logical", RunID: "one"}
	second := &types.Sandbox{ID: "second", RunID: "two"}
	old := sandboxJournalTarget("sandbox", first)
	_ = sandboxJournalTarget("sandbox", second)
	first.RunID = "next"
	_, oldFields := decodeJournalTarget(t, old)
	_, newFields := decodeJournalTarget(t, sandboxJournalTarget("sandbox", first))
	_, otherFields := decodeJournalTarget(t, sandboxJournalTarget("sandbox", second))
	if oldFields["KUASAR_RUN_ID"] != "one" || newFields["KUASAR_RUN_ID"] != "next" || otherFields["KUASAR_STABLE_ID"] != "second" {
		t.Fatalf("identity leaked between targets: %v / %v / %v", oldFields, newFields, otherFields)
	}
}
