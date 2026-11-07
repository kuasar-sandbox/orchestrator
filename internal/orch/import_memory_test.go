package orch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func installSnapshotRuntimeAdmissionStub(t *testing.T, o *Orchestrator, status string) {
	t.Helper()
	raw, err := json.Marshal(configsock.SnapshotRuntimeCheckResult{Status: status})
	if err != nil {
		t.Fatal(err)
	}
	installSnapshotRuntimeHelperScript(t, o, "[ \"$1\" = check-snapshot-runtime ] || exit 64\n/bin/cat >/dev/null\nprintf '%s\\n' '"+string(raw)+"'\n")
}

func installSnapshotRuntimeHelperScript(t *testing.T, o *Orchestrator, body string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node-ctl")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nset -eu\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
	o.SetExecutables(configresolve.ExecutablesForNodeCtl(path))
}

func TestImportMemorySelectionPersistsChosenSource(t *testing.T) {
	for _, tc := range []struct {
		name    string
		source  types.ResumeSourceKind
		memory  *bool
		want    types.ResumeSourceKind
		wantErr error
	}{
		{"S default", types.ResumeSourceSnapshot, nil, types.ResumeSourceSnapshot, nil},
		{"S true", types.ResumeSourceSnapshot, resumeMemoryOption(true), types.ResumeSourceSnapshot, nil},
		{"S false", types.ResumeSourceSnapshot, resumeMemoryOption(false), types.ResumeSourceSandbox, nil},
		{"E default", types.ResumeSourceSandbox, nil, types.ResumeSourceSandbox, nil},
		{"E false", types.ResumeSourceSandbox, resumeMemoryOption(false), types.ResumeSourceSandbox, nil},
		{"E true", types.ResumeSourceSandbox, resumeMemoryOption(true), "", types.ErrMemoryUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			o := migrationOrchestrator(t, dir, []byte("legacy-runtime"))
			source := migrationSandbox(t, dir, "source", strings.Repeat("6", 64), "manifest://"+strings.Repeat("b", 64))
			if tc.source == types.ResumeSourceSandbox {
				source.ResumeSource = types.ResumeSource{Kind: tc.source, Ref: source.ResumeSource.SandboxRef}
			}
			pair := store.KeyPair{APISecret: source.APISecret, ManifestKey: source.ManifestKey}
			token, err := o.mintSandboxToken(source, source.ResumeSource)
			if err != nil {
				t.Fatal(err)
			}
			// No legacy hash may be read at import. E-only must not invoke the helper.
			if err := os.Remove(o.runtimeFileFor(source.Profile)); err != nil {
				t.Fatal(err)
			}
			status := configsock.SnapshotRuntimeCheckError
			if tc.want == types.ResumeSourceSnapshot {
				status = configsock.SnapshotRuntimeCompatible
			}
			installSnapshotRuntimeAdmissionStub(t, o, status)
			got, err := o.importSandboxWithKey(context.Background(), pair, token, "target", migrationtoken.Expectations{ResumeSource: source.ResumeSource}, nil, 0, api.ImportOptions{Memory: tc.memory})
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("error=%v, want %v", err, tc.wantErr)
				}
				if row, _ := o.st.Get(context.Background(), "target"); row != nil {
					t.Fatal("rejected import inserted a row")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			expected := source.ResumeSource
			if tc.want == types.ResumeSourceSandbox && expected.Kind == types.ResumeSourceSnapshot {
				expected = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: expected.SandboxRef}
			}
			if got.ResumeSource != expected || got.State != types.StatePaused || got.AutoPauseMemory != source.AutoPauseMemory {
				t.Fatalf("imported state=%+v want source=%+v", got, expected)
			}
			row, err := o.st.Get(context.Background(), "target")
			if err != nil || row.ResumeSource != expected {
				t.Fatalf("durable source=%+v err=%v", row, err)
			}
			payload, err := migrationtoken.Open(migrationtoken.KeyMaterial{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey}, token)
			if err != nil || payload.ResumeSourceKind != source.ResumeSource.Kind || payload.ResumeSourceRef != source.ResumeSource.Ref || payload.ResumeSandboxRef != source.ResumeSource.SandboxRef {
				t.Fatal("import changed the original token source")
			}
		})
	}
}

func TestImportSnapshotRuntimeFailureIsSynchronousAndInsertFree(t *testing.T) {
	for _, tc := range []struct {
		status       string
		incompatible bool
	}{
		{configsock.SnapshotRuntimeIncompatible, true}, {configsock.SnapshotRuntimeCheckError, false},
	} {
		t.Run(tc.status, func(t *testing.T) {
			dir := t.TempDir()
			o := migrationOrchestrator(t, dir, []byte("runtime"))
			source := migrationSandbox(t, dir, "source", strings.Repeat("6", 64), "manifest://"+strings.Repeat("b", 64))
			token, err := o.mintSandboxToken(source, source.ResumeSource)
			if err != nil {
				t.Fatal(err)
			}
			installSnapshotRuntimeAdmissionStub(t, o, tc.status)
			_, err = o.importSandboxWithKey(context.Background(), store.KeyPair{APISecret: source.APISecret, ManifestKey: source.ManifestKey}, token, "target", migrationtoken.Expectations{}, nil, 0, api.ImportOptions{})
			if err == nil || errors.Is(err, migrationtoken.ErrIncompatible) != tc.incompatible {
				t.Fatalf("error classification=%v", err)
			}
			if row, _ := o.st.Get(context.Background(), "target"); row != nil {
				t.Fatal("failed admission inserted target")
			}
		})
	}
}

func TestConnectNewColdImportFailureKeepsEOnly(t *testing.T) {
	for _, withMMDS := range []bool{false, true} {
		t.Run(fmt.Sprintf("mmds=%t", withMMDS), func(t *testing.T) {
			runtimePath := filepath.Join(t.TempDir(), "runtime")
			if err := os.WriteFile(runtimePath, []byte("runtime"), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg := &config.Config{}
			cfg.Sandbox.Boot.Runtime = runtimePath
			gate := make(chan struct{})
			modes := make(chan string, 1)
			lc := &countingLauncher{connectGate: gate, artifactLaunchModes: modes, readinessWire: []byte("ready\ncontrol_ready\n")}
			o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
			installSnapshotRuntimeAdmissionStub(t, o, configsock.SnapshotRuntimeIncompatible)
			mk := strings.Repeat("d", 64)
			secret, key := defaultTestCredentials(t, mk)
			if _, err := o.st.AddKeyPair(ctx, store.KeyPair{APISecret: secret, ManifestKey: mk}, "", 0, ""); err != nil {
				t.Fatal(err)
			}
			original := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("a", 64), SandboxRef: "manifest://" + strings.Repeat("b", 64)}
			source := &types.Sandbox{ID: "source", Profile: types.ProfileBare, State: types.StatePaused, ResumeSource: original,
				TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("c", 64)}.String(),
				APISecret:  secret, ManifestKey: mk, CreatedUnix: 1, RunDir: nodepath.SandboxRunDir(cfg.Paths.RunRoot, "source"), BaseDir: nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, "source")}
			materializeTestSandboxCredentials(t, source)
			if err := o.st.Put(ctx, source); err != nil {
				t.Fatal(err)
			}
			token, err := o.mintSandboxToken(source, source.ResumeSource)
			if err != nil {
				t.Fatal(err)
			}
			var accepted *types.Sandbox
			options := api.ConnectOptions{Memory: resumeMemoryOption(false)}
			if withMMDS {
				accepted, err = o.ConnectWithMMDS(ctx, "target", key, token, options, nil, nil)
			} else {
				accepted, err = o.Connect(ctx, "target", key, token, options)
			}
			if err != nil || accepted == nil || accepted.LaunchMode != types.LaunchCold {
				t.Fatalf("Connect=%+v %v", accepted, err)
			}
			expected := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: original.SandboxRef}
			if accepted.ResumeSource != expected {
				t.Fatalf("new import did not persist E: %+v", accepted.ResumeSource)
			}
			attempt, found := o.launches.Lookup("target")
			if !found {
				t.Fatal("no accepted attempt")
			}
			close(gate)
			select {
			case mode := <-modes:
				if mode != string(types.LaunchCold) {
					t.Fatal(mode)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cold launch not attempted")
			}
			if err := attempt.wait(ctx); err == nil {
				t.Fatal("malformed readiness unexpectedly succeeded")
			}
			paused := waitForSandbox(t, o, ctx, "target", func(sb *types.Sandbox) bool { return sb.State == types.StatePaused }, "failed cold import")
			if paused.ResumeSource != expected || paused.LaunchMode != "" {
				t.Fatalf("cold failure source=%+v", paused)
			}
			if originalRow, _ := o.st.Get(ctx, "source"); originalRow == nil || originalRow.ResumeSource != original {
				t.Fatal("import modified original source")
			}
			if _, err := o.Connect(ctx, "target", key, "", api.ConnectOptions{Memory: resumeMemoryOption(true)}); !errors.Is(err, types.ErrMemoryUnavailable) {
				t.Fatalf("E-only memory request error=%v", err)
			}
		})
	}
}

func TestClusterNewColdImportAndConcurrentWinnerSource(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	cmd := fixture.command("cold-target", fixture.token)
	cmd.Memory = resumeMemoryOption(false)
	installSnapshotRuntimeAdmissionStub(t, fixture.o, configsock.SnapshotRuntimeIncompatible)
	got, err := fixture.o.prepareClusterConnect(context.Background(), cmd, 0)
	expected := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: fixture.source.ResumeSource.SandboxRef}
	if err != nil || got.ResumeSource != expected {
		t.Fatalf("cluster cold import=%+v %v", got, err)
	}
	installSnapshotRuntimeAdmissionStub(t, fixture.o, configsock.SnapshotRuntimeCompatible)
	type result struct {
		sb  *types.Sandbox
		err error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for _, memory := range []*bool{nil, resumeMemoryOption(false)} {
		go func(memory *bool) {
			<-start
			cmd := fixture.command("race-target", fixture.token)
			cmd.Memory = memory
			sb, err := fixture.o.prepareClusterConnect(context.Background(), cmd, 0)
			results <- result{sb, err}
		}(memory)
	}
	close(start)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("race errors=%v, %v", first.err, second.err)
	}
	stored, err := fixture.o.st.Get(context.Background(), "race-target")
	if err != nil || stored == nil || first.sb.ResumeSource != stored.ResumeSource || second.sb.ResumeSource != stored.ResumeSource {
		t.Fatal("concurrent import overwrote the winner source")
	}
	if stored.ResumeSource != expected && stored.ResumeSource != fixture.source.ResumeSource {
		t.Fatalf("mixed source=%+v", stored.ResumeSource)
	}
}

func TestSnapshotRuntimeHelperIsolatesTenantEnvironmentAndCancellation(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	t.Setenv("MANIFEST_KEY", "conductor-value")
	records := t.TempDir()
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	installSnapshotRuntimeHelperScript(t, o, "[ \"$1\" = check-snapshot-runtime ]\n/bin/cat > "+quote(records)+"/\"$MANIFEST_KEY\"\nprintf '%s\\n' '{\"status\":\"compatible\"}'\n")
	results := make(chan error, 2)
	sources := make([]*types.Sandbox, 2)
	for i, key := range []string{strings.Repeat("6", 64), strings.Repeat("7", 64)} {
		sources[i] = migrationSandbox(t, dir, fmt.Sprintf("source-%d", i), key, "manifest://"+strings.Repeat("b", 64))
		go func(sb *types.Sandbox) { results <- o.checkImportedSnapshotRuntime(context.Background(), sb) }(sources[i])
	}
	for range sources {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if os.Getenv("MANIFEST_KEY") != "conductor-value" {
		t.Fatal("shared conductor key changed")
	}
	for _, source := range sources {
		raw, err := os.ReadFile(filepath.Join(records, source.ManifestKey))
		if err != nil {
			t.Fatal("tenant helper did not receive its key")
		}
		var req configsock.SnapshotRuntimeCheckRequest
		if err := json.Unmarshal(raw, &req); err != nil || req.Source != source.ResumeSource || !strings.HasPrefix(req.Runtime, "file://") {
			t.Fatalf("helper source binding changed: %v", err)
		}
	}
	installSnapshotRuntimeHelperScript(t, o, "exec /bin/sleep 30\n")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := o.checkImportedSnapshotRuntime(ctx, sources[0])
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Fatalf("helper did not inherit cancellation: %v", err)
	}
}

func TestImportAdmissionBudgetUnblocksNodeLinkCommands(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	installSnapshotRuntimeHelperScript(t, fixture.o, "exec /bin/sleep 30\n")
	// The node-link passes its stream context, not the registry's ACK timeout.
	ctx := context.Background()
	started := time.Now()
	cmd := fixture.command("hung-target", fixture.token)
	ack := fixture.o.HandleCommand(ctx, cmd)
	elapsed := time.Since(started)
	if ack.Status != routesync.AckRejected || ack.HTTPStatus == 409 {
		t.Fatalf("hung admission ACK=%+v", ack)
	}
	if elapsed > snapshotRuntimeAdmissionBudget+750*time.Millisecond {
		t.Fatalf("admission exceeded local budget: %s", elapsed)
	}
	if row, _ := fixture.o.st.Get(ctx, cmd.SID); row != nil {
		t.Fatal("timed out admission inserted target")
	}
	next := fixture.command("next-target", fixture.token)
	next.Memory = resumeMemoryOption(false)
	started = time.Now()
	ack = fixture.o.HandleCommand(ctx, next)
	if ack.Status != routesync.AckAccepted || time.Since(started) > time.Second {
		t.Fatalf("subsequent command blocked: %+v", ack)
	}
}

func TestSnapshotRuntimeHelperBoundsOutput(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	source := migrationSandbox(t, dir, "source", strings.Repeat("6", 64), "manifest://"+strings.Repeat("b", 64))
	output := filepath.Join(t.TempDir(), "large-output")
	if err := os.WriteFile(output, []byte(strings.Repeat("x", 32<<10)), 0o600); err != nil {
		t.Fatal(err)
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	installSnapshotRuntimeHelperScript(t, o, "/bin/cat "+quote(output)+"\n")
	err := o.checkImportedSnapshotRuntime(context.Background(), source)
	if err == nil || !strings.Contains(err.Error(), "result too large") || errors.Is(err, migrationtoken.ErrIncompatible) {
		t.Fatalf("oversize output error=%v", err)
	}
}
