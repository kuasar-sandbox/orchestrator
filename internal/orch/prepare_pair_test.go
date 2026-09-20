package orch

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

func TestPreparePairReplayAndStaleSourceRejection(t *testing.T) {
	for _, name := range []string{"initial S-only", "persisted memory", "persisted cold"} {
		t.Run(name, func(t *testing.T) {
			initial := name == "initial S-only"
			o := migrationOrchestrator(t, t.TempDir(), []byte("runtime"))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			sb := migrationSandbox(t, t.TempDir(), "prepare-pair", strings.Repeat("4", 64), "manifest://"+strings.Repeat("a", 64))
			parent := "file://" + t.TempDir()
			location, err := reflocation.Resolve(parent, "source")
			if err != nil {
				t.Fatal(err)
			}
			dir := location.Path
			if err := os.MkdirAll(filepath.Clean(dir), 0700); err != nil {
				t.Fatal(err)
			}
			response, err := capturePairResponse(ctx, ctl.Request{Mode: "local", OutDir: dir}, types.CaptureSnapshot)
			if err != nil {
				t.Fatal(err)
			}
			pair := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: response.SnapshotRef + "@location:source", SandboxRef: response.SandboxRef + "@location:source"}
			sb.ResumeSource = pair
			sb.TemplateID = types.TemplateID{Profile: sb.Profile, Kind: types.KindSnp, Ref: pair.Ref}.String()
			sb.State = types.StateStarting
			sb.LaunchMode = types.LaunchMemory
			if name == "persisted cold" {
				sb.LaunchMode = types.LaunchCold
			}
			sb.RunID = "matching-run"
			kind := launchResume
			if initial {
				sb.ResumeSource = types.ResumeSource{}
				kind = launchCreate
			}
			if err := o.st.Put(ctx, sb); err != nil {
				t.Fatal(err)
			}
			attempt, err := o.launches.Claim(ctx, sb.ID, kind)
			if err != nil {
				t.Fatal(err)
			}
			defer o.launches.Finish(attempt, nil)
			attempt.SetRunID(sb.RunID)
			input := configsock.ArtifactPrepareSpec{RunID: sb.RunID, RootSourceKind: string(pair.Kind), RootRef: pair.Ref, RelativeDir: dir, RefLocationParent: parent, LaunchMode: string(sb.LaunchMode)}
			if !initial {
				input.RootSandboxRef = pair.SandboxRef
			}
			prepared, err := taskartifact.Prepare(ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			summary := prepared.Summary
			if summary.RootSource != pair {
				t.Fatalf("producer/prepare pair=%+v want=%+v", summary.RootSource, pair)
			}
			if err := os.RemoveAll(dir); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, err := o.CompleteSandboxPrepare(ctx, sb.ID, sb.RunID, summary); done <- err }()
			if _, err := attempt.WaitPrepare(ctx); err != nil {
				t.Fatal(err)
			}
			conflict := summary
			conflict.RootSource.SandboxRef = "manifest://" + strings.Repeat("c", 64)
			if _, err := o.CompleteSandboxPrepare(ctx, sb.ID, sb.RunID, conflict); err == nil || !configsock.IsArtifactPrepareRejection(err) {
				t.Fatalf("same digest/different E completion=%v", err)
			}
			if _, err := o.CompleteSandboxPrepare(ctx, sb.ID, "stale-run", summary); err == nil || !configsock.IsArtifactPrepareRejection(err) {
				t.Fatalf("stale run=%v", err)
			}
			final := &configsock.LaunchSpec{Exec: "sandbox-ctl"}
			attempt.PublishFinalSpec(final)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if got, err := o.CompleteSandboxPrepare(ctx, sb.ID, sb.RunID, summary); err != nil || !reflect.DeepEqual(got, final) {
				t.Fatalf("same pair retry=%+v %v", got, err)
			}
			if changed, err := o.st.CommitPreparedRunning(ctx, sb, pair); err != nil || !changed {
				t.Fatalf("initial source commit=%t %v", changed, err)
			}
			current, err := o.st.Get(ctx, sb.ID)
			if err != nil || current.ResumeSource != pair {
				t.Fatalf("accepted pair=%+v %v", current, err)
			}
			// The same S and RunID cannot authorize a completion after E changes.
			current.ResumeSource.SandboxRef = conflict.RootSource.SandboxRef
			if err := o.st.Put(ctx, current); err != nil {
				t.Fatal(err)
			}
			if _, err := o.CompleteSandboxPrepare(ctx, sb.ID, sb.RunID, summary); err == nil || !configsock.IsArtifactPrepareRejection(err) {
				t.Fatalf("stale source E=%v", err)
			}
		})
	}
}
