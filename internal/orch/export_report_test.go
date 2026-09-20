package orch

import (
	"context"
	"errors"
	"fmt"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
)

func (o *Orchestrator) exportSandboxTokenForTest(ctx context.Context, apiKey, sid string, toTemplate, keepSource bool) (string, error) {
	result, err := o.ExportSandbox(ctx, apiKey, sid, toTemplate, keepSource)
	if err != nil {
		return "", err
	}
	if err := result.Validate(); err != nil {
		return "", fmt.Errorf("export contract: %w", err)
	}
	return result.Result, nil
}

func testPublicationReport(kind types.ResumeSourceKind, root string) artifact.PublishReport {
	report := artifact.PublishReport{SandboxRef: root, RemovedRefs: []string{}}
	if kind == types.ResumeSourceSnapshot {
		report.SnapshotRef = root
		report.SandboxRef = "manifest://" + strings.Repeat("e", 64)
	}
	return report
}

func promoteReportScript(root string) string {
	return "for arg in \"$@\"; do input=$arg; done\n" +
		"case \"$input\" in\n" +
		" *.sandbox) printf '%s\\n' '{\"sandboxRef\":\"" + root + "\",\"removedRefs\":[]}' ;;\n" +
		" *) printf '%s\\n' '{\"snapshotRef\":\"" + root + "\",\"sandboxRef\":\"manifest://" + strings.Repeat("e", 64) + "\",\"removedRefs\":[]}' ;;\n" +
		"esac\n"
}

func TestInvalidPublicationReportPreservesMoveSource(t *testing.T) {
	e := "manifest://" + strings.Repeat("e", 64)
	s := "manifest://" + strings.Repeat("a", 64)
	valid := `{"snapshotRef":"` + s + `","sandboxRef":"` + e + `","removedRefs":[]}`
	for name, body := range map[string]string{
		"missing E":      `{"snapshotRef":"` + s + `","removedRefs":[]}`,
		"wrong role":     `{"sandboxRef":"` + e + `","removedRefs":[]}`,
		"local root":     `{"snapshotRef":"file://s.snapshot","sandboxRef":"` + e + `","removedRefs":[]}`,
		"unsafe removal": `{"snapshotRef":"` + s + `","sandboxRef":"` + e + `","removedRefs":["file:///private/old.snapshot"]}`,
		"null removals":  `{"snapshotRef":"` + s + `","sandboxRef":"` + e + `","removedRefs":null}`,
		"trailing":       valid + "{}", "polluted": "log\n" + valid,
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newExportResumeFixture(t)
			dir := t.TempDir()
			script := "#!/bin/sh\nprintf '%s\\n' '" + body + "'\nprintf '%s\\n' 'private diagnostics' >&2\n"
			if err := os.WriteFile(filepath.Join(dir, "sandbox-ctl"), []byte(script), 0755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			result, err := fixture.o.ExportSandbox(fixture.ctx, fixture.apiKey, fixture.sb.ID, false, false)
			if err == nil || result.Result != "" {
				t.Fatalf("invalid success=%+v err=%v", result, err)
			}
			stored, err := fixture.o.st.Get(fixture.ctx, fixture.sb.ID)
			if err != nil || stored == nil || stored.ResumeSource != fixture.sb.ResumeSource {
				t.Fatal("invalid report finalized source")
			}
			if _, err := os.Stat(fixture.localRef); err != nil {
				t.Fatal("invalid report removed checkpoint")
			}
		})
	}
}

func TestPortableSnapshotMetadataResume(t *testing.T) {
	for _, template := range []bool{false, true} {
		t.Run(fmt.Sprint(template), func(t *testing.T) {
			fixture := newExportResumeFixture(t)
			source := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("c", 64)}
			fixture.sb.ResumeSource = source
			if err := fixture.o.st.Put(fixture.ctx, fixture.sb); err != nil {
				t.Fatal(err)
			}
			fixture.o.cache(fixture.sb)
			started := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			finish := func() { releaseOnce.Do(func() { close(release) }) }
			defer finish()
			e := "manifest://" + strings.Repeat("e", 64)
			fixture.o.snapshotSandboxRef = func(ctx context.Context, _ *types.Sandbox, _ types.ResumeSource) (string, error) {
				close(started)
				select {
				case <-release:
					return e, nil
				case <-ctx.Done():
					return "", context.Cause(ctx)
				}
			}
			type outcome struct {
				result types.ExportResult
				err    error
			}
			done := make(chan outcome, 1)
			go func() {
				result, err := fixture.o.ExportSandbox(fixture.ctx, fixture.apiKey, fixture.sb.ID, template, false)
				done <- outcome{result, err}
			}()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("metadata read did not start")
			}
			connected, err := fixture.o.Connect(fixture.ctx, fixture.sb.ID, fixture.apiKey, "", api.ConnectOptions{})
			if err != nil || connected == nil || connected.State != types.StateStarting {
				t.Fatalf("resume=%+v err=%v", connected, err)
			}
			finish()
			select {
			case out := <-done:
				if template {
					if out.err != nil || out.result.SnapshotRef != source.Ref || out.result.SandboxRef != e || len(out.result.RemovedRefs) != 0 {
						t.Fatalf("detached result=%+v err=%v", out.result, out.err)
					}
				} else if !errors.Is(out.err, api.ErrExportPreempted) {
					t.Fatalf("preempt=%v", out.err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("metadata export did not complete")
			}
			if _, err := os.Stat(fixture.localRef); err != nil {
				t.Fatal("resume-winning metadata export removed checkpoint")
			}
			stored, err := fixture.o.st.Get(fixture.ctx, fixture.sb.ID)
			if err != nil || stored == nil || stored.ResumeSource != source {
				t.Fatal("metadata export finalized resumed source")
			}
		})
	}
}
