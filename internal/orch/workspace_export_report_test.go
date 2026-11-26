package orch

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspacedisk"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestWorkspaceExportTokenAndReportUseSameDisk(t *testing.T) {
	o, sb, apiKey := runningWorkspaceFixture(t)
	sb.State = types.StatePaused
	sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://" + strings.Repeat("e", 64)}
	if err := o.st.Put(t.Context(), sb); err != nil {
		t.Fatal(err)
	}
	size := int64(8192)
	disk := rtconfig.DiskV1{Kind: "disk", Version: 1, Size: &size, Base: "manifest://" + strings.Repeat("a", 64), BaseFromRefs: []string{"manifest://" + strings.Repeat("b", 64)}}
	report := artifact.DiskReport{Refs: true, DiskRefs: []string{disk.Base, disk.BaseFromRefs[0]}, RemovedRefs: []string{"file://old.overlay"}}
	result := workspacedisk.Result{Disk: &disk, Report: &report}
	file := installWorkspaceResult(t, o, result)
	out, err := o.ExportWorkspace(t.Context(), sb.ID, apiKey, "project")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := migrationtoken.OpenWorkspace(migrationtoken.KeyMaterial{APISecret: sb.APISecret, ManifestKey: sb.ManifestKey}, out.Result)
	if err != nil || decoded.Base != disk.Base || !slices.Equal(decoded.BaseFromRefs, disk.BaseFromRefs) || !slices.Equal(out.DiskRefs, report.DiskRefs) || !slices.Equal(out.RemovedRefs, report.RemovedRefs) || !out.Refs {
		t.Fatalf("token/report mismatch: disk=%+v report=%+v err=%v", decoded, out, err)
	}
	stored, err := o.st.Get(t.Context(), sb.ID)
	if err != nil || stored.ResumeSource != sb.ResumeSource || stored.State != types.StatePaused {
		t.Fatalf("workspace export changed source: %+v %v", stored, err)
	}

	for _, invalid := range []workspacedisk.Result{
		{Disk: &disk},
		{Disk: &disk, Report: &artifact.DiskReport{}},
		{Disk: &disk, Report: &artifact.DiskReport{Refs: true, DiskRefs: []string{disk.Base}, RemovedRefs: []string{}}},
	} {
		raw, err := json.Marshal(invalid)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if got, err := o.ExportWorkspace(t.Context(), sb.ID, apiKey, "project"); !errors.Is(err, api.ErrWorkspaceUnavailable) || got.Result != "" {
			t.Fatalf("invalid report produced token: %+v, %v", got, err)
		}
	}
}

// Long logical carrier names may recur in distinct removed selectors. The
// export helper must retain the complete delta beyond the old 1 MiB budget,
// while unrelated helper operations keep their original bound.
func TestWorkspaceExportHelperCarriesLargeReferenceDelta(t *testing.T) {
	o, sb, _ := runningWorkspaceFixture(t)
	size := int64(4096)
	disk := rtconfig.DiskV1{Kind: "disk", Version: 1, Size: &size, Base: "manifest://" + strings.Repeat("a", 64)}
	report := artifact.DiskReport{Refs: true, DiskRefs: []string{disk.Base}, RemovedRefs: []string{}}
	for i := range 18 {
		report.RemovedRefs = append(report.RemovedRefs, fmt.Sprintf("file://old.bundle@manifest:%064x@location:%s", i, strings.Repeat("a", 64<<10)))
	}
	want := workspacedisk.Result{Disk: &disk, Report: &report}
	raw, err := json.Marshal(want)
	if err != nil || len(raw) <= rtconfig.MaxPortableConfigBytes || len(raw) > workspacedisk.MaxExportResultBytes {
		t.Fatalf("invalid boundary fixture size=%d: %v", len(raw), err)
	}
	installWorkspaceResult(t, o, want)
	got, err := o.callWorkspaceStorage(t.Context(), sb.ManifestKey, workspacedisk.Request{Operation: "export"})
	if err != nil {
		t.Fatal(err)
	}
	if err := got.ValidateExport(); err != nil || !slices.Equal(got.Report.RemovedRefs, report.RemovedRefs) {
		t.Fatalf("truncated export report: %v", err)
	}
	if _, err := o.callWorkspaceStorage(t.Context(), sb.ManifestKey, workspacedisk.Request{Operation: "list"}); !errors.Is(err, api.ErrWorkspaceUnavailable) {
		t.Fatalf("unrelated helper budget changed: %v", err)
	}
}

func installWorkspaceResult(t *testing.T, o *Orchestrator, result workspacedisk.Result) string {
	t.Helper()
	dir := t.TempDir()
	file := filepath.Join(dir, "result.json")
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KS_TEST_WORKSPACE_RESULT", file)
	nodeCtl := filepath.Join(dir, "node-ctl")
	if err := os.WriteFile(nodeCtl, []byte("#!/bin/sh\ncat \"$KS_TEST_WORKSPACE_RESULT\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	o.SetExecutables(configresolve.ExecutablesForNodeCtl(nodeCtl))
	return file
}
