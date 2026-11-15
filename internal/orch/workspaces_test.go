package orch

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

func runningWorkspaceFixture(t *testing.T) (*Orchestrator, *types.Sandbox, string) {
	t.Helper()
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	mk := strings.Repeat("a", 64)
	sb := migrationSandbox(t, dir, "b", mk, "manifest://"+mk)
	_, key := defaultTestCredentials(t, mk)
	sb.State = types.StateRunning
	sb.WorkspacePlan = []workspace.Prepared{{Name: "project", Mounts: []workspace.Mount{{Path: "/data"}}}}
	o.cfg.Sandbox.Boot.OverlayDiffTemplate = filepath.Join(dir, "default-one")
	if err := o.st.Put(t.Context(), sb); err != nil {
		t.Fatal(err)
	}
	return o, sb, key
}

func TestWorkspaceAttachRetainsDefaultWhenOwnerUnavailable(t *testing.T) {
	o, sb, key := runningWorkspaceFixture(t)
	ctx := t.Context()
	selected := o.cfg.Sandbox.Boot.OverlayDiffTemplate
	if err := o.AttachWorkspace(ctx, sb.ID, "other", "project", []byte("{}")); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("foreign sandbox: %v", err)
	}
	if err := o.AttachWorkspace(ctx, sb.ID, key, "ordinary", []byte("{}")); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("undeclared workspace: %v", err)
	}
	if got, err := o.st.WorkspaceDefault(ctx, sb.ID, sb.InstanceID, "project", nil); err != nil || got != nil {
		t.Fatalf("unauthorized attach recorded a default: %v, %v", got, err)
	}
	for _, template := range []string{selected, filepath.Join(t.TempDir(), "new-default")} {
		o.cfg.Sandbox.Boot.OverlayDiffTemplate = template
		if err := o.AttachWorkspace(ctx, sb.ID, key, "project", []byte("{}")); !errors.Is(err, api.ErrWorkspaceUnavailable) {
			t.Fatalf("owner unavailable: %v", err)
		}
		got, err := o.st.WorkspaceDefault(ctx, sb.ID, sb.InstanceID, "project", nil)
		if err != nil || got == nil || got.Disk.DiffTemplate != "file://"+selected {
			t.Fatalf("stored default: %v, %v", got, err)
		}
	}
}

func TestWorkspaceLiveRetryAndPrivacy(t *testing.T) {
	o, sb, key := runningWorkspaceFixture(t)
	// Keep the Unix socket short, even for runners with long build paths.
	runRoot, err := os.MkdirTemp("", "ws-owner-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(runRoot)
	o.cfg.Paths.RunRoot = runRoot
	sock := o.diskSocket(sb)
	if err := os.MkdirAll(filepath.Dir(sock), 0700); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	state, source := "unbound", ""
	attempts := 0
	s := ctl.Server{Path: sock, DiskListHandler: func() (ctl.DiskStateReport, error) {
		mu.Lock()
		defer mu.Unlock()
		return ctl.DiskStateReport{Disks: []ctl.DiskState{{Name: "ordinary", State: "ready"}, {Name: "project", State: state}}}, nil
	}, DiskBindHandler: func(req ctl.BindDiskRequest) error {
		mu.Lock()
		defer mu.Unlock()
		id := req.Disk.Identity()
		if source != "" && source != id {
			return ctl.ErrDiskConflict
		}
		attempts++
		if source == "" {
			source = id
			// The writer was published before the directory sync failed.
			state = "ready"
			return ctl.ErrDiskPending
		}
		return nil
	}}
	if err := s.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = s.Serve(ctx) }()
	defer func() { cancel(); s.Stop(); <-done }()
	check := func(want string) {
		t.Helper()
		got, err := o.ListWorkspaces(ctx, sb.ID, key)
		if err != nil || len(got.Workspaces) != 1 || got.Workspaces[0].Name != "project" || got.Workspaces[0].State != want {
			t.Fatal(got, err)
		}
	}
	check("unattached")
	if _, err := o.ListWorkspaces(ctx, sb.ID, "other"); !errors.Is(err, api.ErrNotFound) {
		t.Fatal(err)
	}
	if err := o.AttachWorkspace(ctx, sb.ID, key, "ordinary", []byte("{}")); !errors.Is(err, api.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := o.ExportWorkspace(ctx, sb.ID, key, "project"); !errors.Is(err, api.ErrWorkspaceConflict) {
		t.Fatal(err)
	}
	if err := o.AttachWorkspace(ctx, sb.ID, key, "project", []byte("{}")); !errors.Is(err, api.ErrWorkspaceUnavailable) {
		t.Fatal(err)
	}
	check("attached")
	o.cfg.Sandbox.Boot.OverlayDiffTemplate = filepath.Join(t.TempDir(), "replaced-default")
	binDir := t.TempDir()
	entered, release := filepath.Join(binDir, "entered"), filepath.Join(binDir, "release")
	t.Setenv("KS_TEST_PROBE_ENTERED", entered)
	t.Setenv("KS_TEST_PROBE_RELEASE", release)
	fixture := `#!/bin/sh
case "$*" in
    *flatten-ctl*) cat >/dev/null ;;
    *) touch "$KS_TEST_PROBE_ENTERED"; while [ ! -e "$KS_TEST_PROBE_RELEASE" ]; do sleep 0.01; done ;;
esac
`
	if err := os.WriteFile(filepath.Join(binDir, "sandbox-ctl"), []byte(fixture), 0700); err != nil {
		t.Fatal(err)
	}
	o.SetExecutables(configresolve.ExecutablesForNodeCtl(filepath.Join(binDir, "node-ctl")))
	doneAttach := make(chan error, 1)
	attachCtx, cancelAttach := context.WithTimeout(ctx, 5*time.Second)
	defer cancelAttach()
	go func() { doneAttach <- o.AttachWorkspace(attachCtx, sb.ID, key, "project", []byte("{}")) }()
	for {
		if _, err := os.Stat(entered); err == nil {
			break
		}
		select {
		case err := <-doneAttach:
			t.Fatal("attach returned before barrier", err)
		case <-time.After(time.Millisecond):
		}
	}
	check("attaching")
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := <-doneAttach; err != nil {
		t.Fatal(err)
	}
	check("attached")
	size := int64(4096)
	other, err := migrationtoken.SealWorkspace(migrationtoken.KeyMaterial{APISecret: sb.APISecret, ManifestKey: sb.ManifestKey}, rtconfig.DiskV1{Kind: "disk", Version: 1, Size: &size, Base: "manifest://" + sb.ManifestKey})
	if err != nil {
		t.Fatal(err)
	}
	token, _ := json.Marshal(map[string]string{"migrationToken": other})
	if err := o.AttachWorkspace(ctx, sb.ID, key, "project", token); !errors.Is(err, api.ErrWorkspaceConflict) {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 {
		t.Fatalf("got %d bind attempts, want 2", attempts)
	}
}
