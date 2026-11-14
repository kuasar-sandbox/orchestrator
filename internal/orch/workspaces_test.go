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

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

func TestWorkspaceLiveOwnerGenerationRetryAndPrivacy(t *testing.T) {
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("runtime"))
	mk := strings.Repeat("a", 64)
	sb := migrationSandbox(t, dir, "b", mk, "manifest://"+mk)
	_, key := defaultTestCredentials(t, mk)
	sb.State = types.StateRunning
	sb.RunID = "first"
	sb.WorkspacePlan = []workspace.Prepared{{Name: "project", Mounts: []workspace.Mount{{Path: "/data"}}}}
	o.cfg.Sandbox.Boot.OverlayDiffTemplate = filepath.Join(dir, "default-one")
	if err := o.st.Put(t.Context(), sb); err != nil {
		t.Fatal(err)
	}
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
	run, state, source := "first", "unbound", ""
	attempts := 0
	s := ctl.Server{Path: sock, DiskListHandler: func(expected string) (ctl.DiskStateReport, error) {
		mu.Lock()
		defer mu.Unlock()
		if expected != "" && expected != run {
			return ctl.DiskStateReport{}, ctl.ErrDiskConflict
		}
		return ctl.DiskStateReport{RunID: run, Disks: []ctl.DiskState{{Name: "ordinary", State: "ready"}, {Name: "project", State: state}}}, nil
	}, DiskBindHandler: func(req ctl.BindDiskRequest) error {
		mu.Lock()
		defer mu.Unlock()
		if req.ExpectedRun != run {
			return ctl.ErrDiskConflict
		}
		id := req.Disk.Identity()
		if source != "" && source != id {
			return ctl.ErrDiskConflict
		}
		attempts++
		if source == "" {
			source = id
			state = "published_pending_guest"
			return ctl.ErrDiskPending
		}
		state = "ready"
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
		if err != nil || len(got.Workspaces) != 1 || got.Workspaces[0].Name != "project" || got.Workspaces[0].State != want || got.RunGeneration != "first" {
			t.Fatal(got, err)
		}
	}
	check("unattached")
	if _, err := o.ListWorkspaces(ctx, sb.ID, "other"); !errors.Is(err, api.ErrNotFound) {
		t.Fatal(err)
	}
	if err := o.AttachWorkspace(ctx, sb.ID, key, "ordinary", "", []byte("{}")); !errors.Is(err, api.ErrNotFound) {
		t.Fatal(err)
	}
	if err := o.AttachWorkspace(ctx, sb.ID, key, "project", "stale", []byte("{}")); !errors.Is(err, api.ErrWorkspaceGeneration) {
		t.Fatal(err)
	}
	if err := o.AttachWorkspace(ctx, sb.ID, key, "old-name", "stale", []byte("{}")); !errors.Is(err, api.ErrWorkspaceGeneration) {
		t.Fatal(err)
	}
	if _, err := o.ExportWorkspace(ctx, sb.ID, key, "project"); !errors.Is(err, api.ErrWorkspaceConflict) {
		t.Fatal(err)
	}
	if err := o.AttachWorkspace(ctx, sb.ID, key, "project", "first", []byte("{}")); !errors.Is(err, api.ErrWorkspaceUnavailable) {
		t.Fatal(err)
	}
	check("attaching")
	o.cfg.Sandbox.Boot.OverlayDiffTemplate = filepath.Join(dir, "replaced-default")
	if err := o.AttachWorkspace(ctx, sb.ID, key, "project", "first", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	check("attached")
	size := int64(4096)
	other, err := migrationtoken.SealWorkspace(migrationtoken.KeyMaterial{APISecret: sb.APISecret, ManifestKey: mk}, rtconfig.DiskV1{Kind: "disk", Version: 1, Size: &size, Base: "manifest://" + mk})
	if err != nil {
		t.Fatal(err)
	}
	token, _ := json.Marshal(map[string]string{"migrationToken": other})
	if err := o.AttachWorkspace(ctx, sb.ID, key, "project", "first", token); !errors.Is(err, api.ErrWorkspaceConflict) {
		t.Fatal(err)
	}
	mu.Lock()
	run = "replacement"
	mu.Unlock()
	if err := o.AttachWorkspace(ctx, sb.ID, key, "project", "first", []byte("{}")); !errors.Is(err, api.ErrWorkspaceGeneration) {
		t.Fatal(err)
	}
	if err := o.AttachWorkspace(ctx, sb.ID, key, "project", "", []byte("{}")); !errors.Is(err, api.ErrWorkspaceConflict) {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 {
		t.Fatalf("publication or notification handed to wrong owner (%d attempts)", attempts)
	}
}
