package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestCheckpointInspectionMissingDirectory(t *testing.T) {
	portableE := "manifest://" + strings.Repeat("a", 64)
	portableS := "manifest://" + strings.Repeat("b", 64)
	localE := "file://" + strings.Repeat("c", 64) + ".sandbox@digest:" + strings.Repeat("c", 64)
	localS := "file://" + strings.Repeat("d", 64) + ".snapshot@digest:" + strings.Repeat("d", 64)
	for _, tt := range []struct {
		name              string
		current, retained types.ResumeSource
		wantErr           bool
	}{
		{name: "empty"},
		{name: "portable sandbox", current: types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: portableE}},
		{name: "portable snapshot", current: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: portableS, SandboxRef: portableE}},
		{name: "portable retained", retained: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: portableS, SandboxRef: portableE}},
		{name: "local current", current: types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: localE}, wantErr: true},
		{name: "local current memory", current: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: localS, SandboxRef: portableE}, wantErr: true},
		{name: "local current disk", current: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: portableS, SandboxRef: localE}, wantErr: true},
		{name: "local retained", retained: types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: localE}, wantErr: true},
		{name: "local retained disk", retained: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: portableS, SandboxRef: localE}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newDiskCheckpointFixture(t, "missing-inspect")
			ctx := context.Background()
			if !tt.retained.Empty() {
				if ok, err := f.o.st.SetPausing(ctx, f.sb.ID, f.sb.RunID, true); err != nil || !ok {
					t.Fatalf("pausing: %t %v", ok, err)
				}
				if ok, err := f.o.st.CommitPaused(ctx, f.sb.ID, f.sb.RunID, tt.retained, "retained"); err != nil || !ok {
					t.Fatalf("retain: %t %v", ok, err)
				}
			}
			f.sb.ResumeSource = tt.current
			if err := f.o.st.Put(ctx, f.sb); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(f.sb.BaseDir, "checkpoint")
			if err := os.Remove(dir); err != nil {
				t.Fatal(err)
			}
			checkpointCleanupTool(t, f.o, "echo unexpected tool invocation >&2\nexit 93\n")
			refs, err := f.o.inspectOrCleanupCheckpoint(ctx, f.sb, true)
			if tt.wantErr {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("missing local source: refs=%v err=%v", refs, err)
				}
				return
			}
			if err != nil || len(refs) != 0 {
				t.Fatalf("empty local inspection: refs=%v err=%v", refs, err)
			}
			peer := serveDiskCheckpointPeer(t, f.sb)
			diskCheckpointReleaseLifecycle(t, f.o)
			f.o.requestDiskCheckpointRelease(f.sb.ID)
			waitDiskCheckpointRelease(t, f.o, f.sb.ID)
			peer.mu.Lock()
			defer peer.mu.Unlock()
			if peer.lists != 1 || len(peer.deletes) != 0 {
				t.Fatalf("empty cleanup: lists=%d deletes=%v", peer.lists, peer.deletes)
			}
			if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("inspection created unused directory: %v", err)
			}
		})
	}
}

func TestCheckpointInspectionDoesNotSuppressDirectoryIOError(t *testing.T) {
	f := newDiskCheckpointFixture(t, "invalid-inspect")
	// An inaccessible path is not evidence that the local inventory is empty.
	root := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(root, nil, 0600); err != nil {
		t.Fatal(err)
	}
	f.o.cfg.Paths.BaseRoot = root
	f.sb.BaseDir = nodepath.SandboxBaseDir(root, f.sb.ID)
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	if _, err := f.o.inspectOrCleanupCheckpoint(context.Background(), f.sb, true); !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("invalid directory was accepted as empty: %v", err)
	}
}
