package orch

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestCapturedWorkspaceOnlyBindsEmptyDevicesOnFreshCreate(t *testing.T) {
	saved := types.ArtifactDiskTopology{WorkspaceTopology: `[{"name":"used","mounts":[{"path":"/a"}]},{"name":"empty","mounts":[{"path":"/b"}]}]`, Disks: []types.ArtifactDiskShape{{Name: "used", Mode: types.ArtifactDiskSingle}, {Name: "empty", Mode: types.ArtifactDiskSingle, Slot: true}}}
	d := &rtconfig.DiskV1{Kind: "disk", Version: 1, DiffTemplate: "file:///seed"}
	choices := []workspace.Prepared{{Name: "empty", Disk: d}}
	effective, bind, err := resolveCapturedWorkspaces(saved, choices)
	if err != nil || len(effective) != 2 || effective[0].Name != "used" || effective[1].Mounts[0].Path != "/b" || len(bind) != 1 || bind[0].Disk != d {
		t.Fatalf("effective=%+v bind=%v err=%v", effective, bind, err)
	}
	_, bind, err = resolveCapturedWorkspaces(saved, nil)
	if err != nil || len(bind) != 0 {
		t.Fatal("pure inheritance replayed a bind", err)
	}
	for _, c := range []workspace.Prepared{{Name: "used", Disk: d}, {Name: "empty", Mounts: []workspace.Mount{{Path: "/bad"}}}, {Name: "new", Disk: d}} {
		if _, _, err := resolveCapturedWorkspaces(saved, []workspace.Prepared{c}); err == nil {
			t.Fatalf("accepted %v", c)
		}
	}
	saved.Disks[1].Bound = true
	if _, _, err := resolveCapturedWorkspaces(saved, choices); err == nil {
		t.Fatal("replaced a captured backend")
	}
}

func TestImageCreateValidatesWorkspaceTopologyAtAdmission(t *testing.T) {
	o := &Orchestrator{}
	ctx := context.Background()
	for _, raw := range []string{`[{"name":"missing","slot":{}}]`, `[{"name":"p","mounts":[{"path":"/data"}],"slot":{}}]`} {
		meta, entries, err := workspace.Extract(map[string]string{workspace.Namespace: raw})
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(raw, "/data") {
			meta["kuasar-sandbox.mounts"] = `[{"type":"tmpfs","target":"/data/tmp"}]`
		}
		err = o.prepareCreateWorkspaces(ctx, &types.Sandbox{Metadata: meta}, entries, true)
		if !errors.Is(err, api.ErrBadRequest) {
			t.Errorf("admitted invalid image topology: %s: %v", raw, err)
		}
	}
}

func TestWorkspaceTemplateRegistrationRetainsDefaultAndAutoTarget(t *testing.T) {
	o := testOrch(t)
	o.cfg.Sandbox.Boot.OverlayDiffTemplate = "/seeds/first.ext4"
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	meta, entries, err := workspace.Extract(map[string]string{workspace.Namespace: `[{"name":"project","mounts":[{"path":"/data"}],"disk":{}}]`})
	if err != nil {
		t.Fatal(err)
	}
	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{Name: "ws-template", Profile: types.ProfileBare, Resources: testBuildResources(), Metadata: meta, Workspaces: entries})
	if err != nil {
		t.Fatal(err)
	}
	o.cfg.Sandbox.Boot.OverlayDiffTemplate = "/seeds/updated.ext4"
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.WorkspacePlan) != 1 || stored.WorkspacePlan[0].Disk.DiffTemplate != "file:///seeds/first.ext4" || strings.Contains(stored.Metadata[workspace.Namespace], "disk") {
		t.Fatalf("registration lost its fixed source: %+v", stored.WorkspacePlan)
	}
	stored.FromImage = "example.org/base:1"
	if err := validateKnownBuildTargetConfig(stored); err != nil {
		t.Fatal(err)
	}
	target := types.BuildTarget{Kind: types.BuildTargetSandbox}
	result := types.BuildResult{Target: target, SandboxRef: "manifest://" + strings.Repeat("1", 64)}
	if err := validateBuildResult(stored, result); err != nil {
		t.Fatal(err)
	}
	stored.Builder.Target = &types.BuildTarget{Kind: types.BuildTargetImage}
	if err := validateKnownBuildTargetConfig(stored); err == nil {
		t.Fatal("explicit IMG silently dropped workspace disks")
	}
}
