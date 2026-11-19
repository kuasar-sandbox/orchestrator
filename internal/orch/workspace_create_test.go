package orch

import (
	"context"
	"encoding/json"
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

func TestWorkspaceCreatePreservesDefaultIntentUntilTargetGeometry(t *testing.T) {
	o := testOrch(t)
	o.cfg.Sandbox.Boot.OverlayDiffTemplate = "/legacy-overlay-must-not-be-used"
	o.cfg.Sandbox.Boot.WorkspaceDiffTemplate = ""
	sb := &types.Sandbox{Metadata: map[string]string{}}
	entries := []workspace.Entry{{Name: "project", Disk: json.RawMessage(`{}`)}}
	sb.Metadata[workspace.Namespace] = workspace.TopologyJSON(entries)
	if err := o.prepareCreateWorkspaces(t.Context(), sb, entries, false); err != nil {
		t.Fatal(err)
	}
	if len(sb.WorkspacePlan) != 1 || !sb.WorkspacePlan[0].DefaultFromSlot || sb.WorkspacePlan[0].Disk != nil {
		t.Fatalf("default intent lost or resolved early: %+v", sb.WorkspacePlan)
	}
	raw, err := json.Marshal(sb.WorkspacePlan)
	if err != nil {
		t.Fatal(err)
	}
	var recovered []workspace.Prepared
	if err := json.Unmarshal(raw, &recovered); err != nil {
		t.Fatalf("pending plan is not recoverable: %v", err)
	}
	// Configuration can change while source preparation is running. The
	// admitted choice still derives from the target, not this newer template.
	o.cfg.Sandbox.Boot.WorkspaceDiffTemplate = "mkfs://ext4?size=16MiB"
	saved := types.ArtifactDiskTopology{WorkspaceTopology: `[{"name":"project","mounts":[{"path":"/data"}]}]`, Disks: []types.ArtifactDiskShape{{Name: "project", Mode: types.ArtifactDiskSingle, Slot: true, SlotSize: 32 << 20}}}
	plan, binds, err := resolveCapturedWorkspaces(saved, recovered)
	if err != nil || len(plan) != 1 || len(binds) != 1 || binds[0].Disk == nil || binds[0].Disk.DiffTemplate != "mkfs://ext4?size=33554432" || binds[0].DefaultFromSlot {
		t.Fatalf("target default = %+v, %+v, %v", plan, binds, err)
	}
	saved.Disks[0].SlotSize = 0
	if _, _, err := resolveCapturedWorkspaces(saved, recovered); err == nil {
		t.Fatal("dynamic slot silently acquired a default capacity")
	}
	saved.Disks[0].SlotSize, saved.Disks[0].Bound = 32<<20, true
	if _, _, err := resolveCapturedWorkspaces(saved, recovered); err == nil {
		t.Fatal("pending default replaced a captured disk")
	}
}

func TestWorkspaceCreateExplicitSizeAndOmission(t *testing.T) {
	o := testOrch(t)
	o.cfg.Sandbox.Boot.WorkspaceDiffTemplate = "mkfs://ext4?size=32MiB"
	sb := &types.Sandbox{Metadata: map[string]string{}}
	entries := []workspace.Entry{
		{Name: "sized", Disk: json.RawMessage(`{"kind":"disk","version":1,"size":16777216}`)},
		{Name: "inherit"},
	}
	sb.Metadata[workspace.Namespace] = workspace.TopologyJSON(entries)
	if err := o.prepareCreateWorkspaces(t.Context(), sb, entries, false); err != nil {
		t.Fatal(err)
	}
	if sb.WorkspacePlan[0].Disk.DiffTemplate != "mkfs://ext4?size=16777216" || sb.WorkspacePlan[1].Disk != nil || sb.WorkspacePlan[1].DefaultFromSlot {
		t.Fatalf("explicit size or omission changed: %+v", sb.WorkspacePlan)
	}
	o.cfg.Sandbox.Boot.WorkspaceDiffTemplate = ""
	sb.WorkspacePlan = nil
	entries = []workspace.Entry{{Name: "project", Mounts: []workspace.Mount{{Path: "/data"}}, Disk: json.RawMessage(`{}`)}}
	sb.Metadata[workspace.Namespace] = workspace.TopologyJSON(entries)
	if err := o.prepareCreateWorkspaces(t.Context(), sb, entries, true); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("new image device without geometry: %v", err)
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

func TestWorkspaceTemplateRegistrationRequiresTargetAndRetainsDefault(t *testing.T) {
	o := testOrch(t)
	o.cfg.Sandbox.Boot.WorkspaceDiffTemplate = "/seeds/first.ext4"
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	meta, entries, err := workspace.Extract(map[string]string{workspace.Namespace: `[{"name":"project","mounts":[{"path":"/data"}],"disk":{}}]`})
	if err != nil {
		t.Fatal(err)
	}
	spec := api.RegisterSpec{Name: "ws-template", Resources: testBuildResources(), Metadata: meta, Workspaces: entries}
	for _, profile := range []types.Profile{types.ProfileBare, types.ProfileE2B} {
		spec.Profile = profile
		for _, target := range []*types.BuildTarget{nil, {Kind: types.BuildTargetImage}} {
			spec.Builder.Target = target
			if _, err := o.RegisterBuild(ctx, apiKey, spec); !errors.Is(err, api.ErrBadRequest) || !strings.Contains(err.Error(), "builder.target") {
				t.Fatalf("profile=%s target=%v: %v", profile, target, err)
			}
		}
	}
	spec.Builder.Target = &types.BuildTarget{Kind: types.BuildTargetSandbox}
	b, err := o.RegisterBuild(ctx, apiKey, spec)
	if err != nil {
		t.Fatal(err)
	}
	o.cfg.Sandbox.Boot.WorkspaceDiffTemplate = "/seeds/updated.ext4"
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
	for _, requested := range []*types.BuildTarget{nil, {Kind: types.BuildTargetImage}} {
		stored.Builder.Target = requested
		if err := validateBuildTriggerDefinition(stored, api.TriggerSpec{}); !errors.Is(err, api.ErrBadRequest) {
			t.Fatalf("trigger admitted stored workspace build with target %v: %v", requested, err)
		}
		stored.FromImage = ""
		stored.FromTemplate = types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: result.SandboxRef}.String()
		if got, handled, err := directImageBuildResult(stored, false); got != nil || !handled || err == nil || !strings.Contains(err.Error(), "builder.target") {
			t.Fatalf("fast path with target %v: got %+v, handled %t, error %v", requested, got, handled, err)
		}
		if err := validateBuildResult(stored, result); err == nil || !strings.Contains(err.Error(), "workspace") {
			t.Fatalf("accepted stored workspace build result with target %v: %v", requested, err)
		}
	}
	for _, requested := range []*types.BuildTarget{nil, {Kind: types.BuildTargetImage}} {
		spec.Builder.Target = requested
		spec.Metadata = map[string]string{workspace.Namespace: "[ ]"}
		spec.Workspaces = nil
		spec.Profile = types.ProfileBare
		emptyNode := testOrch(t)
		emptyKey, _, _ := allowlistedBuildIdentity(t, emptyNode)
		empty, err := emptyNode.RegisterBuild(ctx, emptyKey, spec)
		if err != nil || len(empty.WorkspacePlan) != 0 {
			t.Fatalf("empty workspace with target %v: %v", requested, err)
		}
	}
}
