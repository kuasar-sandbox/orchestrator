package builder

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"github.com/kuasar-sandbox/orchestrator/internal/workspacedisk"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"gopkg.in/yaml.v3"
)

func (p *buildPipeline) prepareWorkspaces() error {
	s := p.spec
	if p.target.Kind != types.BuildTargetSandbox {
		return nil
	}
	plan := slices.Clone(s.SandboxSpec.WorkspacePlan)
	if source := s.SourceSandboxConfig; source != nil {
		entries := workspace.Captured(source)
		if len(entries) == 0 && len(plan) == 0 {
			return nil
		}
		inputs := make(map[string]workspace.Prepared, len(plan))
		for _, in := range plan {
			inputs[in.Name] = in
		}
		disks := make(map[string]rtconfig.PortableDiskConfig, len(source.Boot.Disks))
		for _, disk := range source.Boot.Disks {
			disks[disk.Name] = disk
		}
		plan = nil
		for _, e := range entries {
			d := disks[e.Name]
			choice := inputs[e.Name]
			delete(inputs, e.Name)
			if choice.Mounts != nil && !slices.Equal(choice.Mounts, e.Mounts) {
				return fmt.Errorf("workspace %q: source mounts differ", e.Name)
			}
			if !d.Unbound() && (choice.Slot != nil || choice.Disk != nil) {
				return fmt.Errorf("workspace %q: source is already attached", e.Name)
			}
			if choice.Slot != nil {
				want, _ := d.Slot.Capacity()
				got, err := choice.Slot.Capacity()
				if err != nil || got != want {
					return fmt.Errorf("workspace %q: slot size differs", e.Name)
				}
			}
			if choice.Disk != nil {
				p.workspaceBindings = append(p.workspaceBindings, ctl.BindDiskRequest{SlotName: e.Name, Disk: *choice.Disk, RefLocations: p.artifacts.locations})
			}
			plan = append(plan, workspace.Prepared{Name: e.Name, Mounts: e.Mounts, Slot: d.Slot})
		}
		if len(inputs) != 0 {
			return errors.New("workspace: source has no matching device")
		}
		if len(entries) != 0 {
			staged, err := p.stageDiskTable(s.SourceSandboxRef)
			if err != nil {
				return err
			}
			s.SourceSandboxConfig = staged
		}
	} else {
		for i := range plan {
			if plan[i].Disk == nil {
				continue
			}
			fixed, err := workspacedisk.FreezeSeed(p.ctx, *plan[i].Disk, p.artifacts.storage, p.artifacts.locations, p.phaseRunDir("workspace"))
			if err != nil {
				return fmt.Errorf("prepare workspace %q: %w", plan[i].Name, err)
			}
			plan[i].Disk = &fixed
		}
	}
	s.SandboxSpec.WorkspacePlan = plan
	return nil
}

func (p *buildPipeline) stageDiskTable(input string) (*rtconfig.PortableSandboxConfig, error) {
	dir := p.phaseRunDir("workspace")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	return taskartifact.StageDiskTable(p.ctx, input, p.artifacts.storage, p.artifacts.locations, dir)
}

// Only a build selecting a medium for an inherited empty slot needs a native
// binding record. Bind in the already running steps sandbox and capture its
// complete device table before RUN or COPY can change the medium.
func (p *buildPipeline) captureSelectedSlots(sb *phaseSandbox) error {
	if len(p.workspaceBindings) == 0 {
		return nil
	}
	for _, req := range p.workspaceBindings {
		if err := ctl.BindDisk(p.ctx, filepath.Join(p.phaseRunDir(sb.pathID), "ctl.sock"), req); err != nil {
			return fmt.Errorf("bind template workspace %q: %w", req.SlotName, err)
		}
	}
	dir := p.phaseRunDir("workspace-bind")
	out, err := p.hostCmdEnv(p.spec.Env, p.spec.Paths.SandboxCtl, "export", "--json", "--resume", "--mode", "local", "--path-id", sb.pathID, "--run-root", sb.runRoot, "--output", dir)
	if err != nil {
		return fmt.Errorf("capture initial template disk table: %w (%s)", err, firstLine(out))
	}
	report, err := artifact.DecodeCaptureReport(out, artifact.RoleSandbox)
	if err != nil {
		return err
	}
	ref, err := manifest.ParseRef(report.SandboxRef)
	if err != nil {
		return err
	}
	ref.Path = filepath.Join(dir, ref.Path)
	table, err := p.stageDiskTable(ref.String())
	if err != nil {
		return err
	}
	return p.acceptSelectedSlots(table)
}

func (p *buildPipeline) acceptSelectedSlots(table *rtconfig.PortableSandboxConfig) error {
	original := p.spec.SourceSandboxConfig
	if len(table.Boot.Disks) != len(original.Boot.Disks) {
		return errors.New("captured template disk count changed")
	}
	for _, req := range p.workspaceBindings {
		i := slices.IndexFunc(original.Boot.Disks, func(d rtconfig.PortableDiskConfig) bool { return d.Name == req.SlotName })
		if i < 0 || table.Boot.Disks[i].Name != req.SlotName || table.Boot.Disks[i].Slot == nil || table.Boot.Disks[i].Binding == nil {
			return fmt.Errorf("template slot %q is missing its native binding", req.SlotName)
		}
		original.Boot.Disks[i] = table.Boot.Disks[i]
	}
	return nil
}

// A memory build with data disks boots the assembled local E. The ordinary
// run --from path restores binding records and allocates fresh private writers.
func (p *buildPipeline) templateBoot() ([]byte, string, bool, error) {
	if len(p.spec.SandboxSpec.WorkspacePlan) == 0 {
		doc, err := p.buildColdConfigYAML()
		return doc, p.spec.SourceSandboxRef, p.spec.SourceSandboxRef != "", err
	}
	image, err := p.openCurrentImage()
	if err != nil {
		return nil, "", false, err
	}
	defer image.Close()
	cfg, portable, err := p.sandboxAssembly(image)
	if err != nil {
		return nil, "", false, err
	}
	source, err := p.assembleWorkspaceBoot(cfg, portable)
	if err != nil {
		return nil, "", false, err
	}
	document, err := sandboxcfg.MarshalBuildColdConfig(cfg)
	if err != nil {
		return nil, "", false, err
	}
	var host map[string]any
	if err = yaml.Unmarshal(document, &host); err != nil {
		return nil, "", false, err
	}
	host["boot"] = map[string]any{"kernel": cfg.Boot.Kernel, "runtime": cfg.Boot.Runtime}
	doc, err := yaml.Marshal(host)
	return doc, source, false, err
}

func (p *buildPipeline) assembleWorkspaceBoot(cfg *rtconfig.SandboxConfig, portable *rtconfig.PortableSandboxConfig) (string, error) {
	dir := p.phaseRunDir("workspace")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	sink := snapshot.NewFileSink(dir, "template", p.artifacts.storage.LocalCodec(), p.artifacts.storage.LocalRequired(), nil)
	return p.writeWorkspaceBoot(cfg, portable, sink, dir)
}

// Staging keeps local dependencies next to the selected E. Resolve those paths
// for the host config; the portable config will again use the content identities.
func (p *buildPipeline) resolveStagedDiskPaths(cfg *rtconfig.SandboxConfig) error {
	if p.spec.SourceSandboxConfig == nil {
		return nil
	}
	for i := range cfg.Boot.Disks {
		root := &cfg.Boot.Disks[i].RootConfig
		refs := []*string{&root.Base}
		for j := range root.BaseFromRefs {
			refs = append(refs, &root.BaseFromRefs[j])
		}
		if root.Overlay != nil {
			refs = append(refs, &root.Overlay.Base)
			for j := range root.Overlay.BaseFromRefs {
				refs = append(refs, &root.Overlay.BaseFromRefs[j])
			}
		}
		for _, field := range refs {
			if *field == "" {
				continue
			}
			ref, err := manifest.ParseRef(*field)
			if err != nil {
				return err
			}
			if ref.Scheme == manifest.RefSchemeFile && ref.Location == "" && !filepath.IsAbs(ref.Path) {
				ref.Path = filepath.Join(p.phaseRunDir("workspace"), ref.Path)
				*field = ref.String()
			}
		}
	}
	return nil
}
