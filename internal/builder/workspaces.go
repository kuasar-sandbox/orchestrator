package builder

import (
	"errors"
	"fmt"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"path/filepath"
	"slices"

	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"github.com/kuasar-sandbox/orchestrator/internal/workspacedisk"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

// A derived template has its own publication lifetime. Export each bound
// source disk from the selected E, including versions held inside a Checkpoint.
// Preserve slot geometry across C's explicit cold boot and bind before capture.
func (p *buildPipeline) prepareWorkspaces() error {
	s := p.spec
	if p.target.Kind != types.BuildTargetSandbox {
		return nil
	}
	var plan []workspace.Prepared
	inherited := map[string]rtconfig.PortableDiskConfig{}
	if source := s.SourceSandboxConfig; source != nil {
		entries, err := workspace.Captured(source)
		if err != nil {
			return err
		}
		selections := make(map[string]workspace.Prepared, len(s.SandboxSpec.WorkspacePlan))
		for _, entry := range s.SandboxSpec.WorkspacePlan {
			selections[entry.Name] = entry
		}
		for _, d := range source.Boot.Disks {
			inherited[d.Name] = d
		}
		for _, e := range entries {
			disk := inherited[e.Name]
			choice := selections[e.Name]
			delete(selections, e.Name)
			if choice.Mounts != nil && !slices.Equal(choice.Mounts, e.Mounts) {
				return fmt.Errorf("workspace %q: source mounts differ", e.Name)
			}
			if !disk.Unbound() && (choice.Slot != nil || choice.Disk != nil) {
				return fmt.Errorf("workspace %q: source is already attached", e.Name)
			}
			if choice.Slot != nil {
				expected, _ := disk.Slot.Capacity()
				actual, err := choice.Slot.Capacity()
				if err != nil || expected != actual {
					return fmt.Errorf("workspace %q: source slot size differs", e.Name)
				}
			}
			plan = append(plan, workspace.Prepared{Name: e.Name, Mounts: e.Mounts, Slot: disk.Slot, Disk: choice.Disk, RefLocations: choice.RefLocations})
		}
		if len(selections) != 0 {
			return errors.New("workspace: source has no matching device")
		}
		if len(source.Boot.Disks) > len(entries) && len(entries) > 0 {
			// Ordinary data disks may have overlay filesystems or relative Bundle
			// members. Let the whole-graph publisher relocate these dependencies.
			graph, err := p.portableSourceForBuild()
			if err != nil {
				return err
			}
			s.SourceSandboxConfig = graph
		}
	} else {
		plan = slices.Clone(s.SandboxSpec.WorkspacePlan)
	}
	if len(plan) == 0 {
		return nil
	}
	dest := artifact.DiskDestination{Upload: true}
	name := reflocation.PublicationName(s.BuildID)
	if p.publication.ImageClassTarget == ImageClassCheckpointBundleLocation {
		loc, err := reflocation.Resolve(s.CheckpointRefLocationParent, name)
		if err != nil {
			return err
		}
		if err = p.addPublicationLocation(name, loc); err != nil {
			return err
		}
		dest = artifact.DiskDestination{OutputDir: loc.Path}
	}
	for i := range plan {
		entry := &plan[i]
		var fixed rtconfig.DiskV1
		var err error
		switch {
		case inherited[entry.Name].Name != "" && !inherited[entry.Name].Unbound():
			fixed, err = p.artifacts.storage.ExportDisk(p.ctx, artifact.ExportDiskRequest{SourceRef: s.SourceSandboxRef, DiskName: entry.Name, RefLocations: p.artifacts.locations, OutputDir: dest.OutputDir, Upload: dest.Upload})
		case entry.Disk != nil:
			fixed, err = p.artifacts.storage.PublishDisk(p.ctx, *entry.Disk, p.artifacts.locations, dest)
		default:
			continue
		}
		if err != nil {
			return fmt.Errorf("prepare workspace %q: %w", entry.Name, err)
		}
		if !dest.Upload {
			if err := workspacedisk.Qualify(&fixed, dest.OutputDir, name); err != nil {
				return err
			}
		}
		if entry.Slot != nil {
			capacity, err := entry.Slot.Capacity()
			if err != nil || capacity != 0 && capacity != *fixed.Size {
				return fmt.Errorf("workspace %q: disk size differs from source slot", entry.Name)
			}
			p.workspaceBindings = append(p.workspaceBindings, ctl.BindDiskRequest{SlotName: entry.Name, Disk: fixed, RefLocations: p.artifacts.locations})
			entry.Disk = nil
		} else {
			entry.Disk = &fixed
		}
	}
	s.SandboxSpec.WorkspacePlan = plan
	return nil
}

func (p *buildPipeline) bindTemplateDisks() error {
	for _, req := range p.workspaceBindings {
		if err := ctl.BindDisk(p.ctx, filepath.Join(p.phaseRunDir("c"), "ctl.sock"), req); err != nil {
			return fmt.Errorf("bind template workspace %q: %w", req.SlotName, err)
		}
	}
	return nil
}

// portableSourceForBuild makes each ordinary disk layer addressable outside
// the original E before --replace-boot installs the new root image.
func (p *buildPipeline) portableSourceForBuild() (_ *rtconfig.PortableSandboxConfig, retErr error) {
	var publisher *artifact.Publisher
	var err error
	if p.publication.ImageClassTarget == ImageClassCheckpointBundleLocation {
		name := reflocation.PublicationName(p.spec.BuildID)
		loc, e := reflocation.Resolve(p.spec.CheckpointRefLocationParent, name)
		if e != nil {
			return nil, e
		}
		if e = p.addPublicationLocation(name, loc); e != nil {
			return nil, e
		}
		publisher, err = artifact.NewLocationPublisher(p.artifacts.storage, name, loc.Path, p.artifacts.locations, nil)
	} else {
		publisher, err = artifact.NewManifestPublisher(p.artifacts.storage, p.artifacts.cfg, p.artifacts.locations, nil)
	}
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, publisher.Close()) }()
	result, err := publisher.Publish(p.ctx, p.spec.SourceSandboxRef)
	if err != nil {
		return nil, err
	}
	ref, err := manifest.ParseRef(result.Ref)
	if err != nil {
		return nil, err
	}
	stream, err := workspacedisk.OpenLayer(p.ctx, p.artifacts.storage, ref, "", p.artifacts.locations)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, stream.Close()) }()
	resultConfig, _, err := sandboxfile.ReadDiskTable(p.ctx, stream)
	return resultConfig, err
}
