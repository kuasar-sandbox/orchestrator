package orch

import (
	"context"
	"errors"
	"fmt"
	"github.com/kuasar-sandbox/orchestrator/internal/workspacedisk"
	"slices"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func (o *Orchestrator) prepareBuildWorkspaces(ctx context.Context, b *types.Build, entries []workspace.Entry) error {
	sb := &types.Sandbox{Metadata: b.Metadata, APISecret: b.APISecret, ManifestKey: b.ManifestKey}
	if err := o.prepareCreateWorkspaces(ctx, sb, entries, false); err != nil {
		return err
	}
	b.WorkspacePlan = sb.WorkspacePlan
	return nil
}

func (o *Orchestrator) prepareCreateWorkspaces(ctx context.Context, sb *types.Sandbox, entries []workspace.Entry, image bool) error {
	if err := workspace.CheckEnvelope(sb.Metadata, entries); err != nil {
		return fmt.Errorf("%w: %v", api.ErrBadRequest, err)
	}
	if entries == nil {
		// Public-only inputs may specify unused slots, but never supply a disk source.
		if raw, ok := sb.Metadata[workspace.Namespace]; ok {
			var err error
			entries, err = workspace.Parse(raw)
			if err != nil {
				return fmt.Errorf("%w: %v", api.ErrBadRequest, err)
			}
		}
	}
	if len(entries) == 0 {
		return nil
	}
	for _, e := range entries {
		p := workspace.Prepared{Name: e.Name, Mounts: e.Mounts, Slot: e.Slot}
		if e.Disk != nil {
			d, l, check, err := workspacedisk.Plan(e.Disk, o.workspaceDiskOptions(sb))
			if errors.Is(err, workspacedisk.ErrUnauthorized) {
				return api.ErrNotAllowed
			}
			if errors.Is(err, workspacedisk.ErrSelection) {
				return api.ErrBadRequest
			}
			p.AuthorizeLayers = check
			if err != nil {
				return err
			}
			p.Disk = &d
			p.RefLocations = l
		}
		sb.WorkspacePlan = append(sb.WorkspacePlan, p)
	}
	if image {
		spec, err := sandboxcfg.ParseSpec(sb.Metadata)
		if err != nil {
			return err
		}
		c := rtconfig.SandboxConfig{Mounts: slices.Clone(spec.Mounts)}
		if err := workspace.ApplyImage(&c, sb.WorkspacePlan); err != nil {
			return fmt.Errorf("%w: %v", api.ErrBadRequest, err)
		}
	}
	return nil
}

func (o *Orchestrator) authorizeWorkspacePlan(ctx context.Context, sb *types.Sandbox) error {
	for _, p := range sb.WorkspacePlan {
		if p.AuthorizeLayers {
			if p.Disk == nil {
				return errors.New("workspace source missing from persisted plan")
			}
			if err := o.authorizeWorkspaceLayers(ctx, sb, *p.Disk, p.RefLocations); err != nil {
				return err
			}
		}
	}
	return nil
}

// resolveCapturedWorkspaces runs after the task has read the exact paired E.
// A fresh template Create may choose sources only for its empty slots;
// every resume inherits the saved media and must not replay a former Create.
func resolveCapturedWorkspaces(topology types.ArtifactDiskTopology, choices []workspace.Prepared) (plan, bind []workspace.Prepared, err error) {
	var entries []workspace.Entry
	if topology.WorkspaceTopology != "" {
		entries, err = workspace.Parse(topology.WorkspaceTopology)
		if err != nil {
			return nil, nil, err
		}
	}
	shape := make(map[string]types.ArtifactDiskShape, len(topology.Disks))
	for _, d := range topology.Disks {
		shape[d.Name] = d
	}
	byName := make(map[string]workspace.Prepared, len(choices))
	for _, p := range choices {
		byName[p.Name] = p
	}
	for _, e := range entries {
		d, ok := shape[e.Name]
		if !ok || len(e.Mounts) == 0 || e.Slot != nil || e.Disk != nil {
			return nil, nil, fmt.Errorf("workspace %q: invalid captured disk table", e.Name)
		}
		if p, ok := byName[e.Name]; ok {
			if p.Mounts != nil && !slices.Equal(e.Mounts, p.Mounts) {
				return nil, nil, fmt.Errorf("workspace %q: mounts conflict with template", e.Name)
			}
			if p.Slot != nil || p.Disk != nil {
				if !d.Slot || d.Bound {
					return nil, nil, fmt.Errorf("workspace %q: captured disk is already attached", e.Name)
				}
				if p.Slot != nil {
					size, err := p.Slot.Capacity()
					if err != nil || size != d.SlotSize {
						return nil, nil, fmt.Errorf("workspace %q: captured slot geometry differs", e.Name)
					}
				}
				if p.Disk != nil {
					bind = append(bind, p)
				}
			}
			delete(byName, e.Name)
		}
		plan = append(plan, workspace.Prepared{Name: e.Name, Mounts: e.Mounts})
	}
	if len(byName) != 0 {
		return nil, nil, fmt.Errorf("workspace: template has no matching device")
	}
	return plan, bind, nil
}
