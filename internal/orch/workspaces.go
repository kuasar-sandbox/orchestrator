package orch

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"github.com/kuasar-sandbox/orchestrator/internal/workspacedisk"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

func (o *Orchestrator) workspaceDiskOptions(sb *types.Sandbox) workspacedisk.Options {
	return workspacedisk.Options{APISecret: sb.APISecret, ManifestKey: sb.ManifestKey, ManifestConfig: o.cfg.ManifestConfig, DefaultDisk: o.cfg.Sandbox.Boot.WorkspaceDefaultDisk, OverlayDiffTemplate: o.cfg.Sandbox.Boot.OverlayDiffTemplate, RefLocationParent: o.cfg.Checkpoint.Remote.RefLocationParent}
}

func (o *Orchestrator) resolveWorkspaceDisk(ctx context.Context, sb *types.Sandbox, raw json.RawMessage) (rtconfig.DiskV1, rtconfig.RefLocations, error) {
	d, l, check, err := workspacedisk.Plan(raw, o.workspaceDiskOptions(sb))
	if err == nil && check {
		err = o.authorizeWorkspaceLayers(ctx, sb, d, l)
	}

	switch {
	case errors.Is(err, workspacedisk.ErrSelection):
		err = api.ErrBadRequest
	case errors.Is(err, workspacedisk.ErrUnauthorized):
		err = api.ErrNotAllowed
	}
	return d, l, err
}

func (o *Orchestrator) authorizeWorkspaceLayers(ctx context.Context, sb *types.Sandbox, disk rtconfig.DiskV1, locations rtconfig.RefLocations) error {
	_, err := o.callWorkspaceStorage(ctx, sb.ManifestKey, workspacedisk.Request{Operation: "authorize", ManifestConfig: o.cfg.ManifestConfig, Disk: &disk, Locations: locations})
	return err
}

func (o *Orchestrator) diskSocket(sb *types.Sandbox) string {
	return filepath.Join(nodepath.SandboxRunDir(o.cfg.Paths.RunRoot, sb.ID), "ctl.sock")
}

func (o *Orchestrator) bindWorkspaceDisk(ctx context.Context, sb *types.Sandbox, name string, d rtconfig.DiskV1, locations rtconfig.RefLocations) error {
	if sb.RunID == "" {
		return api.ErrWorkspaceUnavailable
	}
	return ctl.BindDisk(ctx, o.diskSocket(sb), ctl.BindDiskRequest{ExpectedRun: sb.RunID, SlotName: name, Disk: d, RefLocations: locations})
}

func workspaceDeclared(sb *types.Sandbox, name string) bool {
	for _, entry := range sb.WorkspacePlan {
		if entry.Name == name {
			return true
		}
	}
	return false
}

func workspaceChanged(conditional bool) error {
	if conditional {
		return api.ErrWorkspaceGeneration
	}
	return api.ErrWorkspaceConflict
}

func (o *Orchestrator) AttachWorkspace(ctx context.Context, id, apiKey, name, generation string, input json.RawMessage) error {
	ctx, finish, err := o.workspaceOperation(ctx)
	if err != nil {
		return err
	}
	defer finish()
	unlock := o.lifecycle.Lock(id)
	defer unlock()
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return err
	}
	if !ownsSandbox(sb, apiKey) {
		return api.ErrNotFound
	}
	if generation != "" && generation != sb.RunID {
		return api.ErrWorkspaceGeneration
	}
	if !workspaceDeclared(sb, name) {
		return api.ErrNotFound
	}
	if !holdRunning(sb) {
		return api.ErrWorkspaceConflict
	}

	var fields map[string]json.RawMessage
	if json.Unmarshal(input, &fields) != nil || fields == nil {
		return api.ErrBadRequest
	}
	var d rtconfig.DiskV1
	var locations rtconfig.RefLocations
	if len(fields) == 0 {
		fixed, err := o.st.WorkspaceDefault(ctx, id, sb.InstanceID, sb.RunID, name, nil)
		if errors.Is(err, store.ErrWorkspaceGeneration) {
			return workspaceChanged(generation != "")
		}
		if err != nil {
			return err
		}
		if fixed == nil {
			d, locations, err = o.resolveWorkspaceDisk(ctx, sb, input)
			if err != nil {
				return err
			}
			fixed, err = o.st.WorkspaceDefault(ctx, id, sb.InstanceID, sb.RunID, name, &store.WorkspaceDefault{Disk: d, Locations: locations})
			if errors.Is(err, store.ErrWorkspaceGeneration) {
				return workspaceChanged(generation != "")
			}
			if err != nil {
				return err
			}
		}
		d, locations = fixed.Disk, fixed.Locations
	} else {
		d, locations, err = o.resolveWorkspaceDisk(ctx, sb, input)
		if err != nil {
			return err
		}
	}
	err = o.bindWorkspaceDisk(ctx, sb, name, d, locations)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	switch {
	case errors.Is(err, ctl.ErrDiskPermission):
		return api.ErrNotAllowed
	case errors.Is(err, ctl.ErrDiskInvalid):
		return api.ErrBadRequest
	case errors.Is(err, ctl.ErrDiskCapacity), errors.Is(err, ctl.ErrDiskUnsupported):
		return api.ErrWorkspaceConflict
	case errors.Is(err, ctl.ErrDiskConflict):
		// Conflict also covers a different source already bound to this slot.
		// Ask the owner which generation it serves to distinguish that case.
		report, e := ctl.ListDataDisks(ctx, o.diskSocket(sb), "")
		if e == nil && report.RunID != sb.RunID {
			return workspaceChanged(generation != "")
		}
		return api.ErrWorkspaceConflict
	default:
		return api.ErrWorkspaceUnavailable
	}
}

func (o *Orchestrator) ListWorkspaces(ctx context.Context, id, apiKey string) (api.WorkspaceList, error) {
	var out api.WorkspaceList
	ctx, finish, err := o.workspaceOperation(ctx)
	if err != nil {
		return out, err
	}
	defer finish()
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return out, err
	}
	if !ownsSandbox(sb, apiKey) {
		return out, api.ErrNotFound
	}
	var disks []ctl.DiskState
	out.Workspaces = []api.WorkspaceStatus{}
	switch {
	case holdRunning(sb):
		out.RunGeneration = sb.RunID
		if len(sb.WorkspacePlan) == 0 {
			return out, nil
		}
		report, err := ctl.ListDataDisks(ctx, o.diskSocket(sb), sb.RunID)
		if err != nil {
			if ctx.Err() != nil {
				return out, ctx.Err()
			}
			return out, api.ErrWorkspaceUnavailable
		}
		disks = report.Disks
	case sb.State == types.StatePaused:
		if len(sb.WorkspacePlan) == 0 && sb.Metadata[workspace.Namespace] == "" {
			return out, nil
		}
		guard, unlock, err := o.lockLifecycleMutation(ctx, id)
		if err != nil {
			return out, err
		}
		defer unlock()
		current, err := o.st.Get(guard, id)
		if err != nil {
			return out, err
		}
		if !ownsSandbox(current, apiKey) || current.InstanceID != sb.InstanceID {
			return out, api.ErrNotFound
		}
		if current.State != types.StatePaused || !current.ResumeSource.Valid() {
			return out, api.ErrWorkspaceUnavailable
		}
		sb = current
		result, err := o.workspaceArchiveOperation(guard, sb, "list", "")
		if err != nil {
			return out, err
		}
		sb.WorkspacePlan = nil
		for _, e := range result.Entries {
			sb.WorkspacePlan = append(sb.WorkspacePlan, workspace.Prepared{Name: e.Name, Mounts: e.Mounts})
		}
		disks = result.States
	default:
		return out, api.ErrWorkspaceUnavailable
	}
	for _, decl := range sb.WorkspacePlan {
		i := slices.IndexFunc(disks, func(d ctl.DiskState) bool { return d.Name == decl.Name })
		if i < 0 {
			return out, api.ErrWorkspaceUnavailable
		}
		d := disks[i]
		s := api.WorkspaceStatus{Name: decl.Name, Mounts: decl.Mounts, SizeBytes: d.SizeBytes}
		switch d.State {
		case "unbound":
			s.State = "unattached"
		case "published_pending_guest":
			s.State = "attaching"
		case "ready":
			s.State = "attached"
		default:
			return out, api.ErrWorkspaceUnavailable
		}
		out.Workspaces = append(out.Workspaces, s)
	}
	return out, nil
}
