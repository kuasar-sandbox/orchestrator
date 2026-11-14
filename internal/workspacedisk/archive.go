package workspacedisk

import (
	"context"
	"errors"
	"path/filepath"
	"slices"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
)

type workspaceArchive struct {
	storage    *artifact.ProcessStorage
	root       fetch.Stream
	ref        string
	parent     string
	dir        string
	locations  rtconfig.RefLocations
	config     *rtconfig.PortableSandboxConfig
	checkpoint *sandboxfile.Checkpoint
}

func (a *workspaceArchive) close() error {
	return errors.Join(a.root.Close(), a.storage.Close())
}

// The row's E reference is committed together with its S. Select it directly
// so listing and publishing never fetch guest memory, even from an S/E pair.
func openArchive(ctx context.Context, req Request, key string) (_ *workspaceArchive, retErr error) {
	a := &workspaceArchive{ref: req.Source, dir: req.SourceDir, parent: req.RefLocationParent, locations: rtconfig.RefLocations{}}
	add := func(ref string) error {
		return AddLocation(a.locations, req.RefLocationParent, ref)
	}
	if err := add(a.ref); err != nil {
		return nil, err
	}
	st, err := Storage(req.ManifestConfig, key)
	if err != nil {
		return nil, err
	}
	a.storage = st
	defer func() {
		if retErr != nil {
			_ = st.Close()
		}
	}()
	r, err := manifest.ParseRef(a.ref)
	if err != nil {
		return nil, err
	}
	a.root, err = OpenLayer(ctx, st, r, a.dir, a.locations)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = a.root.Close()
		}
	}()
	if opened, ok := a.root.(*artifact.OpenedFile); ok {
		a.dir = filepath.Dir(opened.ResolvedPath())
		if bundle := opened.BundleReader(); bundle != nil {
			for _, ref := range bundle.Refs() {
				if err := add(ref); err != nil {
					return nil, err
				}
			}
		}
	}
	a.config, a.checkpoint, err = sandboxfile.ReadDiskTable(ctx, a.root)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// inventory reads only committed metadata and logical sizes. A v4 ordinary
// disk's current diff may be its only layer and its size lives in that version.
func (a *workspaceArchive) inventory(ctx context.Context, plan []workspace.Entry) ([]ctl.DiskState, error) {
	var pins *artifact.PinnedDiffCheckpoints
	defer func() { _ = pins.Close() }()
	states := make([]ctl.DiskState, 0, len(plan))
	for _, disk := range a.config.Boot.Disks {
		if !slices.ContainsFunc(plan, func(e workspace.Entry) bool { return e.Name == disk.Name }) {
			continue
		}
		state := ctl.DiskState{Name: disk.Name, State: "ready"}
		var capacity int64
		switch {
		case disk.Unbound():
			state.State = "unbound"
			var err error
			capacity, err = disk.Slot.Capacity()
			if err != nil {
				return nil, err
			}
		case disk.Binding != nil:
			capacity = disk.Binding.SizeBytes
		case a.checkpoint != nil:
			if pins == nil {
				var err error
				pins, err = a.storage.PinDiffCheckpoints(ctx, a.dir, a.checkpoint)
				if err != nil {
					return nil, err
				}
			}
			view := pins.Disks[disk.Name]
			if view == nil {
				return nil, errors.New("missing saved workspace version")
			}
			capacity = int64(view.Size())
		default:
			if err := AddLocation(a.locations, a.parent, disk.Base); err != nil {
				return nil, err
			}
			r, err := manifest.ParseRef(disk.Base)
			if err != nil {
				return nil, err
			}
			store := a.storage
			if carrier, ok := a.root.(*artifact.OpenedFile); ok && carrier.ScopedFetcher() != nil {
				store = a.storage.BorrowWithFetcher(carrier.ScopedFetcher())
			}
			layer, err := OpenLayer(ctx, store, r, a.dir, a.locations)
			if err != nil {
				return nil, err
			}
			if !disk.PureBlock {
				layer, _, err = sandboxfile.PayloadIfSandbox(ctx, layer, filepath.Ext(r.Path) == ".sandbox")
				if err != nil {
					return nil, err
				}
			}
			capacity = int64(layer.Size())
			if err := layer.Close(); err != nil {
				return nil, err
			}
		}
		if capacity > 0 {
			state.SizeBytes = &capacity
		}
		states = append(states, state)
	}
	return states, nil
}

func (a *workspaceArchive) publish(guard context.Context, request Request, name string) (*rtconfig.DiskV1, error) {
	req := artifact.ExportDiskRequest{SourceRef: a.ref, SourceDir: a.dir, DiskName: name, RefLocations: a.locations, Upload: true}
	parent := request.RefLocationParent
	var output reflocation.Location
	var err error
	publication := reflocation.PublicationName(request.Publication)
	if parent != "" {
		output, err = reflocation.Resolve(parent, publication)
		if err != nil {
			return nil, err
		}
		req.Upload = false
		req.OutputDir = output.Path
	}
	// Resolve only the selected disk: an unrelated disk can be unavailable
	// or use a location that is not mounted on this node.
	for _, saved := range a.config.Boot.Disks {
		if saved.Name == name {
			for _, raw := range append([]string{saved.Base}, saved.BaseFromRefs...) {
				if err := AddLocation(a.locations, parent, raw); err != nil {
					return nil, err
				}
			}
			break
		}
	}
	disk, err := a.storage.ExportDisk(guard, req)
	if errors.Is(err, artifact.ErrDiskUnbound) || errors.Is(err, artifact.ErrDiskLayout) {
		return nil, artifact.ErrDiskUnbound
	}
	if err != nil {
		return nil, err
	}
	if !req.Upload {
		if err := Qualify(&disk, output.Path, publication); err != nil {
			return nil, err
		}
		a.locations[publication] = output.Path
	}
	// Validate every returned root independently in its exported namespace,
	// even if the publisher reused an existing content-addressed object.
	for _, raw := range append([]string{disk.Base}, disk.BaseFromRefs...) {
		ref, err := manifest.ParseRef(raw)
		if err != nil {
			return nil, err
		}
		layer, err := OpenLayer(guard, a.storage, ref, "", a.locations)
		if err != nil {
			return nil, err
		}
		if err := layer.Close(); err != nil {
			return nil, err
		}
	}
	return &disk, nil
}
