package taskartifact

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/workspacedisk"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

// prepareWorkspaceSandbox creates an ordinary E in this instance's checkpoint
// directory. The original E remains the admitted template, including on retry.
func prepareWorkspaceSandbox(ctx context.Context, spec configsock.ArtifactPrepareSpec, topology types.ArtifactDiskTopology, storage *artifact.ProcessStorage, locations rtconfig.RefLocations, bundleRefs []string) (_ string, _ *rtconfig.PortableSandboxConfig, retErr error) {
	var captured []workspace.Entry
	if raw := topology.WorkspaceTopology; raw != "" {
		var err error
		captured, err = workspace.Parse(raw)
		if err != nil {
			return "", nil, err
		}
	}
	var names []string
	for _, d := range topology.Disks {
		names = append(names, d.Name)
	}
	added, err := workspace.AdditionalDevices(names, captured, spec.SBXCreateWorkspaces)
	if err != nil || len(added) == 0 {
		return "", nil, err
	}
	if !filepath.IsAbs(spec.RelativeDir) {
		return "", nil, errors.New("workspace SBX requires the instance checkpoint directory")
	}
	// The whole-graph reader needs the optional named source locations when
	// choosing a Bundle member. Carry only selected locations into the child.
	for _, raw := range bundleRefs {
		if err := workspacedisk.AddLocation(locations, spec.RefLocationParent, raw); err != nil {
			return "", nil, err
		}
	}
	root, err := stageSandboxRoot(ctx, spec.RootRef, storage, locations, spec.RelativeDir)
	if err != nil {
		return "", nil, err
	}
	defer func() { retErr = errors.Join(retErr, root.Close()) }()
	child := root.Portable
	child.Version = 2
	// A caller-supplied mount list replaces ordinary application mounts.
	// The source E owns all disk mounts, including disks absent from metadata.
	if len(spec.SBXCreateMounts) > 0 {
		if err := rtconfig.ValidateMountPaths(spec.SBXCreateMounts); err != nil {
			return "", nil, err
		}
		var diskMounts []rtconfig.MountConfig
		for _, m := range child.Mounts {
			if m.Type == "disk" {
				diskMounts = append(diskMounts, m)
			}
		}
		var kept []rtconfig.MountConfig
		for _, m := range spec.SBXCreateMounts {
			if m.Type == "disk" {
				if !slices.Contains(diskMounts, m) {
					return "", nil, fmt.Errorf("workspace: requested disk mount %s differs from source", m.Target)
				}
				continue
			}
			kept = append(kept, m)
		}
		child.Mounts = append(kept, diskMounts...)
	}
	sink := snapshot.NewFileSink(spec.RelativeDir, "workspace", storage.LocalCodec(), storage.LocalRequired(), nil)
	defer func() { retErr = errors.Join(retErr, sink.Close()) }()
	for _, p := range added {
		d := rtconfig.PortableDiskConfig{Name: p.Name, Slot: p.Slot}
		if p.Disk == nil && d.Slot == nil {
			d.Slot = &rtconfig.DiskSlotConfig{}
		}
		if p.Disk != nil {
			for name, dir := range p.RefLocations {
				if old, ok := locations[name]; ok && old != dir {
					return "", nil, fmt.Errorf("workspace %q: conflicting location %s", p.Name, name)
				}
				locations[name] = dir
			}
			fixed, err := workspacedisk.FreezeSeed(ctx, *p.Disk, storage, locations, spec.RelativeDir)
			if err != nil {
				return "", nil, fmt.Errorf("workspace %q: %w", p.Name, err)
			}
			var refs []string
			for _, raw := range append([]string{fixed.Base}, fixed.BaseFromRefs...) {
				ref, err := manifest.ParseRef(raw)
				if err != nil {
					return "", nil, err
				}
				if ref.Scheme == manifest.RefSchemeFile && ref.Location == "" {
					if filepath.Dir(ref.Path) == spec.RelativeDir && ref.Digest != "" {
						ref.Path = filepath.Base(ref.Path)
						raw = ref.String()
					} else {
						raw, err = copyWorkspaceLayer(ctx, raw, storage, locations, sink)
						if err != nil {
							return "", nil, err
						}
					}
				}
				refs = append(refs, raw)
			}
			d.PureBlock, d.Base, d.BaseFromRefs = true, refs[0], refs[1:]
		}
		child.Boot.Disks = append(child.Boot.Disks, d)
		for _, m := range p.Mounts {
			child.Mounts = append(child.Mounts, rtconfig.MountConfig{Type: "disk", Source: p.Name, Target: m.Path, Subpath: m.Subpath})
		}
		captured = append(captured, workspace.Entry{Name: p.Name, Mounts: p.Mounts})
	}
	if child.Metadata == nil {
		child.Metadata = make(map[string]string)
	}
	child.Metadata[workspace.Namespace] = workspace.TopologyJSON(captured)
	content, err := rtconfig.MarshalPortableSandboxConfig(child)
	if err != nil {
		return "", nil, err
	}
	source, err := sandboxfile.BuildSourceContext(ctx, root.Payload, root.ImageConfig, content)
	if err != nil {
		return "", nil, err
	}
	ref, _, err := sink.AbsorbSandbox(ctx, source)
	return ref, child, err
}

func copyWorkspaceLayer(ctx context.Context, raw string, storage *artifact.ProcessStorage, locations rtconfig.RefLocations, sink *snapshot.FileSink) (_ string, retErr error) {
	stream, err := openArtifactStream(ctx, raw, "", storage, locations)
	if err != nil {
		return "", err
	}
	defer func() { retErr = errors.Join(retErr, stream.Close()) }()
	ref, _, err := sink.AbsorbOverlaySource(ctx, stream)
	return ref, err
}

// StageDiskTable relocates an ordinary or checkpoint E into directory and
// returns its complete saved device graph. Unlocated references in the result
// resolve relative to directory. The returned graph owns no open resources.
func StageDiskTable(ctx context.Context, raw string, storage *artifact.ProcessStorage, locations rtconfig.RefLocations, directory string) (*rtconfig.PortableSandboxConfig, error) {
	root, err := stageSandboxRoot(ctx, raw, storage, locations, directory)
	if err != nil {
		return nil, err
	}
	return root.Portable, root.Close()
}

func stageSandboxRoot(ctx context.Context, raw string, storage *artifact.ProcessStorage, locations rtconfig.RefLocations, directory string) (*sandboxfile.Root, error) {
	input, err := parseArtifactRef(raw, directory)
	if err != nil {
		return nil, err
	}
	if input.Scheme == manifest.RefSchemeFile && input.Location == "" && !filepath.IsAbs(input.Path) {
		input.Path = filepath.Join(directory, input.Path)
	}
	// The whole-artifact publisher relocates every disk layout and pins the
	// version of each writable disk referenced by a checkpoint.
	staged, err := storage.MaterializeCheckpoint(ctx, input.String(), locations, directory)
	if err != nil {
		return nil, fmt.Errorf("stage source Sandbox E: %w", err)
	}
	stream, err := openArtifactStream(ctx, staged.Ref, directory, storage, locations)
	if err != nil {
		return nil, err
	}
	root, err := sandboxfile.Open(ctx, stream)
	if err != nil {
		return nil, err
	}
	if file, ok := stream.(*artifact.OpenedFile); ok && file.ManifestFetcher() != nil {
		root.Portable, err = bindBundleMembers(ctx, staged.Ref, file, root.Portable)
		if err != nil {
			return nil, errors.Join(err, root.Close())
		}
	}
	return root, nil
}

// A copied Bundle keeps its manifest references implicit. Once the child E
// is in a tarstream, each borrowed member needs its selected Bundle carrier.
func bindBundleMembers(ctx context.Context, rootRef string, file *artifact.OpenedFile, cfg *rtconfig.PortableSandboxConfig) (*rtconfig.PortableSandboxConfig, error) {
	carrier, err := manifest.ParseRef(rootRef)
	if err != nil {
		return nil, err
	}
	rewrite := map[string]string{}
	for _, raw := range portableArtifactRefs(cfg) {
		ref, err := manifest.ParseRef(raw)
		if err != nil {
			return nil, err
		}
		if ref.Scheme != manifest.RefSchemeManifest {
			continue
		}
		key, err := manifest.ParseKeyRef(ref.Path)
		if err != nil {
			return nil, err
		}
		member, err := file.ManifestFetcher().SelectManifest(ctx, key)
		if err != nil {
			return nil, err
		}
		if member.Reader == nil {
			continue
		}
		at := carrier
		if member.Ref != "" {
			at, err = manifest.ParseRef(member.Ref)
			if err != nil {
				return nil, err
			}
		}
		if at.Scheme == manifest.RefSchemeFile && at.Location == "" && filepath.Base(at.Path) == at.Path {
			at.Location = carrier.Location
		}
		at.DigestScheme, at.Digest = "manifest", ref.Path
		rewrite[raw] = at.String()
	}
	return cfg.RewriteDiskArtifactRefs(rewrite)
}
