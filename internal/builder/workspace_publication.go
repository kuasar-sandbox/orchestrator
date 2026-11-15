package builder

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/workspacedisk"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

// publishWorkspaceTarget emits every local disk dependency and the final E
// through the same sink. Located and remote parents keep the source selected by
// the native dependency plan; staging paths never enter the published graph.
func (p *buildPipeline) publishWorkspaceTarget(image *sandboxfile.FlattenedImage, cfg *rtconfig.SandboxConfig, portable *rtconfig.PortableSandboxConfig) (_ string, retErr error) {
	stage := p.phaseRunDir("workspace")
	if err := os.MkdirAll(stage, 0700); err != nil {
		return "", err
	}
	a := p.artifacts
	bundleTarget := p.publication.ImageClassTarget == ImageClassCheckpointBundleLocation
	plan, err := p.workspaceDependencyPlan(cfg, portable, stage, bundleTarget)
	if err != nil {
		return "", err
	}
	defer func() { retErr = errors.Join(retErr, plan.Close()) }()
	var sink snapshot.ArtifactSink
	var destination *artifact.Publisher
	switch p.publication.ImageClassTarget {
	case ImageClassManifestStore:
		ing, err := a.cfg.NewIngester(a.keyFn, nil)
		if err != nil {
			return "", err
		}
		sink = snapshot.NewIngestSink(ing, p.progress)
	case ImageClassCheckpointBundleLocation:
		name := reflocation.PublicationName(p.spec.BuildID)
		location, err := reflocation.Resolve(p.spec.CheckpointRefLocationParent, name)
		if err != nil {
			return "", err
		}
		if err = p.addPublicationLocation(name, location); err != nil {
			return "", err
		}
		destination, err = artifact.NewLocationPublisher(a.storage, name, location.Path, a.locations, nil)
		if err != nil {
			return "", err
		}
		defer func() { retErr = errors.Join(retErr, destination.Close()) }()
		// Assemble on the destination filesystem. The native Bundle sink's
		// semantic alias is private to this operation. Publish verifies and
		// atomically exposes only the content-addressed root and its closure.
		bundleDir, err := os.MkdirTemp(location.Path, ".workspace-build-*")
		if err != nil {
			return "", err
		}
		defer func() { retErr = errors.Join(retErr, os.RemoveAll(bundleDir)) }()
		sink, err = snapshot.NewPlannedBundleSink(bundleDir, p.spec.BuildID, a.cfg, a.keyFn, a.admission, plan.Refs(), p.progress)
		if err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("unsupported image-class target %q", p.publication.ImageClassTarget)
	}
	defer func() { retErr = errors.Join(retErr, sink.Close()) }()
	rewritten, err := p.emitWorkspaceDependencies(portable, plan, sink)
	if err != nil {
		return "", err
	}
	source, err := sandbox.AssembleSandboxE(p.ctx, image, rewritten)
	if err != nil {
		return "", err
	}
	ref, path, err := p.commitWorkspaceSandbox(source, sink)
	if err != nil {
		return "", err
	}
	if destination == nil {
		return validateImageClassPublicationResult(artifact.PublishResult{Role: artifact.RoleSandbox, Ref: ref}, artifact.RoleSandbox)
	}
	local, err := absoluteCommittedSandbox(sink, ref, path)
	if err != nil {
		return "", err
	}
	published, err := destination.Publish(p.ctx, local)
	if err != nil {
		return "", err
	}
	return validateImageClassPublicationResult(published, artifact.RoleSandbox)
}

// The memory target boots a private E with the initial ext4 upper as its self
// payload. Native captures retain the already-published Phase C IMG as the
// immutable EROFS lower, independently of the checkpoint publication target.
func (p *buildPipeline) writeWorkspaceBoot(cfg *rtconfig.SandboxConfig, portable *rtconfig.PortableSandboxConfig, sink *snapshot.FileSink, directory string) (_ string, retErr error) {
	portable, err := rtconfig.ProjectPortableCold(cfg, rtconfig.PortableProjection{KernelRef: portable.Boot.Kernel, RuntimeRef: portable.Boot.Runtime})
	if err != nil {
		return "", err
	}
	plan, err := p.workspaceDependencyPlan(cfg, portable, directory, false)
	if err != nil {
		return "", err
	}
	defer func() { retErr = errors.Join(retErr, plan.Close()) }()
	rewritten, err := p.emitWorkspaceDependencies(portable, plan, sink)
	if err != nil {
		return "", err
	}
	runtime, err := rtconfig.MarshalPortableSandboxConfig(rewritten)
	if err != nil {
		return "", err
	}
	a := p.artifacts
	staged, err := os.MkdirTemp(directory, ".root-upper-*")
	if err != nil {
		return "", err
	}
	defer func() { retErr = errors.Join(retErr, os.RemoveAll(staged)) }()
	seed, err := workspacedisk.FreezeSeed(p.ctx, rtconfig.DiskV1{Kind: "disk", Version: 1, DiffTemplate: cfg.Boot.Root.Overlay.DiffTemplate}, a.storage, a.locations, staged)
	if err != nil {
		return "", err
	}
	layer, err := manifest.ParseRef(seed.Base)
	if err != nil {
		return "", err
	}
	upper, err := workspacedisk.OpenLayer(p.ctx, a.storage, layer, staged, a.locations)
	if err != nil {
		return "", err
	}
	defer func() { retErr = errors.Join(retErr, upper.Close()) }()
	source, err := sandboxfile.BuildSourceContext(p.ctx, upper, nil, runtime)
	if err != nil {
		return "", err
	}
	ref, path, err := p.commitWorkspaceSandbox(source, sink)
	if err != nil {
		return "", err
	}
	return absoluteCommittedSandbox(sink, ref, path)
}

func (p *buildPipeline) workspaceDependencyPlan(cfg *rtconfig.SandboxConfig, portable *rtconfig.PortableSandboxConfig, directory string, bundleTarget bool) (*sandbox.SandboxDependencyPlan, error) {
	a := p.artifacts
	opts := sandbox.RunOptions{
		Cfg: cfg, PortableConfig: portable, SandboxID: p.spec.BuildID,
		Fetcher: a.storage.Fetcher(), ManifestCfg: a.cfg, CustomerKeyFn: a.keyFn,
		LocalCodec: a.storage.LocalCodec(), LocalRequired: a.storage.LocalRequired(), RefLocations: a.locations,
	}
	return sandbox.PrepareSandboxDependencyPlan(p.ctx, opts, make([]bool, len(portable.Boot.Disks)+1), directory, a.admission, bundleTarget)
}

func (p *buildPipeline) emitWorkspaceDependencies(portable *rtconfig.PortableSandboxConfig, plan *sandbox.SandboxDependencyPlan, sink snapshot.ArtifactSink) (*rtconfig.PortableSandboxConfig, error) {
	replacements, err := plan.Emit(p.ctx, sink, p.progress)
	if err != nil {
		return nil, err
	}
	return portable.RewriteDiskArtifactRefs(replacements)
}

func (p *buildPipeline) commitWorkspaceSandbox(source sparse.Source, sink snapshot.ArtifactSink) (string, string, error) {
	ref, path, err := sink.AbsorbSandbox(p.ctx, source)
	if err != nil {
		return "", "", err
	}
	if err = sink.CommitSandbox(p.ctx, ref, path); err != nil {
		return "", "", err
	}
	return ref, path, nil
}

func absoluteCommittedSandbox(sink snapshot.ArtifactSink, ref, path string) (string, error) {
	selected, carrier, err := snapshot.CommittedArtifactRef(sink, ref, path)
	if err != nil {
		return "", err
	}
	local, err := manifest.ParseRef(selected)
	if err != nil {
		return "", err
	}
	local.Path, err = filepath.Abs(carrier)
	if err != nil {
		return "", err
	}
	return local.String(), nil
}
