package builder

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/ingest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	rtsandbox "github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
)

type ImageClassPublicationTarget string

const (
	ImageClassManifestStore            ImageClassPublicationTarget = "manifest-store"
	ImageClassCheckpointBundleLocation ImageClassPublicationTarget = "checkpoint-bundle-location"
)

type CheckpointClassPublicationTarget string

const (
	CheckpointClassManifestStore CheckpointClassPublicationTarget = "manifest-store"
	CheckpointClassRefLocation   CheckpointClassPublicationTarget = "checkpoint-ref-location"
)

// BuildPublicationPlan is resolved once, after the Build target, and is the
// only authority used by final publishers. Worker result fields never infer or
// alter either destination.
type BuildPublicationPlan struct {
	ImageClassTarget      ImageClassPublicationTarget
	CheckpointClassTarget CheckpointClassPublicationTarget
}

func resolveBuildPublicationPlan(spec *configsock.BuildSpec) (BuildPublicationPlan, error) {
	if spec == nil {
		return BuildPublicationPlan{}, errors.New("build publication: BuildSpec is required")
	}
	plan := BuildPublicationPlan{
		ImageClassTarget:      ImageClassManifestStore,
		CheckpointClassTarget: CheckpointClassManifestStore,
	}
	if spec.CheckpointRefLocationParent != "" {
		plan.CheckpointClassTarget = CheckpointClassRefLocation
	}
	if spec.CheckpointRemoteManifest {
		if spec.CheckpointRefLocationParent == "" {
			return BuildPublicationPlan{}, errors.New("build publication: checkpoint_remote_manifest requires checkpoint_ref_location_parent")
		}
		plan.ImageClassTarget = ImageClassCheckpointBundleLocation
	}
	return plan, nil
}

type buildPublicationResources struct {
	cfg         *rtconfig.ManifestConfig
	storage     *artifact.ProcessStorage
	keyFn       ingest.CustomerKeyFunc
	customerKey [32]byte
	locations   rtconfig.RefLocations
	admission   store.WriteAdmission
}

func (p *buildPipeline) preparePublication() error {
	plan, err := resolveBuildPublicationPlan(p.spec)
	if err != nil {
		return err
	}
	p.publication = plan
	if strings.TrimSpace(p.spec.Paths.ManifestConfig) == "" {
		return errors.New("build publication: manifest_config is required")
	}
	cfg, err := rtconfig.LoadManifestConfig(p.spec.Paths.ManifestConfig)
	if err != nil {
		return fmt.Errorf("build publication: load manifest config: %w", err)
	}
	rawKey := p.spec.Env[manifest.CustomerKeyEnv]
	parsedKey, err := manifest.ParseHexKey(rawKey)
	if err != nil {
		return fmt.Errorf("build publication: authoritative customer key: %w", err)
	}
	locations, err := buildArtifactRefLocations(p.spec.RefLocations)
	if err != nil {
		return err
	}
	resources := &buildPublicationResources{
		cfg: cfg, customerKey: [32]byte(parsedKey), locations: locations,
	}
	resources.keyFn = func() ([32]byte, error) { return resources.customerKey, nil }
	resources.storage, err = artifact.NewProcessStorageWithCustomerKey(cfg, resources.keyFn)
	if err != nil {
		clear(resources.customerKey[:])
		return fmt.Errorf("build publication: initialize artifact storage: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = resources.Close()
		}
	}()

	if plan.ImageClassTarget == ImageClassCheckpointBundleLocation {
		// Validate the parent without fixing the real publication name/date. The
		// actual name is minted immediately before each image-class publication.
		probeName := reflocation.PublicationName(p.spec.BuildID, time.Unix(0, 0))
		if _, err := reflocation.Resolve(p.spec.CheckpointRefLocationParent, probeName); err != nil {
			return fmt.Errorf("build publication: image Bundle location: %w", err)
		}
		resources.admission, err = cfg.WriteAdmission(p.ctx)
		if err != nil {
			return fmt.Errorf("build publication: Bundle write admission: %w", err)
		}
		if err := artifact.ValidateSingleRootBundlePublication(cfg, resources.keyFn, resources.admission); err != nil {
			return fmt.Errorf("build publication: Bundle preflight: %w", err)
		}
	}
	p.artifacts = resources
	cleanup = false
	return nil
}

func (r *buildPublicationResources) Close() error {
	if r == nil {
		return nil
	}
	err := r.storage.Close()
	clear(r.customerKey[:])
	return err
}

func buildArtifactRefLocations(raw map[string]string) (rtconfig.RefLocations, error) {
	locations := make(rtconfig.RefLocations, len(raw))
	names := make([]string, 0, len(raw))
	for name := range raw {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := locations.Set(name + "=" + raw[name]); err != nil {
			return nil, fmt.Errorf("build publication: ref location %q: %w", name, err)
		}
	}
	return locations, nil
}

func (p *buildPipeline) addPublicationLocation(name string, location reflocation.Location) error {
	if p.spec.RefLocations == nil {
		p.spec.RefLocations = map[string]string{}
	}
	if existing, ok := p.spec.RefLocations[name]; ok && existing != location.URI {
		return fmt.Errorf("build publication: ref location %q conflicts (%q != %q)", name, existing, location.URI)
	}
	p.spec.RefLocations[name] = location.URI
	if existing, ok := p.artifacts.locations[name]; ok && existing != location.Path {
		return fmt.Errorf("build publication: artifact ref location %q conflicts (%q != %q)", name, existing, location.Path)
	}
	p.artifacts.locations[name] = location.Path
	return nil
}

func (p *buildPipeline) openCurrentImage() (*sandboxfile.FlattenedImage, error) {
	if p.artifacts == nil || p.artifacts.storage == nil {
		return nil, errors.New("build publication: artifact storage is not initialized")
	}
	if p.baseRef == "" {
		return nil, errors.New("build publication: current image ref is empty")
	}
	image, err := rtsandbox.OpenFlattenedImage(p.ctx, p.baseRef, p.artifacts.storage, p.artifacts.locations)
	if err != nil {
		return nil, fmt.Errorf("build publication: open current image: %w", err)
	}
	return image, nil
}

func (p *buildPipeline) publishManifestSource(role artifact.LogicalRole, source sparse.Source) (string, error) {
	publisher, err := artifact.NewManifestPublisher(
		p.artifacts.storage, p.artifacts.cfg, p.artifacts.locations, nil,
	)
	if err != nil {
		return "", err
	}
	result, publishErr := publisher.PublishSource(p.ctx, role, source)
	closeErr := publisher.Close()
	if publishErr != nil || closeErr != nil {
		return "", errors.Join(publishErr, closeErr)
	}
	ref, err := validateImageClassPublicationResult(result, role)
	if err != nil {
		return "", err
	}
	parsed, err := manifest.ParseRef(ref)
	if err != nil || parsed.Scheme != manifest.RefSchemeManifest || parsed.Location != "" {
		return "", fmt.Errorf("build publication: Manifest publisher returned non-store ref %q", ref)
	}
	return ref, nil
}

func (p *buildPipeline) publishBundleSource(role artifact.LogicalRole, source sparse.Source) (string, error) {
	if p.now == nil {
		return "", errors.New("build publication: publication clock is not initialized")
	}
	name := reflocation.PublicationName(p.spec.BuildID, p.now())
	location, err := reflocation.Resolve(p.spec.CheckpointRefLocationParent, name)
	if err != nil {
		return "", fmt.Errorf("build publication: image Bundle location %q: %w", name, err)
	}
	// Install the mapping before the sink becomes visible so any returned ref
	// is immediately usable by Phase C and later checkpoint graph publication.
	if err := p.addPublicationLocation(name, location); err != nil {
		return "", err
	}
	publisher, err := artifact.NewSingleRootBundlePublisher(
		p.artifacts.cfg, p.artifacts.keyFn, p.artifacts.admission,
		name, location.Path, nil,
	)
	if err != nil {
		return "", err
	}
	result, publishErr := publisher.PublishSource(p.ctx, role, source)
	closeErr := publisher.Close()
	if publishErr != nil || closeErr != nil {
		return "", errors.Join(publishErr, closeErr)
	}
	ref, err := validateImageClassPublicationResult(result, role)
	if err != nil {
		return "", err
	}
	parsed, err := manifest.ParseRef(ref)
	if err != nil || parsed.Scheme != manifest.RefSchemeFile || parsed.Location != name ||
		parsed.DigestScheme != "manifest" || filepath.Ext(parsed.Path) != ".bundle" {
		return "", fmt.Errorf("build publication: Bundle publisher returned invalid located ref %q", ref)
	}
	return ref, nil
}

func validateImageClassPublicationResult(result artifact.PublishResult, role artifact.LogicalRole) (string, error) {
	if result.Role != role {
		return "", fmt.Errorf("build publication: publisher role %q, want %q", result.Role, role)
	}
	if _, err := types.ParsePortableRef(result.Ref); err != nil {
		return "", fmt.Errorf("build publication: publisher ref %q: %w", result.Ref, err)
	}
	return result.Ref, nil
}

func (p *buildPipeline) publishImageClassSource(role artifact.LogicalRole, source sparse.Source) (string, error) {
	switch p.publication.ImageClassTarget {
	case ImageClassManifestStore:
		return p.publishManifestSource(role, source)
	case ImageClassCheckpointBundleLocation:
		return p.publishBundleSource(role, source)
	default:
		return "", fmt.Errorf("build publication: unsupported image-class target %q", p.publication.ImageClassTarget)
	}
}

func (p *buildPipeline) publishCurrentImageManifest() (ref string, retErr error) {
	if p.baseImageRef != "" {
		parsed, err := manifest.ParseRef(p.baseImageRef)
		if err != nil {
			return "", fmt.Errorf("build publication: existing image ref: %w", err)
		}
		if parsed.Scheme == manifest.RefSchemeManifest && parsed.Location == "" {
			return p.baseImageRef, nil
		}
	}
	image, err := p.openCurrentImage()
	if err != nil {
		return "", err
	}
	defer func() {
		if err := image.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("build publication: close current image: %w", err))
			ref = ""
		}
	}()
	ref, err = p.publishManifestSource(artifact.RoleImage, image.FullStream)
	if err != nil {
		return "", err
	}
	p.baseImageRef = ref
	return ref, nil
}

func (p *buildPipeline) publishImageTarget() (ref string, retErr error) {
	if p.publication.ImageClassTarget == ImageClassManifestStore {
		p.progress("publishing final image to the Manifest store")
		return p.publishCurrentImageManifest()
	}
	p.progress("publishing final image as a named-location Manifest Bundle")
	image, err := p.openCurrentImage()
	if err != nil {
		return "", err
	}
	defer func() {
		if err := image.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("build publication: close current image: %w", err))
			ref = ""
		}
	}()
	ref, err = p.publishBundleSource(artifact.RoleImage, image.FullStream)
	if err != nil {
		return "", err
	}
	p.baseImageRef = ref
	return ref, nil
}

func (p *buildPipeline) publishPhaseCImage() error {
	ref, err := p.publishImageTarget()
	if err != nil {
		return err
	}
	p.baseRef = ref
	return nil
}

func (p *buildPipeline) publishSandboxTarget() (ref string, retErr error) {
	p.progress("assembling top-level Sandbox E from the final image carrier")
	image, err := p.openCurrentImage()
	if err != nil {
		return "", err
	}
	defer func() {
		if err := image.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("build publication: close Sandbox E image: %w", err))
			ref = ""
		}
	}()
	cfg, err := p.buildColdConfig()
	if err != nil {
		return "", err
	}
	opener := rtsandbox.FileStreamOpener(func(ctx context.Context, path string, ref manifest.Ref) (fetch.Stream, error) {
		return p.artifacts.storage.OpenFileWithLocations(ctx, path, ref, p.artifacts.locations)
	})
	portable, err := rtsandbox.PrepareSandboxEConfig(
		p.ctx, cfg, image.ImageConfig, p.artifacts.locations,
		p.artifacts.storage.LocalCodec(), p.artifacts.storage.LocalRequired(), opener,
	)
	if err != nil {
		return "", fmt.Errorf("build publication: prepare top-level Sandbox E: %w", err)
	}
	source, err := rtsandbox.AssembleSandboxE(p.ctx, image, portable)
	if err != nil {
		return "", err
	}
	ref, err = p.publishImageClassSource(artifact.RoleSandbox, source)
	if err != nil {
		return "", err
	}
	p.progress("published top-level Sandbox E: %s", ref)
	return ref, nil
}

func (p *buildPipeline) allowsImportRefererManifestWriteback() bool {
	if p.publication.ImageClassTarget != ImageClassManifestStore {
		return false
	}
	// A top-level Sandbox E is the only final image-class root for this target;
	// publishing its local source IMG solely for referer writeback would restore
	// the forbidden intermediate IMG Manifest.
	return p.target.Kind != types.BuildTargetSandbox || p.target.Memory
}
