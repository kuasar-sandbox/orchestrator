// Package taskartifact performs tenant-bound task-local Artifact preparation.
// It owns MANIFEST_KEY-dependent reads and never returns an artifact reference
// or portable config to the conductor process.
package taskartifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	manifestbundle "github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
)

const (
	DefaultMaxRefs                 = 1024
	HardMaxRefs                    = 1024
	maxArtifactNetworkMetadataSize = 1 << 20
)

// CarrierBinding records the physical root selected during preparation. It is
// task-local and is useful for deterministic replay and diagnostics without
// exposing customer key material.
type CarrierBinding struct {
	Format          string
	FilePath        string
	RootManifestKey string
}

// Result remains in the tenant task process. Only Summary is sent to the
// conductor; PreparedSource is appended to sandbox-ctl argv by node-ctl.
type Result struct {
	PreparedSource types.ResumeSource
	// SourceSandboxConfig and SourceImageConfig stay in the tenant task. Build
	// source normalization uses them after an S root has converged to its E;
	// neither document crosses back into the conductor.
	SourceSandboxConfig *rtconfig.PortableSandboxConfig
	SourceImageConfig   []byte
	Summary             configsock.ArtifactPrepareSummary
	RefLocationURIs     map[string]string
	CarrierBindings     []CarrierBinding
	ConfigReadDuration  time.Duration
	PrepareDuration     time.Duration
}

type canonicalLocation struct {
	Name string `json:"name"`
	Path string `json:"path"`
	URI  string `json:"uri"`
}

type canonicalResolution struct {
	SchemaVersion        int                         `json:"schema_version"`
	SourceKind           string                      `json:"source_kind"`
	SourceRef            string                      `json:"source_ref"`
	LaunchMode           string                      `json:"launch_mode"`
	ReadImageConfig      bool                        `json:"read_image_config"`
	PreflightImageBundle bool                        `json:"preflight_image_bundle"`
	PublicationParent    string                      `json:"publication_parent,omitempty"`
	PreparedSourceKind   string                      `json:"prepared_source_kind"`
	PreparedSourceRef    string                      `json:"prepared_source_ref"`
	HasBuildCommands     bool                        `json:"has_build_commands,omitempty"`
	Capacity             configsock.ArtifactCapacity `json:"capacity"`
	Network              configsock.ArtifactNetwork  `json:"network"`
	DiskTopology         types.ArtifactDiskTopology  `json:"disk_topology"`
	RequiredRefs         []string                    `json:"required_refs"`
	Locations            []canonicalLocation         `json:"locations"`
	CarrierBindings      []CarrierBinding            `json:"carrier_bindings,omitempty"`
}

// Prepare opens the claimed E/S root, validates its logical role, resolves the
// launch-mode-specific source, and computes the exact ref closure. It does not
// start a runner, VM, network, or any other runtime side effect.
func Prepare(ctx context.Context, spec configsock.ArtifactPrepareSpec) (*Result, error) {
	started := time.Now()
	sourceKind := types.ResumeSourceKind(spec.RootSourceKind)
	launchMode := types.LaunchMode(spec.LaunchMode)
	if err := validateRequest(sourceKind, spec.RootRef, launchMode); err != nil {
		return nil, err
	}
	maxRefs := spec.MaxRefs
	if maxRefs == 0 {
		maxRefs = DefaultMaxRefs
	}
	if maxRefs < 1 || maxRefs > HardMaxRefs {
		return nil, fmt.Errorf("task artifact prepare: max_refs=%d outside 1..%d", maxRefs, HardMaxRefs)
	}
	if err := validateRootResolution(spec.RootRef, spec.RelativeDir); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	manifestCfg, err := rtconfig.LoadManifestConfig(spec.ManifestConfig)
	if err != nil {
		if !errors.Is(err, manifest.ErrConfigNotProvided) {
			return nil, fmt.Errorf("task artifact prepare: manifest config: %w", err)
		}
		manifestCfg = nil
	}
	if spec.PreflightImageBundle {
		if manifestCfg == nil {
			return nil, errors.New("task artifact prepare: image Bundle preflight requires manifest_config")
		}
		probeName := reflocation.PublicationName("build-preflight")
		if _, err := reflocation.Resolve(spec.RefLocationParent, probeName); err != nil {
			return nil, fmt.Errorf("task artifact prepare: image Bundle location: %w", err)
		}
		admission, err := manifestCfg.WriteAdmission(ctx)
		if err != nil {
			return nil, fmt.Errorf("task artifact prepare: image Bundle write admission: %w", err)
		}
		if err := artifact.ValidateSingleRootBundlePublication(manifestCfg, manifestCfg.CustomerKey, admission); err != nil {
			return nil, fmt.Errorf("task artifact prepare: image Bundle preflight: %w", err)
		}
	}

	pathLocations := rtconfig.RefLocations{}
	uriLocations := map[string]string{}
	addLocation := func(source, name string) error {
		if name == "" {
			return nil
		}
		if _, exists := uriLocations[name]; exists {
			return nil
		}
		location, err := reflocation.Resolve(spec.RefLocationParent, name)
		if err != nil {
			return fmt.Errorf("task artifact prepare: %s location %q: %w", source, name, err)
		}
		pathLocations[name] = location.Path
		uriLocations[name] = location.URI
		return nil
	}
	if name, err := refLocationName(spec.RootRef); err != nil {
		return nil, fmt.Errorf("task artifact prepare: root ref: %w", err)
	} else if err := addLocation("root", name); err != nil {
		return nil, err
	}

	bundleRefs, binding, err := rootCarrier(spec.RootRef, spec.RelativeDir, pathLocations)
	if err != nil {
		return nil, err
	}
	bindings := []CarrierBinding(nil)
	if binding.Format != "" {
		bindings = append(bindings, binding)
	}
	// A cold S source is only a selector for E. Do not resolve the Bundle's
	// memory/ref-location closure; after E has been selected only E and its disk
	// graph may reach the runner. Memory restore still needs the complete S
	// carrier closure.
	if launchMode == types.LaunchMemory {
		for _, raw := range bundleRefs {
			name, err := refLocationName(raw)
			if err != nil {
				return nil, fmt.Errorf("task artifact prepare: Bundle ref %q: %w", raw, err)
			}
			if err := addLocation(fmt.Sprintf("Bundle ref %q", raw), name); err != nil {
				return nil, err
			}
		}
	}

	readStarted := time.Now()
	var (
		prepared            types.ResumeSource
		sourceSandboxConfig *rtconfig.PortableSandboxConfig
		sourceImageConfig   []byte
		capacity            configsock.ArtifactCapacity
		network             configsock.ArtifactNetwork
		topology            types.ArtifactDiskTopology
		refs                []string
	)
	switch sourceKind {
	case types.ResumeSourceSnapshot:
		reader, err := restore.NewSnapshotCfgReader(manifestCfg)
		if err != nil {
			return nil, fmt.Errorf("task artifact prepare: initialize Snapshot reader: %w", err)
		}
		document, readErr := reader.Read(ctx, spec.RootRef, restore.SnapshotCfgReadOptions{
			RefLocations: pathLocations,
			RelativeDir:  spec.RelativeDir,
		})
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			if readErr != nil {
				readErr = fmt.Errorf("task artifact prepare: read Snapshot S: %w", readErr)
			}
			if closeErr != nil {
				closeErr = fmt.Errorf("task artifact prepare: close Snapshot reader: %w", closeErr)
			}
			return nil, errors.Join(readErr, closeErr)
		}
		selectedSandbox, err := selectSandboxFromSnapshot(spec.RootRef, document.Config.SandboxRef, spec.RelativeDir, pathLocations, binding)
		if err != nil {
			return nil, err
		}
		if name, locationErr := refLocationName(selectedSandbox); locationErr != nil {
			return nil, fmt.Errorf("task artifact prepare: selected Sandbox E ref: %w", locationErr)
		} else if locationErr := addLocation("selected Sandbox E", name); locationErr != nil {
			return nil, locationErr
		}
		sourceStorage, err := artifact.NewProcessStorage(manifestCfg)
		if err != nil {
			return nil, fmt.Errorf("task artifact prepare: initialize referenced Sandbox reader: %w", err)
		}
		sandboxCfg, imageConfig, inspectErr := inspectSandboxSource(
			ctx, selectedSandbox, spec.RelativeDir, sourceStorage, pathLocations,
			spec.ReadSourceImageConfig, addLocation,
		)
		closeErr = sourceStorage.Close()
		if inspectErr != nil || closeErr != nil {
			return nil, errors.Join(inspectErr, closeErr)
		}
		sourceSandboxConfig, sourceImageConfig = sandboxCfg, imageConfig
		capacity = configsock.ArtifactCapacity{
			CPU: sandboxCfg.Resources.Capacity.CPU, Memory: sandboxCfg.Resources.Capacity.Memory,
			AllocatableCPU:    sandboxCfg.Resources.Allocatable.CPU,
			AllocatableMemory: sandboxCfg.Resources.Allocatable.Memory,
			DeflateOnOOM:      cloneBool(sandboxCfg.Resources.Allocatable.DeflateOnOOM),
		}
		network, err = summarizeNetwork(sandboxCfg.Metadata[sandboxcfg.NsNetwork])
		if err != nil {
			return nil, fmt.Errorf("task artifact prepare: Sandbox E network metadata: %w", err)
		}
		topology, err = summarizeDiskTopology(sandboxCfg)
		if err != nil {
			return nil, err
		}
		if launchMode == types.LaunchMemory {
			prepared = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: spec.RootRef}
			refs = append(refs, spec.RootRef)
			refs = append(refs, document.Config.FromRefs...)
		} else {
			prepared = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: selectedSandbox}
		}
		refs = append(refs, selectedSandbox)
		refs = append(refs, portableArtifactRefs(sandboxCfg)...)
	case types.ResumeSourceSandbox:
		storage, err := artifact.NewProcessStorage(manifestCfg)
		if err != nil {
			return nil, fmt.Errorf("task artifact prepare: initialize Sandbox reader: %w", err)
		}
		sandboxCfg, imageConfig, readErr := inspectSandboxSource(
			ctx, spec.RootRef, spec.RelativeDir, storage, pathLocations,
			spec.ReadSourceImageConfig, addLocation,
		)
		closeErr := storage.Close()
		if readErr != nil || closeErr != nil {
			return nil, errors.Join(readErr, closeErr)
		}
		sourceSandboxConfig, sourceImageConfig = sandboxCfg, imageConfig
		prepared = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: spec.RootRef}
		capacity = configsock.ArtifactCapacity{
			CPU: sandboxCfg.Resources.Capacity.CPU, Memory: sandboxCfg.Resources.Capacity.Memory,
			AllocatableCPU:    sandboxCfg.Resources.Allocatable.CPU,
			AllocatableMemory: sandboxCfg.Resources.Allocatable.Memory,
			DeflateOnOOM:      cloneBool(sandboxCfg.Resources.Allocatable.DeflateOnOOM),
		}
		network, err = summarizeNetwork(sandboxCfg.Metadata[sandboxcfg.NsNetwork])
		if err != nil {
			return nil, fmt.Errorf("task artifact prepare: Sandbox E network metadata: %w", err)
		}
		topology, err = summarizeDiskTopology(sandboxCfg)
		if err != nil {
			return nil, err
		}
		refs = append(refs, spec.RootRef)
		refs = append(refs, portableArtifactRefs(sandboxCfg)...)
	}
	readDuration := time.Since(readStarted)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	requiredRefs, err := canonicalRefs(refs, maxRefs)
	if err != nil {
		return nil, err
	}
	for _, raw := range requiredRefs {
		name, err := refLocationName(raw)
		if err != nil {
			return nil, fmt.Errorf("task artifact prepare: required ref %q: %w", raw, err)
		}
		if err := addLocation(fmt.Sprintf("required ref %q", raw), name); err != nil {
			return nil, err
		}
	}
	// Discard locations used only while selecting E (notably the source S
	// location). The final task handoff contains exactly the selected source and
	// its required graph, so a cold build/run cannot accidentally pass memory
	// locations into a phase.
	if launchMode == types.LaunchCold {
		reachableLocations := make(map[string]struct{})
		for _, raw := range append(append([]string(nil), requiredRefs...), prepared.Ref) {
			name, err := refLocationName(raw)
			if err != nil {
				return nil, fmt.Errorf("task artifact prepare: reachable ref %q: %w", raw, err)
			}
			if name != "" {
				reachableLocations[name] = struct{}{}
			}
		}
		for name := range uriLocations {
			if _, reachable := reachableLocations[name]; !reachable {
				delete(uriLocations, name)
			}
		}
	}

	names := make([]string, 0, len(uriLocations))
	for name := range uriLocations {
		names = append(names, name)
	}
	sort.Strings(names)
	canonicalLocations := make([]canonicalLocation, 0, len(names))
	for _, name := range names {
		canonicalLocations = append(canonicalLocations, canonicalLocation{
			Name: name, Path: pathLocations[name], URI: uriLocations[name],
		})
	}
	// This capability is Build-only. Ordinary Sandbox tasks retain their wire
	// summary and digest, including when E has e2b command metadata.
	hasBuildCommands := spec.ReadSourceImageConfig && sourceSandboxConfig != nil &&
		(sourceSandboxConfig.Metadata[sandboxcfg.E2BStartCommandMetadata] != "" || sourceSandboxConfig.Metadata[sandboxcfg.E2BReadyCommandMetadata] != "")
	canonical := canonicalResolution{
		SchemaVersion: configsock.ArtifactPrepareSchemaVersion,
		SourceKind:    string(sourceKind), SourceRef: spec.RootRef, LaunchMode: string(launchMode),
		ReadImageConfig:      spec.ReadSourceImageConfig,
		PreflightImageBundle: spec.PreflightImageBundle,
		PublicationParent:    spec.RefLocationParent,
		PreparedSourceKind:   string(prepared.Kind), PreparedSourceRef: prepared.Ref,
		HasBuildCommands: hasBuildCommands,
		Capacity:         capacity, Network: network, DiskTopology: topology, RequiredRefs: requiredRefs,
		Locations: canonicalLocations, CarrierBindings: bindings,
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("task artifact prepare: canonical resolution: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return &Result{
		PreparedSource:      prepared,
		SourceSandboxConfig: sourceSandboxConfig,
		SourceImageConfig:   append([]byte(nil), sourceImageConfig...),
		Summary: configsock.ArtifactPrepareSummary{
			SchemaVersion:      configsock.ArtifactPrepareSchemaVersion,
			PreparedSourceKind: string(prepared.Kind), Capacity: capacity,
			HasBuildCommands: hasBuildCommands,
			Network:          network, DiskTopology: topology, ResolutionDigest: hex.EncodeToString(digest[:]),
			RequiredRefCount: len(requiredRefs),
		},
		RefLocationURIs: uriLocations, CarrierBindings: bindings,
		ConfigReadDuration: readDuration, PrepareDuration: time.Since(started),
	}, nil
}

func summarizeNetwork(raw string) (configsock.ArtifactNetwork, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return configsock.ArtifactNetwork{}, nil
	}
	if len(trimmed) > maxArtifactNetworkMetadataSize {
		return configsock.ArtifactNetwork{}, fmt.Errorf("metadata exceeds %d bytes", maxArtifactNetworkMetadataSize)
	}
	if trimmed[0] != '{' {
		return configsock.ArtifactNetwork{}, errors.New("metadata must be a JSON object")
	}
	var parsed sandboxcfg.NetworkSpec
	if err := strictjson.Decode([]byte(trimmed), &parsed); err != nil {
		return configsock.ArtifactNetwork{}, err
	}
	if err := sandboxcfg.ValidateNetworkSpec(parsed); err != nil {
		return configsock.ArtifactNetwork{}, err
	}
	return configsock.ArtifactNetwork{
		Hostname: parsed.Hostname, DNS: append([]string(nil), parsed.DNS...),
		InnerIP: parsed.InnerIP, Nexthop: parsed.Nexthop,
		TransitGatewayIP: parsed.TransitGatewayIP, TransitGeneveVNI: parsed.TransitGeneveVNI,
		TransitMAC: parsed.TransitMAC,
	}, nil
}

func inspectSandboxSource(
	ctx context.Context,
	raw, relativeDir string,
	storage *artifact.ProcessStorage,
	locations rtconfig.RefLocations,
	loadImageConfig bool,
	addLocation func(string, string) error,
) (*rtconfig.PortableSandboxConfig, []byte, error) {
	if storage == nil {
		return nil, nil, errors.New("task artifact prepare: initialize referenced Sandbox reader")
	}
	stream, err := openArtifactStream(ctx, raw, relativeDir, storage, locations)
	if err != nil {
		return nil, nil, fmt.Errorf("task artifact prepare: referenced Sandbox E: %w", err)
	}
	root, err := sandboxfile.Open(ctx, stream)
	if err != nil {
		return nil, nil, fmt.Errorf("task artifact prepare: read referenced Sandbox E: %w", err)
	}
	portable, err := root.Portable.Clone()
	imageConfig := append([]byte(nil), root.ImageConfig...)
	if err == nil && addLocation != nil {
		err = addSourceImageRefLocation(portable, addLocation)
	}
	if err == nil && loadImageConfig {
		var scoped fetch.Fetcher
		if provider, ok := stream.(interface{ ScopedFetcher() fetch.Fetcher }); ok {
			scoped = provider.ScopedFetcher()
		}
		imageConfig, err = inspectSourceImageConfig(
			ctx, portable, imageConfig, sourceRelativeDir(raw, relativeDir, locations),
			storage, locations, scoped,
		)
	}
	closeErr := root.Close()
	if err != nil || closeErr != nil {
		return nil, nil, errors.Join(err, closeErr)
	}
	return portable, imageConfig, nil
}

func addSourceImageRefLocation(cfg *rtconfig.PortableSandboxConfig, add func(string, string) error) error {
	raw := sourceRootImageRef(cfg)
	if raw == "" {
		return nil
	}
	name, err := refLocationName(raw)
	if err != nil {
		return fmt.Errorf("task artifact prepare: Sandbox E base image ref %q: %w", raw, err)
	}
	return add(fmt.Sprintf("Sandbox E base image ref %q", raw), name)
}

// inspectSourceImageConfig preserves the container defaults used by Phase B.
// Top-level EROFS Sandboxes carry config.json in their own logical root; a
// captured incremental overlay E points at the flattened base image instead, so
// read that image through the same task-local carrier and crypto boundary.
func inspectSourceImageConfig(
	ctx context.Context,
	cfg *rtconfig.PortableSandboxConfig,
	embedded []byte,
	relativeDir string,
	storage *artifact.ProcessStorage,
	locations rtconfig.RefLocations,
	scoped fetch.Fetcher,
) ([]byte, error) {
	if embedded != nil {
		return append([]byte(nil), embedded...), nil
	}
	ref := sourceRootImageRef(cfg)
	if ref == "" {
		return nil, nil
	}
	stream, err := openArtifactStreamWithFetcher(ctx, ref, relativeDir, storage, locations, scoped)
	if err != nil {
		return nil, fmt.Errorf("task artifact prepare: open Sandbox E base image: %w", err)
	}
	payload, _, err := sandboxfile.PayloadIfSandbox(ctx, stream, false)
	if err != nil {
		return nil, fmt.Errorf("task artifact prepare: narrow Sandbox E base image: %w", err)
	}
	image, err := sandboxfile.OpenEROFSArtifact(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("task artifact prepare: read Sandbox E base image: %w", err)
	}
	config := append([]byte(nil), image.ImageConfig...)
	if err := image.Close(); err != nil {
		return nil, fmt.Errorf("task artifact prepare: close Sandbox E base image: %w", err)
	}
	return config, nil
}

// sourceRootImageRef returns the explicitly typed immutable root-image carrier.
// Only overlay mode has one: single-disk roots are ext4 chains and require an
// explicit launch command instead of container image defaults.
func sourceRootImageRef(cfg *rtconfig.PortableSandboxConfig) string {
	if cfg == nil {
		return ""
	}
	root := cfg.Boot.Root
	if root.Overlay == nil || root.Base == "" || root.Base == "self" {
		return ""
	}
	return root.Base
}

func openArtifactStream(ctx context.Context, raw, relativeDir string, storage *artifact.ProcessStorage, locations rtconfig.RefLocations) (fetch.Stream, error) {
	return openArtifactStreamWithFetcher(ctx, raw, relativeDir, storage, locations, nil)
}

func openArtifactStreamWithFetcher(
	ctx context.Context,
	raw, relativeDir string,
	storage *artifact.ProcessStorage,
	locations rtconfig.RefLocations,
	manifestFetcher fetch.Fetcher,
) (fetch.Stream, error) {
	ref, err := parseArtifactRef(raw, relativeDir)
	if err != nil {
		return nil, err
	}
	switch ref.Scheme {
	case manifest.RefSchemeManifest:
		if manifestFetcher == nil {
			manifestFetcher = storage.Fetcher()
		}
		if manifestFetcher == nil {
			return nil, errors.New("manifest configuration is required")
		}
		key, err := manifest.ParseKeyRef(ref.Path)
		if err != nil {
			return nil, err
		}
		return manifestFetcher.OpenManifest(ctx, key)
	case manifest.RefSchemeFile:
		path, err := locations.ResolveFile(ref, relativeDir)
		if err != nil {
			return nil, err
		}
		if !filepath.IsAbs(path) {
			path, err = filepath.Abs(path)
			if err != nil {
				return nil, err
			}
		}
		return storage.OpenFileWithLocations(ctx, path, ref, locations)
	default:
		return nil, fmt.Errorf("unsupported ref scheme %q", ref.Scheme)
	}
}

func sourceRelativeDir(raw, fallback string, locations rtconfig.RefLocations) string {
	if strings.HasPrefix(raw, "file://") {
		ref, err := manifest.ParseRef(raw)
		if err == nil {
			if path, resolveErr := locations.ResolveFile(ref, fallback); resolveErr == nil {
				if absolute, absoluteErr := filepath.Abs(path); absoluteErr == nil {
					return filepath.Dir(absolute)
				}
			}
		}
	} else if filepath.IsAbs(raw) {
		return filepath.Dir(raw)
	}
	return fallback
}

func parseArtifactRef(raw, relativeDir string) (manifest.Ref, error) {
	if strings.HasPrefix(raw, "manifest://") || strings.HasPrefix(raw, "file://") {
		return manifest.ParseRef(raw)
	}
	path := raw
	if !filepath.IsAbs(path) {
		path = filepath.Join(relativeDir, path)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return manifest.Ref{}, err
	}
	if _, err := os.Stat(absolute); err != nil {
		return manifest.Ref{}, err
	}
	return manifest.Ref{Scheme: manifest.RefSchemeFile, Path: absolute}, nil
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

func summarizeDiskTopology(cfg *rtconfig.PortableSandboxConfig) (types.ArtifactDiskTopology, error) {
	if cfg == nil {
		return types.ArtifactDiskTopology{}, errors.New("task artifact prepare: Sandbox E has no portable disk topology")
	}
	if err := cfg.Validate(); err != nil {
		return types.ArtifactDiskTopology{}, fmt.Errorf("task artifact prepare: Sandbox E portable config: %w", err)
	}
	shape := func(name string, root rtconfig.PortableRootConfig) types.ArtifactDiskShape {
		if root.Overlay == nil {
			return types.ArtifactDiskShape{
				Name: name, Mode: types.ArtifactDiskSingle, HasActiveBase: root.Base != "",
			}
		}
		return types.ArtifactDiskShape{
			Name: name, Mode: types.ArtifactDiskOverlay, HasActiveBase: root.Overlay.Base != "",
		}
	}
	topology := types.ArtifactDiskTopology{Root: shape("", cfg.Boot.Root)}
	topology.Disks = make([]types.ArtifactDiskShape, len(cfg.Boot.Disks))
	for i := range cfg.Boot.Disks {
		disk := &cfg.Boot.Disks[i]
		topology.Disks[i] = shape(disk.Name, disk.PortableRootConfig)
	}
	return topology, nil
}

func validateRequest(kind types.ResumeSourceKind, root string, mode types.LaunchMode) error {
	if strings.TrimSpace(root) == "" {
		return errors.New("task artifact prepare: root ref is empty")
	}
	switch kind {
	case types.ResumeSourceSandbox:
		if mode != types.LaunchCold {
			return errors.New("task artifact prepare: Sandbox E requires cold launch mode")
		}
	case types.ResumeSourceSnapshot:
		if mode != types.LaunchCold && mode != types.LaunchMemory {
			return errors.New("task artifact prepare: Snapshot S requires cold or memory launch mode")
		}
	default:
		return fmt.Errorf("task artifact prepare: unsupported source kind %q", kind)
	}
	return nil
}

func canonicalRefs(candidates []string, maxRefs int) ([]string, error) {
	seen := make(map[string]struct{}, len(candidates))
	refs := make([]string, 0, len(candidates))
	for _, raw := range candidates {
		if raw == "" || raw == "self" {
			continue
		}
		if _, ok := seen[raw]; ok {
			continue
		}
		seen[raw] = struct{}{}
		refs = append(refs, raw)
		if len(refs) > maxRefs {
			return nil, fmt.Errorf("task artifact prepare: required refs exceed %d entries", maxRefs)
		}
	}
	sort.Strings(refs)
	return refs, nil
}

func portableArtifactRefs(cfg *rtconfig.PortableSandboxConfig) []string {
	if cfg == nil {
		return nil
	}
	refs := make([]string, 0)
	appendRoot := func(root rtconfig.PortableRootConfig) {
		if root.Base != "" && root.Base != "self" {
			refs = append(refs, root.Base)
		}
		refs = append(refs, root.BaseFromRefs...)
		if root.Overlay != nil {
			if root.Overlay.Base != "" && root.Overlay.Base != "self" {
				refs = append(refs, root.Overlay.Base)
			}
			refs = append(refs, root.Overlay.BaseFromRefs...)
		}
	}
	appendRoot(cfg.Boot.Root)
	for i := range cfg.Boot.Disks {
		appendRoot(cfg.Boot.Disks[i].PortableRootConfig)
	}
	return refs
}

func selectSandboxFromSnapshot(root, sandboxRef, relativeDir string, locations rtconfig.RefLocations, binding CarrierBinding) (string, error) {
	eRef, err := manifest.ParseRef(sandboxRef)
	if err != nil {
		return "", fmt.Errorf("task artifact prepare: Snapshot sandbox_ref: %w", err)
	}
	if binding.Format == "bundle" && eRef.Scheme == manifest.RefSchemeManifest {
		rootRef, err := physicalRootFileRef(root, relativeDir, locations)
		if err != nil {
			return "", err
		}
		rootRef.DigestScheme = "manifest"
		rootRef.Digest = eRef.Path
		if err := rootRef.Validate(); err != nil {
			return "", fmt.Errorf("task artifact prepare: Bundle Sandbox selector: %w", err)
		}
		return rootRef.String(), nil
	}
	if eRef.Scheme == manifest.RefSchemeFile && eRef.Location == "" && !filepath.IsAbs(eRef.Path) {
		rootPath, isFile, err := rootFilePath(root, relativeDir, locations)
		if err != nil {
			return "", err
		}
		if !isFile {
			return "", errors.New("task artifact prepare: relative Sandbox E ref has no local Snapshot parent")
		}
		eRef.Path = filepath.Join(filepath.Dir(rootPath), eRef.Path)
		if err := eRef.Validate(); err != nil {
			return "", fmt.Errorf("task artifact prepare: resolve local Sandbox E: %w", err)
		}
		return eRef.String(), nil
	}
	return eRef.String(), nil
}

func physicalRootFileRef(root, relativeDir string, locations rtconfig.RefLocations) (manifest.Ref, error) {
	if strings.HasPrefix(root, "file://") {
		ref, err := manifest.ParseRef(root)
		if err != nil {
			return manifest.Ref{}, err
		}
		ref.DigestScheme, ref.Digest = "", ""
		return ref, nil
	}
	path, isFile, err := rootFilePath(root, relativeDir, locations)
	if err != nil {
		return manifest.Ref{}, err
	}
	if !isFile {
		return manifest.Ref{}, errors.New("task artifact prepare: Bundle root has no file carrier")
	}
	return manifest.Ref{Scheme: manifest.RefSchemeFile, Path: path}, nil
}

func validateRootResolution(raw, relativeDir string) error {
	if strings.HasPrefix(raw, "manifest://") {
		_, err := manifest.ParseRef(raw)
		return err
	}
	if strings.HasPrefix(raw, "file://") {
		ref, err := manifest.ParseRef(raw)
		if err != nil {
			return err
		}
		if ref.Location == "" && !filepath.IsAbs(ref.Path) && !filepath.IsAbs(relativeDir) {
			return errors.New("task artifact prepare: relative root file ref requires an absolute relative_dir")
		}
		return nil
	}
	if !filepath.IsAbs(raw) {
		return errors.New("task artifact prepare: raw local root must be absolute")
	}
	return nil
}

func refLocationName(raw string) (string, error) {
	if !strings.HasPrefix(raw, "file://") && !strings.HasPrefix(raw, "manifest://") {
		return "", nil
	}
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		return "", err
	}
	return ref.Location, nil
}

func rootCarrier(root, relativeDir string, locations rtconfig.RefLocations) ([]string, CarrierBinding, error) {
	path, isFile, err := rootFilePath(root, relativeDir, locations)
	if err != nil || !isFile {
		return nil, CarrierBinding{}, err
	}
	format, err := artifact.DetectFileFormat(path)
	if err != nil {
		return nil, CarrierBinding{}, fmt.Errorf("task artifact prepare: inspect root file format: %w", err)
	}
	if format != artifact.FileFormatManifestBundle {
		return nil, CarrierBinding{Format: "tarstream", FilePath: path}, nil
	}
	metadata, err := manifestbundle.OpenMetadata(path)
	if err != nil {
		return nil, CarrierBinding{}, fmt.Errorf("task artifact prepare: read root Bundle metadata: %w", err)
	}
	binding := CarrierBinding{Format: "bundle", FilePath: path}
	if parsed, err := manifest.ParseRef(root); err == nil && parsed.DigestScheme == "manifest" {
		binding.RootManifestKey = parsed.Digest
	}
	return metadata.Refs(), binding, nil
}

func rootFilePath(root, relativeDir string, locations rtconfig.RefLocations) (string, bool, error) {
	if strings.HasPrefix(root, "manifest://") {
		return "", false, nil
	}
	if strings.HasPrefix(root, "file://") {
		ref, err := manifest.ParseRef(root)
		if err != nil {
			return "", false, fmt.Errorf("task artifact prepare: parse root file ref: %w", err)
		}
		path, err := locations.ResolveFile(ref, relativeDir)
		if err != nil {
			return "", false, fmt.Errorf("task artifact prepare: resolve root file ref: %w", err)
		}
		return path, true, nil
	}
	return root, true, nil
}
