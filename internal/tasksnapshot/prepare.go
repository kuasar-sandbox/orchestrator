// Package tasksnapshot performs one tenant-bound task-local root snapshot
// preparation. It deliberately has no conductor policy, parent traversal, or
// cross-task cache.
package tasksnapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	manifestbundle "github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
)

const (
	DefaultMaxRefs = 1024
	HardMaxRefs    = 1024
)

type Result struct {
	RootCfg            *restore.SnapshotCfg
	Summary            configsock.SnapshotPrepareSummary
	RefLocationURIs    map[string]string
	ConfigReadDuration time.Duration
	PrepareDuration    time.Duration
}

type canonicalLocation struct {
	Name string `json:"name"`
	Path string `json:"path"`
	URI  string `json:"uri"`
}

type canonicalResolution struct {
	SchemaVersion      int                         `json:"schema_version"`
	RootRef            string                      `json:"root_ref"`
	Capacity           configsock.SnapshotCapacity `json:"capacity"`
	RawNetworkMetadata string                      `json:"raw_network_metadata"`
	RequiredRefs       []string                    `json:"required_refs"`
	Locations          []canonicalLocation         `json:"locations"`
}

// Prepare reads the root config exactly once, computes the flattened logical
// ref closure, and derives reader paths plus CLI URIs. A file-backed root is
// content-detected first; Manifest Bundles contribute only their metadata
// prefix refs. Prepare never opens a listed Bundle or a parent snapshot.cfg.
func Prepare(ctx context.Context, spec configsock.SnapshotPrepareSpec) (*Result, error) {
	started := time.Now()
	if spec.RootRef == "" {
		return nil, errors.New("task snapshot prepare: root ref is empty")
	}
	maxRefs := spec.MaxRefs
	if maxRefs == 0 {
		maxRefs = DefaultMaxRefs
	}
	if maxRefs < 1 || maxRefs > HardMaxRefs {
		return nil, fmt.Errorf("task snapshot prepare: max_refs=%d outside 1..%d", maxRefs, HardMaxRefs)
	}
	if err := validateRootResolution(spec.RootRef, spec.RelativeDir); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
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
			return fmt.Errorf("task snapshot prepare: %s location %q: %w", source, name, err)
		}
		pathLocations[name] = location.Path
		uriLocations[name] = location.URI
		return nil
	}
	if name, err := refLocationName(spec.RootRef); err != nil {
		return nil, fmt.Errorf("task snapshot prepare: root ref: %w", err)
	} else if err := addLocation("root", name); err != nil {
		return nil, err
	}

	bundleRefs, err := rootBundleRefs(spec.RootRef, spec.RelativeDir, pathLocations)
	if err != nil {
		return nil, err
	}
	for _, raw := range bundleRefs {
		name, err := refLocationName(raw)
		if err != nil {
			return nil, fmt.Errorf("task snapshot prepare: Bundle ref %q: %w", raw, err)
		}
		if err := addLocation(fmt.Sprintf("Bundle ref %q", raw), name); err != nil {
			return nil, err
		}
	}

	manifestCfg, err := rtconfig.LoadManifestConfig(spec.ManifestConfig)
	if err != nil {
		if !errors.Is(err, manifest.ErrConfigNotProvided) {
			return nil, fmt.Errorf("task snapshot prepare: manifest config: %w", err)
		}
		manifestCfg = nil
	}
	reader, err := restore.NewSnapshotCfgReader(manifestCfg)
	if err != nil {
		return nil, fmt.Errorf("task snapshot prepare: initialize reader: %w", err)
	}
	readStarted := time.Now()
	document, readErr := reader.Read(ctx, spec.RootRef, restore.SnapshotCfgReadOptions{
		RefLocations: pathLocations,
		RelativeDir:  spec.RelativeDir,
	})
	readDuration := time.Since(readStarted)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		if readErr != nil {
			readErr = fmt.Errorf("task snapshot prepare: read root snapshot.cfg: %w", readErr)
		}
		if closeErr != nil {
			closeErr = fmt.Errorf("task snapshot prepare: close reader: %w", closeErr)
		}
		return nil, errors.Join(readErr, closeErr)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	requiredRefs, err := requiredRefs(spec.RootRef, document.Config, maxRefs)
	if err != nil {
		return nil, err
	}
	for _, raw := range requiredRefs {
		name, err := refLocationName(raw)
		if err != nil {
			return nil, fmt.Errorf("task snapshot prepare: required ref %q: %w", raw, err)
		}
		if err := addLocation(fmt.Sprintf("required ref %q", raw), name); err != nil {
			return nil, err
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
			Name: name,
			Path: pathLocations[name],
			URI:  uriLocations[name],
		})
	}

	capacity := configsock.SnapshotCapacity{
		CPU:    document.Config.Resources.Capacity.CPU,
		Memory: document.Config.Resources.Capacity.Memory,
	}
	rawNetwork := document.Config.Metadata[sandboxcfg.NsNetwork]
	canonical := canonicalResolution{
		SchemaVersion:      configsock.SnapshotPrepareSchemaVersion,
		RootRef:            spec.RootRef,
		Capacity:           capacity,
		RawNetworkMetadata: rawNetwork,
		RequiredRefs:       requiredRefs,
		Locations:          canonicalLocations,
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("task snapshot prepare: canonical resolution: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return &Result{
		RootCfg: document.Config,
		Summary: configsock.SnapshotPrepareSummary{
			SchemaVersion:      configsock.SnapshotPrepareSchemaVersion,
			Capacity:           capacity,
			RawNetworkMetadata: rawNetwork,
			ResolutionDigest:   hex.EncodeToString(digest[:]),
			RequiredRefCount:   len(requiredRefs),
		},
		RefLocationURIs:    uriLocations,
		ConfigReadDuration: readDuration,
		PrepareDuration:    time.Since(started),
	}, nil
}

func requiredRefs(root string, cfg *restore.SnapshotCfg, maxRefs int) ([]string, error) {
	if cfg == nil {
		return nil, errors.New("task snapshot prepare: root config is nil")
	}
	candidates := make([]string, 0, 1+len(cfg.FromRefs)+len(cfg.ArtifactRefs()))
	candidates = append(candidates, root)
	candidates = append(candidates, cfg.FromRefs...)
	candidates = append(candidates, cfg.ArtifactRefs()...)
	seen := make(map[string]struct{}, len(candidates))
	refs := make([]string, 0, len(candidates))
	for _, raw := range candidates {
		if raw == "" {
			continue
		}
		if _, ok := seen[raw]; ok {
			continue
		}
		seen[raw] = struct{}{}
		refs = append(refs, raw)
		if len(refs) > maxRefs {
			return nil, fmt.Errorf("task snapshot prepare: required refs exceed %d entries", maxRefs)
		}
	}
	sort.Strings(refs)
	return refs, nil
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
			return fmt.Errorf("task snapshot prepare: relative root file ref requires an absolute relative_dir")
		}
		return nil
	}
	if !filepath.IsAbs(raw) {
		return fmt.Errorf("task snapshot prepare: raw local root must be absolute")
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

// rootBundleRefs discovers the root Bundle's flat physical search path from
// its metadata prefix. It deliberately does not open any listed Bundle: those
// sources remain lazy sandbox-runtime dependencies, and their own refs never
// participate in this root's location discovery.
func rootBundleRefs(root, relativeDir string, locations rtconfig.RefLocations) ([]string, error) {
	path, isFile, err := rootFilePath(root, relativeDir, locations)
	if err != nil {
		return nil, err
	}
	if !isFile {
		return nil, nil
	}
	format, err := artifact.DetectFileFormat(path)
	if err != nil {
		return nil, fmt.Errorf("task snapshot prepare: inspect root file format: %w", err)
	}
	if format != artifact.FileFormatManifestBundle {
		return nil, nil
	}
	metadata, err := manifestbundle.OpenMetadata(path)
	if err != nil {
		return nil, fmt.Errorf("task snapshot prepare: read root Bundle metadata: %w", err)
	}
	return metadata.Refs(), nil
}

func rootFilePath(root, relativeDir string, locations rtconfig.RefLocations) (string, bool, error) {
	if strings.HasPrefix(root, "manifest://") {
		// bundle/refs is deployment metadata, not a Store object. Exact Bundle
		// upload publishes the complete logical Manifest closure, so a Store
		// root is intentionally Store-only and has no Bundle locations to add.
		return "", false, nil
	}
	if strings.HasPrefix(root, "file://") {
		ref, err := manifest.ParseRef(root)
		if err != nil {
			return "", false, fmt.Errorf("task snapshot prepare: parse root file ref: %w", err)
		}
		path, err := locations.ResolveFile(ref, relativeDir)
		if err != nil {
			return "", false, fmt.Errorf("task snapshot prepare: resolve root file ref: %w", err)
		}
		return path, true, nil
	}
	return root, true, nil
}
