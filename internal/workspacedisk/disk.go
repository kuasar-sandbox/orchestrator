// Package workspacedisk resolves a tenant's Workspace disk selection to a
// complete sandboxer disk and a trusted, node-local location map.
package workspacedisk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

var (
	ErrSelection    = errors.New("invalid workspace disk")
	ErrUnauthorized = errors.New("workspace disk source is not authorized")
)

type Options struct {
	APISecret, ManifestKey                                 string
	ManifestConfig, RefLocationParent, OverlayDiffTemplate string
	DefaultDisk                                            *rtconfig.DiskV1
}

// Storage fixes the customer key per operation; a conductor can serve multiple
// tenants concurrently and must never set MANIFEST_KEY in its own environment.
func Storage(cfgPath, manifestKey string) (*artifact.ProcessStorage, error) {
	cfg, err := rtconfig.LoadManifestConfig(cfgPath)
	if err != nil && !errors.Is(err, manifest.ErrConfigNotProvided) {
		return nil, err
	}
	key, err := manifest.ParseHexKey(manifestKey)
	if err != nil {
		return nil, errors.New("invalid tenant manifest key")
	}
	return artifact.NewProcessStorageWithCustomerKey(cfg, func() ([32]byte, error) { return [32]byte(key), nil })
}

func AddLocation(loc rtconfig.RefLocations, parent, raw string) error {
	if raw == "" {
		return nil
	}
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		return err
	}
	if ref.Location == "" {
		return nil
	}
	value, err := reflocation.Resolve(parent, ref.Location)
	if err != nil {
		return err
	}
	if prev, ok := loc[ref.Location]; ok && prev != value.Path {
		return errors.New("conflicting workspace location")
	}
	loc[ref.Location] = value.Path
	return nil
}

func Resolve(ctx context.Context, input json.RawMessage, o Options) (rtconfig.DiskV1, rtconfig.RefLocations, error) {
	d, locations, check, err := Plan(input, o)
	if err == nil && check {
		err = AuthorizeLayers(ctx, d, locations, o)
	}
	return d, locations, err
}

// Plan fixes the source and rejects unsafe paths before accepting an operation.
// Layer authentication can then run in the tenant worker before opening a VM.
func Plan(input json.RawMessage, o Options) (rtconfig.DiskV1, rtconfig.RefLocations, bool, error) {
	var fields map[string]json.RawMessage
	if strictjson.Decode(input, &fields) != nil || fields == nil {
		return rtconfig.DiskV1{}, nil, false, ErrSelection
	}
	var disk rtconfig.DiskV1
	trusted := false
	switch {
	case len(fields) == 0:
		if o.DefaultDisk != nil {
			disk = *o.DefaultDisk
		} else {
			if o.OverlayDiffTemplate == "" {
				return disk, nil, false, errors.New("workspace default disk is not configured")
			}
			path, err := filepath.Abs(o.OverlayDiffTemplate)
			if err != nil {
				return disk, nil, false, err
			}
			disk = rtconfig.DiskV1{Kind: "disk", Version: 1, DiffTemplate: (manifest.Ref{Scheme: manifest.RefSchemeFile, Path: path}).String()}
		}
		trusted = true
	case fields["migrationToken"] != nil:
		var token string
		if len(fields) != 1 || json.Unmarshal(fields["migrationToken"], &token) != nil || token == "" {
			return disk, nil, false, ErrSelection
		}
		var err error
		disk, err = migrationtoken.OpenWorkspace(migrationtoken.KeyMaterial{APISecret: o.APISecret, ManifestKey: o.ManifestKey}, token)
		if err != nil {
			return disk, nil, false, ErrUnauthorized
		}
		trusted = true
	default:
		if json.Unmarshal(input, &disk) != nil {
			return disk, nil, false, ErrSelection
		}
	}
	if err := disk.Validate(); err != nil {
		return rtconfig.DiskV1{}, nil, false, fmt.Errorf("%w: %v", ErrSelection, err)
	}
	locations := rtconfig.RefLocations{}
	for _, raw := range append([]string{disk.DiffTemplate, disk.Base}, disk.BaseFromRefs...) {
		if err := AddLocation(locations, o.RefLocationParent, raw); err != nil {
			return disk, nil, false, err
		}
	}
	if trusted {
		return disk, locations, false, nil
	}

	// A raw filesystem seed has no tenant-authenticated metadata. Only the
	// exact named seed that this node already offers as the default is public.
	if disk.DiffTemplate != "" {
		r, _ := manifest.ParseRef(disk.DiffTemplate)
		if r.Location == "" || o.DefaultDisk == nil || o.DefaultDisk.DiffTemplate != disk.DiffTemplate {
			return disk, nil, false, ErrUnauthorized
		}
	}
	for _, raw := range append([]string{disk.Base}, disk.BaseFromRefs...) {
		if raw == "" {
			continue
		}
		r, _ := manifest.ParseRef(raw)
		if r.Scheme == manifest.RefSchemeFile && (r.Location == "" || r.DigestScheme != "manifest") {
			return disk, nil, false, ErrUnauthorized
		}
	}
	return disk, locations, disk.Base != "", nil
}

func OpenLayer(ctx context.Context, s *artifact.ProcessStorage, r manifest.Ref, dir string, loc rtconfig.RefLocations) (fetch.Stream, error) {
	if r.Scheme == manifest.RefSchemeManifest {
		if s.Fetcher() == nil {
			return nil, errors.New("manifest storage is not configured")
		}
		key, err := manifest.ParseKeyRef(r.Path)
		if err != nil {
			return nil, err
		}
		return s.Fetcher().OpenManifest(ctx, key)
	}
	path, err := loc.ResolveFile(r, dir)
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(path) {
		return nil, errors.New("workspace file has no trusted directory")
	}
	return s.OpenFileWithLocations(ctx, path, r, loc)
}

// Qualify rewrites Bundle members into portable refs in this publication.
func Qualify(d *rtconfig.DiskV1, dir, name string) error {
	qualify := func(raw string) (string, error) {
		ref, err := manifest.ParseRef(raw)
		if err != nil || ref.Scheme != manifest.RefSchemeFile || ref.Location != "" || ref.DigestScheme != "manifest" || filepath.Dir(ref.Path) != dir {
			return "", fmt.Errorf("disk publication returned a layer outside its destination")
		}
		ref.Location = name
		ref.Path = filepath.Base(ref.Path)
		return ref.String(), nil
	}
	var err error
	if d.Base, err = qualify(d.Base); err != nil {
		return err
	}
	for i, raw := range d.BaseFromRefs {
		if d.BaseFromRefs[i], err = qualify(raw); err != nil {
			return err
		}
	}
	return nil
}

func AuthorizeLayers(ctx context.Context, disk rtconfig.DiskV1, locations rtconfig.RefLocations, o Options) error {
	st, err := Storage(o.ManifestConfig, o.ManifestKey)
	if err != nil {
		return err
	}
	defer st.Close()
	for _, raw := range append([]string{disk.Base}, disk.BaseFromRefs...) {
		r, _ := manifest.ParseRef(raw)
		if r.Scheme == manifest.RefSchemeFile && (r.Location == "" || r.DigestScheme != "manifest") {
			return ErrUnauthorized
		}
		layer, err := OpenLayer(ctx, st, r, "", locations)
		if err != nil {
			return ErrUnauthorized
		}
		if err := layer.Close(); err != nil {
			return err
		}
	}
	return nil
}
