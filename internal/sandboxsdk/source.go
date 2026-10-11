// Source binding mirrors sandbox-ctl run_from.go at sandboxer
// 509cf54032dc34157b2e54effa089fb5aff19763 (Apache-2.0).
// Keep canonical identity and physical Bundle selection distinct.
package sandboxsdk

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	manifestbundle "github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxsdk/readretry"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
)

type sandboxRunSource struct {
	Root          *sandboxfile.Root
	PortableRef   string
	RuntimeRef    string
	RelativeDir   string
	Fetcher       fetch.Fetcher
	BundleReader  *manifestbundle.Reader
	BundleFetcher *manifestbundle.ManifestFetcher
	BundleSource  *sandbox.BundleSourceBinding
}

func (s *sandboxRunSource) Close() error {
	if s == nil || s.Root == nil {
		return nil
	}
	return s.Root.Close()
}

func openSandboxRunSource(ctx context.Context, raw string, storage *artifact.ProcessStorage, locations config.RefLocations) (*sandboxRunSource, error) {
	if storage == nil {
		return nil, errors.New("process artifact storage is required")
	}
	ref, path, err := resolveRunSourceRef(raw, locations)
	if err != nil {
		return nil, err
	}
	source := &sandboxRunSource{Fetcher: storage.Fetcher()}
	var stream fetch.Stream
	switch ref.Scheme {
	case manifest.RefSchemeManifest:
		if source.Fetcher == nil {
			return nil, errors.New("manifest:// Sandbox requires --manifest-config or MANIFEST_CONFIG")
		}
		key, err := manifest.ParseKeyRef(ref.Path)
		if err != nil {
			return nil, fmt.Errorf("Sandbox manifest ref: %w", err)
		}
		stream, err = readretry.Open(ctx, func() (fetch.Stream, error) { return source.Fetcher.OpenManifest(ctx, key) })
		if err != nil {
			return nil, fmt.Errorf("open Sandbox manifest: %w", err)
		}
		source.PortableRef = ref.String()
		source.RuntimeRef = ref.String()
	case manifest.RefSchemeFile:
		opened, err := storage.OpenFileWithLocations(ctx, path, ref, locations)
		if err != nil {
			return nil, fmt.Errorf("open Sandbox file: %w", err)
		}
		stream = opened
		resolvedRef := ref
		if resolvedRef.Location == "" {
			resolvedRef.Path = path
		}
		source.RelativeDir = filepath.Dir(path)
		if rootKey, bundle := opened.RootManifestKey(); bundle {
			resolvedRef.DigestScheme = "manifest"
			resolvedRef.Digest = manifest.HexKey(rootKey)
			portable := resolvedRef
			portable.Path = filepath.Base(path)
			source.PortableRef = portable.String()
			source.Fetcher = opened.ScopedFetcher()
			source.BundleReader = opened.BundleReader()
			source.BundleFetcher = opened.ManifestFetcher()
			source.BundleSource, err = runBundleSourceBinding(path, ref, opened.BundleReader())
			if err != nil {
				_ = opened.Close()
				return nil, err
			}
			source.BundleSource.Reader = source.BundleReader
			source.BundleSource.Fetcher = source.BundleFetcher
		} else {
			digester, ok := any(opened).(tarstream.Digester)
			if !ok {
				_ = opened.Close()
				return nil, errors.New("local Sandbox has no declared content identity")
			}
			scheme, digest := digester.Digest()
			if scheme == "" || digest == "" {
				_ = opened.Close()
				return nil, errors.New("local Sandbox has an empty content identity")
			}
			resolvedRef.DigestScheme, resolvedRef.Digest = scheme, digest
			portable := resolvedRef
			portable.Path = digest + ".sandbox"
			source.PortableRef = portable.String()
		}
		source.RuntimeRef = resolvedRef.String()
	default:
		return nil, fmt.Errorf("unsupported Sandbox source scheme %q", ref.Scheme)
	}

	root, err := sandboxfile.Open(ctx, stream)
	if err != nil {
		return nil, err
	}
	source.Root = root
	return source, nil
}

func runBundleSourceBinding(path string, ref manifest.Ref, reader *manifestbundle.Reader) (*sandbox.BundleSourceBinding, error) {
	if reader == nil {
		return nil, errors.New("Sandbox Bundle reader is unavailable")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	realPath, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, err
	}
	realPath, err = filepath.Abs(realPath)
	if err != nil {
		return nil, err
	}
	if ref.Location != "" {
		locationDir, err := filepath.EvalSymlinks(filepath.Dir(absolute))
		if err != nil {
			return nil, err
		}
		if filepath.Clean(locationDir) != filepath.Clean(filepath.Dir(realPath)) {
			return nil, errors.New("located Sandbox Bundle alias target must remain in the same location directory")
		}
	}
	ref.Path = filepath.Base(realPath)
	ref.DigestScheme, ref.Digest = "", ""
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	if _, err := manifestbundle.EncodeRefs([]string{ref.String()}); err != nil {
		return nil, err
	}
	return &sandbox.BundleSourceBinding{
		RootRef: ref.String(), RootPath: realPath, Refs: reader.Refs(),
	}, nil
}

func resolveRunSourceRef(raw string, locations config.RefLocations) (manifest.Ref, string, error) {
	if raw == "" {
		return manifest.Ref{}, "", errors.New("empty Sandbox source")
	}
	if !strings.HasPrefix(raw, "file://") && !strings.HasPrefix(raw, "manifest://") {
		path, err := filepath.Abs(raw)
		if err != nil {
			return manifest.Ref{}, "", err
		}
		return manifest.Ref{Scheme: manifest.RefSchemeFile, Path: path}, path, nil
	}
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		return manifest.Ref{}, "", err
	}
	if ref.Scheme != manifest.RefSchemeFile {
		return ref, "", nil
	}
	path, err := locations.ResolveFile(ref, "")
	if err != nil {
		return manifest.Ref{}, "", err
	}
	if !filepath.IsAbs(path) {
		path, err = filepath.Abs(path)
		if err != nil {
			return manifest.Ref{}, "", err
		}
	}
	return ref, path, nil
}

func applyDefaultArtifactBindings(host *config.SandboxConfig, portable *config.PortableSandboxConfig, relativeDir string) {
	if host == nil || portable == nil || relativeDir == "" {
		return
	}
	bind := func(current *string, raw string) {
		if *current != "" {
			return
		}
		ref, err := manifest.ParseRef(raw)
		if err != nil || ref.Scheme != manifest.RefSchemeFile {
			return
		}
		*current = "file://" + filepath.Join(relativeDir, ref.Path)
	}
	bind(&host.Boot.Kernel, portable.Boot.Kernel)
	bind(&host.Boot.Runtime, portable.Boot.Runtime)
}
