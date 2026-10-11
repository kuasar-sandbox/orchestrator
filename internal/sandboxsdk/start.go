package sandboxsdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	runtimesdk "github.com/kuasar-sandbox/sandboxer/pkg/runtime"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/vhost"
)

func startSpec(ctx context.Context, opts sandbox.RunOptions, storage *artifact.ProcessStorage) (runtimesdk.SandboxSpec, error) {
	launch := sandbox.LaunchSpecFromConfig(opts.Cfg)
	launch.Storage = storage
	launch.ManifestConfig = opts.ManifestCfg
	launch.Fetcher = opts.Fetcher
	launch.BundleReader = opts.BundleReader
	launch.BundleFetcher = opts.BundleFetcher
	launch.RefLocations = opts.RefLocations
	launch.SourceBinding = opts.SourceBinding
	launch.PortableConfig = opts.PortableConfig
	launch.Stdio = opts.StdioMode
	launch.Forwards = opts.Forwards
	shape, err := deriveShape(ctx, opts)
	if err != nil {
		return runtimesdk.SandboxSpec{}, err
	}
	shape.SandboxID, shape.PathID = opts.SandboxID, opts.PathID
	shape.CHBinary, shape.RuntimeRoot, shape.BaseRoot = opts.CHBinary, opts.RuntimeRoot, opts.BaseRoot
	shape.Console = opts.StdioMode.Console
	shape.PingFatalThreshold, shape.StatsInterval = opts.PingFatalThreshold, opts.StatsInterval
	shape.NotifyReadiness = opts.NotifyReadiness
	return runtimesdk.SandboxSpec{Runtime: shape, Launch: launch}, nil
}

// Disk-capacity binding adapted from sandboxer/pkg/sandbox/runtime_sdk.go,
// deriveRuntimeSpec at 509cf54032dc34157b2e54effa089fb5aff19763 (Apache-2.0).
// Public DeriveRuntimeSpec accepts storage, but not the source-scoped Bundle
// fetcher. Keep this bounded metadata adapter until that API accepts artifact
// access/provenance. It does not implement VM lifecycle or identity policy.
func deriveShape(ctx context.Context, opts sandbox.RunOptions) (runtimesdk.RuntimeSpec, error) {
	// Bind a separate graph for capacity inspection. Public SDK Launch binds the
	// original graph once; pre-binding it would lose self/source provenance.
	body, err := json.Marshal(opts.Cfg)
	if err != nil {
		return runtimesdk.RuntimeSpec{}, err
	}
	var cfg config.SandboxConfig
	if err := json.Unmarshal(body, &cfg); err != nil {
		return runtimesdk.RuntimeSpec{}, err
	}
	if opts.SourceBinding != nil {
		if err := config.BindPortableDiskGraph(&cfg, opts.SourceBinding.RuntimeRef, opts.SourceBinding.RelativeDir); err != nil {
			return runtimesdk.RuntimeSpec{}, err
		}
	}
	if err := cfg.ValidateCold(); err != nil {
		return runtimesdk.RuntimeSpec{}, err
	}
	opener := sandbox.FileStreamOpener(func(ctx context.Context, path string, ref manifest.Ref) (fetch.Stream, error) {
		return artifact.OpenFileWithLocations(ctx, path, ref, opts.ManifestCfg, opts.CustomerKeyFn, opts.Fetcher, opts.RefLocations, opts.LocalCodec, opts.LocalRequired)
	})
	var diffOpts []vhost.BlockCOWOption
	if opts.LocalCodec != nil {
		if opts.CustomerKeyFn == nil {
			return runtimesdk.RuntimeSpec{}, errors.New("diff encryption requires customer key")
		}
		key, err := opts.CustomerKeyFn()
		if err != nil {
			return runtimesdk.RuntimeSpec{}, err
		}
		diffOpts = append(diffOpts, vhost.WithDiffEncryption(key, opts.LocalRequired))
		clear(key[:])
	}
	capacity := func(diffURI, defaultURI, template string, baseSize int64) (int64, error) {
		if diffURI == "" && defaultURI != "" {
			if _, p, ok := config.SchemeAndPath(defaultURI); ok {
				if st, err := os.Stat(p); err == nil && st.Size() > 0 {
					diffURI = defaultURI
				}
			}
		}
		if diffURI != "" {
			_, p, ok := config.SchemeAndPath(diffURI)
			if !ok {
				return 0, fmt.Errorf("invalid diff URI %q", diffURI)
			}
			if st, err := os.Stat(p); err == nil && st.Size() > 0 {
				return vhost.DiffSourceCapacity(p, false, diffOpts...)
			}
		}
		if template != "" {
			scheme, p, ok := config.SchemeAndPath(template)
			if !ok || scheme != "file" {
				return 0, fmt.Errorf("invalid diff template %q", template)
			}
			return vhost.DiffSourceCapacity(p, true, diffOpts...)
		}
		if baseSize > 0 {
			return baseSize, nil
		}
		return 0, errors.New("no source for writable disk capacity")
	}
	var shapes []runtimesdk.RuntimeDiskSpec
	derive := func(root config.RootConfig, name, defaultURI string) (runtimesdk.RuntimeDiskSpec, error) {
		d := runtimesdk.RuntimeDiskSpec{Name: name, Overlay: root.Overlay != nil}
		var cowBaseSize int64
		if root.Overlay != nil {
			reader, _, err := sandbox.OpenRootImageBlockReaderWithOpener(ctx, root.Base, opts.Fetcher, opts.RefLocations, opts.LocalCodec, opts.LocalRequired, opener)
			if err != nil {
				return d, err
			}
			d.BaseCapacity = reader.Size()
			_ = reader.Close()
			root = config.RootConfig{Base: root.Overlay.Base, BaseFromRefs: root.Overlay.BaseFromRefs, Diff: root.Overlay.Diff, DiffTemplate: root.Overlay.DiffTemplate}
		}
		if root.Base != "" {
			reader, _, err := sandbox.OpenLayeredBlockReaderWithOpener(ctx, append([]string{root.Base}, root.BaseFromRefs...), opts.Fetcher, opts.RefLocations, opts.LocalCodec, opts.LocalRequired, opener)
			if err != nil {
				return d, err
			}
			cowBaseSize = reader.Size()
			_ = reader.Close()
		}
		d.WritableCapacity, err = capacity(root.Diff, defaultURI, root.DiffTemplate, cowBaseSize)
		return d, err
	}
	baseDir := sandbox.DefaultBaseDir(opts.BaseRoot, opts.PathID)
	root, err := derive(cfg.Boot.Root, "root", sandbox.DefaultDiffURIForBaseDir(baseDir, opts.SandboxID))
	if err != nil {
		return runtimesdk.RuntimeSpec{}, err
	}
	shapes = append(shapes, root)
	for i, disk := range cfg.Boot.Disks {
		shape, err := derive(disk.RootConfig, disk.Name, sandbox.DefaultDiskDiffURI(baseDir, opts.SandboxID, fmt.Sprintf("disk%d", i)))
		if err != nil {
			return runtimesdk.RuntimeSpec{}, err
		}
		shapes = append(shapes, shape)
	}
	return runtimesdk.RuntimeSpec{Kernel: cfg.Boot.Kernel, Bundle: cfg.Boot.Runtime, Cmdline: cfg.Boot.Cmdline,
		Resources: cfg.Resources, Timeouts: cfg.Timeouts, Usage: cfg.Usage, Disks: shapes,
		Network: runtimesdk.NetworkDeviceSpec{TAP: cfg.Network.TAP, MAC: cfg.Network.MAC, TapFD: cfg.Network.TapFD}}, nil
}
