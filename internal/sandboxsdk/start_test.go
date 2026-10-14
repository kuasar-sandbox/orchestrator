package sandboxsdk

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"gopkg.in/yaml.v3"
)

func shapeFixture(t *testing.T) (sandbox.RunOptions, *artifact.ProcessStorage) {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.SandboxConfig{}
	cfg.ApplyDefaults()
	cfg.Resources.Capacity.CPU = 1
	cfg.Resources.Capacity.Memory = "16MiB"
	cfg.Resources.Allocatable.CPU = 1
	cfg.Resources.Allocatable.Memory = "16MiB"
	cfg.Boot.Kernel = "file://" + filepath.Join(dir, "vmlinux")
	cfg.Boot.Runtime = "file://" + filepath.Join(dir, "runtime.bundle")
	template := filepath.Join(dir, "template.ext4")
	data := make([]byte, 8192)
	data[1024+56], data[1024+57] = 0x53, 0xef
	if err := os.WriteFile(template, data, 0600); err != nil {
		t.Fatal(err)
	}
	cfg.Boot.Root.DiffTemplate = "file://" + template
	cfg.Launch.Exec, cfg.Launch.Restart = "/bin/true", "never"
	storage, err := artifact.NewProcessStorageWithCustomerKey(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	return sandbox.RunOptions{Cfg: cfg, SandboxID: "sb", PathID: "sb", RuntimeRoot: filepath.Join(dir, "run"), BaseRoot: filepath.Join(dir, "base"), CHBinary: "/fixture/cloud-hypervisor"}, storage
}

func TestRuntimeShapeMatchesSDKDerivation(t *testing.T) {
	opts, storage := shapeFixture(t)
	opts.Cfg.Boot.Disks = []config.DiskConfig{{Name: "data", RootConfig: config.RootConfig{DiffTemplate: opts.Cfg.Boot.Root.DiffTemplate}}}
	opts.Cfg.Mounts = []config.MountConfig{{Type: "disk", Source: "data", Target: "/data"}}
	want, err := sandbox.DeriveRuntimeSpec(context.Background(), opts.Cfg, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := deriveShape(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("shape=%+v, SDK=%+v", got, want)
	}
	spec, err := startSpec(context.Background(), opts, storage)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Runtime.SandboxID != opts.SandboxID || spec.Runtime.PathID != opts.PathID || spec.Runtime.RuntimeRoot != opts.RuntimeRoot || spec.Runtime.BaseRoot != opts.BaseRoot || spec.Runtime.CHBinary != opts.CHBinary {
		t.Fatalf("runtime identity lost: %+v", spec.Runtime)
	}
	wantLaunch := sandbox.LaunchSpecFromConfig(opts.Cfg)
	wantLaunch.Storage = storage
	if !reflect.DeepEqual(spec.Launch, wantLaunch) {
		t.Fatal("workload fields changed at public SDK boundary")
	}
}

func TestStartSpecRetainsOfflineBundleAndUnboundLaunch(t *testing.T) {
	ctx := context.Background()
	opts, _ := shapeFixture(t)
	cfg := runFromManifestConfig([32]byte{0x71}, "")
	storage, err := artifact.NewProcessStorageWithCustomerKey(cfg, cfg.CustomerKey)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	dir := t.TempDir()
	sink, err := snapshot.NewBundleSink(ctx, dir, "source", cfg, storage.CustomerKeyFunc(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := sink.AbsorbSandbox(ctx, runFromLogical(t, runFromDirectPortable(t)))
	if err == nil {
		err = sink.CommitSandbox(ctx, ref, "")
	}
	closeErr := sink.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("Bundle: %v, close: %v", err, closeErr)
	}
	source, err := openSandboxRunSource(ctx, filepath.Join(dir, "source.sandbox"), storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	opts.Cfg.Boot.Root = config.RootConfig{Base: "self"}
	opts.PortableConfig = source.Root.Portable
	opts.SourceBinding = &sandbox.RunSourceBinding{SandboxRef: source.PortableRef, RuntimeRef: source.RuntimeRef, RelativeDir: source.RelativeDir, BundleSource: source.BundleSource}
	opts.ManifestCfg, opts.CustomerKeyFn = cfg, storage.CustomerKeyFunc()
	opts.Fetcher, opts.BundleReader, opts.BundleFetcher = source.Fetcher, source.BundleReader, source.BundleFetcher
	spec, err := startSpec(ctx, opts, storage)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Runtime.Disks[0].WritableCapacity != 4096 {
		t.Fatalf("shape=%+v", spec.Runtime.Disks)
	}
	if spec.Launch.Root.Base != "self" || opts.Cfg.Boot.Root.Base != "self" {
		t.Fatal("capacity inspection mutated launch source graph")
	}
	if spec.Launch.SourceBinding != opts.SourceBinding || spec.Launch.PortableConfig != opts.PortableConfig || spec.Launch.BundleReader != source.BundleReader || spec.Launch.BundleFetcher != source.BundleFetcher {
		t.Fatal("public SDK launch lost source provenance or Bundle access")
	}
}

func TestExecutePublicStartRejectsInvalidKernelChecksum(t *testing.T) {
	opts, _ := shapeFixture(t)
	kernel := strings.TrimPrefix(opts.Cfg.Boot.Kernel, "file://")
	if err := os.WriteFile(kernel, []byte("kernel"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(kernel+".sha256", []byte("invalid checksum"), 0600); err != nil {
		t.Fatal(err)
	}
	in, cg := optionsFixture(t)
	body, err := yaml.Marshal(opts.Cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(in.Workdir, "sandbox.yaml"), body, 0600); err != nil {
		t.Fatal(err)
	}
	in.LogTo = "default"
	code, err := Execute(context.Background(), in, cg, NewReadiness(nil))
	if code != 1 || err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("Start checksum failure=%d, %v", code, err)
	}
	if _, err := os.Stat(filepath.Join(in.RunRoot, in.PathID)); !os.IsNotExist(err) {
		t.Fatalf("identity failure created runtime state: %v", err)
	}
}

func TestShapeReadsManifestFromSelectedOfflineBundle(t *testing.T) {
	ctx := context.Background()
	opts, _ := shapeFixture(t)
	cfg := runFromManifestConfig([32]byte{0x72}, "")
	storage, err := artifact.NewProcessStorageWithCustomerKey(cfg, cfg.CustomerKey)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	dir := t.TempDir()
	sink, err := snapshot.NewBundleSink(ctx, dir, "source", cfg, storage.CustomerKeyFunc(), nil)
	if err != nil {
		t.Fatal(err)
	}
	dependency, _, err := sink.AbsorbSandbox(ctx, runFromLogical(t, runFromDirectPortable(t)))
	if err != nil {
		t.Fatal(err)
	}
	err = sink.CommitSandbox(ctx, dependency, "")
	closeErr := sink.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("Bundle: %v, close: %v", err, closeErr)
	}
	source, err := openSandboxRunSource(ctx, filepath.Join(dir, "source.sandbox"), storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	opts.Cfg.Boot.Root = config.RootConfig{Base: dependency}
	opts.ManifestCfg, opts.CustomerKeyFn = cfg, storage.CustomerKeyFunc()
	opts.Fetcher = source.Fetcher
	// The public helper lacks the selected Bundle capability and cannot read
	// this manifest from a deliberately offline storage configuration.
	if _, err := sandbox.DeriveRuntimeSpec(ctx, opts.Cfg, storage, nil); err == nil {
		t.Fatal("fixture did not isolate a Bundle-only dependency")
	}
	shape, err := deriveShape(ctx, opts)
	if err != nil || shape.Disks[0].WritableCapacity != 4096 {
		t.Fatalf("scoped derivation=%+v, %v", shape, err)
	}
}

func TestShapeReadsEncryptedSourceWithoutAmbientKey(t *testing.T) {
	ctx := context.Background()
	opts, _ := shapeFixture(t)
	cfg := runFromManifestConfig([32]byte{0x73}, "")
	cfg.Crypto.Local = "required"
	storage, err := artifact.NewProcessStorageWithCustomerKey(cfg, func() ([32]byte, error) { return [32]byte{0x73}, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	_, path, err := snapshot.NewFileSink(t.TempDir(), "encrypted", storage.LocalCodec(), storage.LocalRequired(), nil).AbsorbSandbox(ctx, runFromLogical(t, runFromDirectPortable(t)))
	if err != nil {
		t.Fatal(err)
	}
	source, err := openSandboxRunSource(ctx, path, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	opts.Cfg.Boot.Root = config.RootConfig{Base: "self"}
	opts.SourceBinding = &sandbox.RunSourceBinding{RuntimeRef: source.RuntimeRef, RelativeDir: source.RelativeDir}
	opts.ManifestCfg, opts.CustomerKeyFn = cfg, storage.CustomerKeyFunc()
	opts.LocalCodec, opts.LocalRequired = storage.LocalCodec(), storage.LocalRequired()
	opts.Fetcher = source.Fetcher
	t.Setenv("MANIFEST_KEY", strings.Repeat("ff", 32))
	shape, err := deriveShape(ctx, opts)
	if err != nil || shape.Disks[0].WritableCapacity != 4096 {
		t.Fatalf("encrypted derivation=%+v, %v", shape, err)
	}
}

func TestOverlayShapeMatchesSDKAndRetainsUpperCapacity(t *testing.T) {
	ctx := context.Background()
	opts, storage := shapeFixture(t)
	// A real logical Sandbox carrier containing a minimal EROFS superblock and
	// image config. No VM or filesystem mount is simulated by this fixture.
	body := make([]byte, 4096)
	binary.LittleEndian.PutUint32(body[1024:1028], 0xE0F5E1E2)
	body[1024+12] = 12
	binary.LittleEndian.PutUint32(body[1024+36:1024+40], 1)
	payload, err := sparse.NewSource(bytes.NewReader(body), uint64(len(body)), nil)
	if err != nil {
		t.Fatal(err)
	}
	portable := runFromDirectPortable(t)
	portable.Boot.Root.Overlay = &config.PortableOverlayConfig{}
	cfgBytes, err := config.MarshalPortableSandboxConfig(portable)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := sandboxfile.BuildSource(payload, []byte("{}"), cfgBytes)
	if err != nil {
		t.Fatal(err)
	}
	_, path, err := snapshot.NewFileSink(t.TempDir(), "image", nil, false, nil).AbsorbSandbox(ctx, logical)
	if err != nil {
		t.Fatal(err)
	}
	opts.Cfg.Boot.Root = config.RootConfig{Base: "file://" + path, Overlay: &config.OverlayConfig{DiffTemplate: opts.Cfg.Boot.Root.DiffTemplate}}
	want, err := sandbox.DeriveRuntimeSpec(ctx, opts.Cfg, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := deriveShape(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) || got.Disks[0].BaseCapacity != 4096 || got.Disks[0].WritableCapacity != 8192 {
		t.Fatalf("overlay shape=%+v, SDK=%+v", got, want)
	}
}
