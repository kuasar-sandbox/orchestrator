package sandboxsdk

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/stdio"
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

func TestStartSpecPreservesTaskSettingsAndWorkload(t *testing.T) {
	opts, storage := shapeFixture(t)
	opts.Cfg.Resources.Control.CgroupFD = 47
	opts.Cfg.Launch.EphemeralEnv = map[string]string{"TASK": "value"}
	opts.Cfg.Boot.Disks = []config.DiskConfig{{Name: "data", RootConfig: config.RootConfig{DiffTemplate: opts.Cfg.Boot.Root.DiffTemplate}}}
	opts.Cfg.Mounts = []config.MountConfig{{Type: "disk", Source: "data", Target: "/data"}}
	var err error
	opts.StdioMode, err = stdio.FromFlags(nil, nil, nil, "", "journald=task-output", "journald=task-error", nil, "journald=task-console")
	if err != nil {
		t.Fatal(err)
	}
	opts.Forwards = []sandbox.ForwardSpec{{UDSPath: "/task/exec.sock", Network: "tcp", Address: "127.0.0.1:22"}}
	opts.StatsInterval, opts.PingFatalThreshold = 4*time.Second, 9
	notified := false
	opts.NotifyReadiness = func(sandbox.ReadinessEvent) { notified = true }
	launch := sandbox.LaunchSpecFromConfig(opts.Cfg)
	launch.Storage = storage
	spec, err := startSpec(context.Background(), opts, launch)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Runtime.SandboxID != opts.SandboxID || spec.Runtime.PathID != opts.PathID || spec.Runtime.RuntimeRoot != opts.RuntimeRoot || spec.Runtime.BaseRoot != opts.BaseRoot || spec.Runtime.CHBinary != opts.CHBinary {
		t.Fatalf("task runtime identity lost: %+v", spec.Runtime)
	}
	if spec.Runtime.Resources.Control.CgroupFD != 47 || spec.Runtime.StatsInterval != opts.StatsInterval || spec.Runtime.PingFatalThreshold != opts.PingFatalThreshold || !reflect.DeepEqual(spec.Runtime.Console, opts.StdioMode.Console) {
		t.Fatalf("host descriptors or diagnostics lost: %+v", spec.Runtime)
	}
	spec.Runtime.NotifyReadiness(sandbox.ReadinessReady)
	if !notified {
		t.Fatal("task readiness callback lost")
	}
	if len(spec.Runtime.Disks) != 2 || spec.Runtime.Disks[1].Name != "data" || spec.Runtime.Disks[0].WritableCapacity != 8192 || spec.Runtime.Disks[1].WritableCapacity != 8192 {
		t.Fatalf("task disk topology lost: %+v", spec.Runtime.Disks)
	}
	wantLaunch := launch
	wantLaunch.Stdio, wantLaunch.Forwards = opts.StdioMode, opts.Forwards
	if !reflect.DeepEqual(spec.Launch, wantLaunch) {
		t.Fatal("task workload changed at public SDK boundary")
	}
}

func TestStartSpecPreservesAppliedSourceAndOfflineBundleAccess(t *testing.T) {
	ctx := context.Background()
	opts, _ := shapeFixture(t)
	manifestCfg := runFromManifestConfig([32]byte{0x71}, "")
	storage, err := artifact.NewProcessStorageWithCustomerKey(manifestCfg, manifestCfg.CustomerKey)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	dir := t.TempDir()
	sink, err := snapshot.NewBundleSink(ctx, dir, "source", manifestCfg, storage.CustomerKeyFunc(), nil)
	if err != nil {
		t.Fatal(err)
	}
	lower, _, err := sink.AbsorbOverlay(ctx, bytes.NewReader(make([]byte, 4096)), nil)
	if err != nil {
		t.Fatal(err)
	}
	portable := runFromDirectPortable(t)
	portable.Boot.Root.BaseFromRefs = []string{lower}
	portable.Launch.Env = map[string]string{"INHERITED": "source"}
	ref, _, err := sink.AbsorbSandbox(ctx, runFromLogical(t, portable))
	if err == nil {
		err = sink.CommitSandbox(ctx, ref, "")
	}
	closeErr := sink.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("Bundle: %v, close: %v", err, closeErr)
	}
	locations := config.RefLocations{"prepared": dir}
	source, err := sandbox.OpenRunSource(ctx, "file://source.sandbox@location:prepared", storage, locations)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	host, presence, err := config.LoadConfigBytesWithPresence([]byte("launch:\n  env: {}\n  ephemeral_env: {TASK: value}\n"))
	if err != nil {
		t.Fatal(err)
	}
	host.Boot.Kernel, host.Boot.Runtime = opts.Cfg.Boot.Kernel, opts.Cfg.Boot.Runtime
	host.Resources.Control.CgroupFD = 47
	var launch sandbox.LaunchSpec
	opts.Cfg, launch, err = source.Apply(host, presence, config.ApplyFromOptions{})
	if err != nil {
		t.Fatal(err)
	}
	spec, err := startSpec(ctx, opts, launch)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Runtime.Disks[0].WritableCapacity != 4096 || spec.Runtime.Resources.Control.CgroupFD != 47 {
		t.Fatalf("source shape or inherited descriptor lost: %+v", spec.Runtime)
	}
	if spec.Launch.Root.Base != "self" || opts.Cfg.Boot.Root.Base != "self" || host.Boot.Root.Base != "" {
		t.Fatal("source preparation mutated the caller's unbound graph")
	}
	if len(spec.Launch.Process.Env) != 0 || spec.Launch.Process.EphemeralEnv["TASK"] != "value" {
		t.Fatal("presence-aware or task-only overrides lost")
	}
	// Compare the whole Apply result, including SDK-private provenance. The
	// adapter must forward it by value instead of rebuilding public fields.
	wantLaunch := launch
	wantLaunch.Stdio, wantLaunch.Forwards = opts.StdioMode, opts.Forwards
	if !reflect.DeepEqual(spec.Launch, wantLaunch) || spec.Launch.SourceBinding != launch.SourceBinding || spec.Launch.PortableConfig != launch.PortableConfig {
		t.Fatal("public SDK launch lost the original applied source")
	}
	key, err := manifest.ParseKeyRef(lower)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := spec.Launch.Fetcher.OpenManifest(ctx, key)
	if err != nil {
		t.Fatalf("borrowed offline Bundle access was lost or closed: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStartSpecEncryptedSourceUsesTaskCredentials(t *testing.T) {
	ctx := context.Background()
	opts, _ := shapeFixture(t)
	manifestCfg := runFromManifestConfig([32]byte{0x73}, "")
	manifestCfg.Crypto.Local = "required"
	storage, err := artifact.NewProcessStorageWithCustomerKey(manifestCfg, func() ([32]byte, error) { return [32]byte{0x73}, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	_, path, err := snapshot.NewFileSink(t.TempDir(), "encrypted", storage.LocalCodec(), storage.LocalRequired(), nil).AbsorbSandbox(ctx, runFromLogical(t, runFromDirectPortable(t)))
	if err != nil {
		t.Fatal(err)
	}
	source, err := sandbox.OpenRunSource(ctx, path, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	opts.Cfg.Boot.Root = config.RootConfig{}
	var launch sandbox.LaunchSpec
	opts.Cfg, launch, err = source.Apply(opts.Cfg, config.FieldPresence{}, config.ApplyFromOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("MANIFEST_KEY", strings.Repeat("ff", 32))
	spec, err := startSpec(ctx, opts, launch)
	if err != nil || spec.Runtime.Disks[0].WritableCapacity != 4096 {
		t.Fatalf("encrypted task source = %+v, %v", spec.Runtime.Disks, err)
	}
}

func TestStartSpecKeepsCheckpointSourceVersion(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "diffs"), 0700); err != nil {
		t.Fatal(err)
	}
	portable, err := runFromDirectPortable(t).Checkpointed("")
	if err != nil {
		t.Fatal(err)
	}
	disk, err := diffstore.Create(filepath.Join(dir, "diffs", "root.diff"), diffstore.Options{DiffID: "root", BaseID: artifact.DiffCheckpointBaseID(portable.Boot.Root), LogicalSize: 8192})
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.Checkpoint(ctx, "selected"); err != nil {
		t.Fatal(err)
	}
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	cp := &sandboxfile.Checkpoint{Version: 1, CheckpointID: "selected", Root: sandboxfile.CheckpointDisk{DiffID: "root"}}
	logical, err := sandboxfile.BuildCheckpointSource(ctx, portable, cp)
	if err != nil {
		t.Fatal(err)
	}
	_, path, err := snapshot.NewFileSink(dir, "source", nil, false, nil).AbsorbSandbox(ctx, logical)
	if err != nil {
		t.Fatal(err)
	}
	in, cgroup := optionsFixture(t)
	in.Source = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: path}
	t.Setenv("MANIFEST_KEY", strings.Repeat("ff", 32))
	opts, presence, storage, err := in.options(cgroup)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	if storage.CustomerKeyFunc() != nil {
		t.Fatal("plaintext local-only task unexpectedly acquired a key resolver")
	}
	source, err := sandbox.OpenRunSource(ctx, path, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	var launch sandbox.LaunchSpec
	opts.Cfg, launch, err = source.Apply(opts.Cfg, presence, config.ApplyFromOptions{})
	if err != nil {
		t.Fatal(err)
	}
	spec, err := startSpec(ctx, opts, launch)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Runtime.Disks[0].WritableCapacity != 8192 || spec.Launch.SourceBinding.Checkpoint == nil || spec.Launch.SourceBinding.Checkpoint.CheckpointID != "selected" {
		t.Fatalf("checkpoint source lost during adapter projection: %+v", spec)
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
