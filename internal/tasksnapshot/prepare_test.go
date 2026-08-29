package tasksnapshot

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	sandboxconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
	"gopkg.in/yaml.v3"
)

func TestPrepareReadsOnlyRootAndCollectsFlattenedClosure(t *testing.T) {
	const rootCfg = `resources:
  capacity:
    cpu: 4
    memory: 2GiB
metadata:
  kuasar-sandbox.network: '{"hostname":"inherited"}'
from_refs:
  - file://parent.snapshot@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef@location:0198f7a11101-7234-9abc-012345670001-20260824
boot:
  runtime_ref: file://runtime.bundle@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  root:
    base: self
    base_from_refs:
      - file://root.image@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef@location:0198f7a11102-7234-9abc-012345670002-20260824
      - file://root-old.overlay@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef@location:0198f7a11103-7234-9abc-012345670003-20260824
      - manifest://bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  disks:
    - base_ref: file://data.image@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef@location:0198f7a11104-7234-9abc-012345670004-20260824
      overlay:
        base: file://data.overlay@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef@location:0198f7a11105-7234-9abc-012345670005-20260824
        base_from_refs:
          - manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
`
	_, rootPath := writeTaskSnapshot(t, rootCfg)
	result, err := Prepare(context.Background(), configsock.SnapshotPrepareSpec{
		RootRef:           rootPath,
		RefLocationParent: "file:///mnt/task-locations",
		MaxRefs:           32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.Capacity != (configsock.SnapshotCapacity{CPU: 4, Memory: "2GiB"}) {
		t.Fatalf("capacity = %+v", result.Summary.Capacity)
	}
	if result.Summary.RawNetworkMetadata != `{"hostname":"inherited"}` {
		t.Fatalf("raw network = %q", result.Summary.RawNetworkMetadata)
	}
	if result.Summary.SchemaVersion != configsock.SnapshotPrepareSchemaVersion || len(result.Summary.ResolutionDigest) != 64 {
		t.Fatalf("summary = %+v", result.Summary)
	}
	if result.Summary.RequiredRefCount != 9 {
		t.Fatalf("required ref count = %d, want 9", result.Summary.RequiredRefCount)
	}
	for _, name := range []string{"0198f7a11101-7234-9abc-012345670001-20260824", "0198f7a11102-7234-9abc-012345670002-20260824", "0198f7a11103-7234-9abc-012345670003-20260824", "0198f7a11104-7234-9abc-012345670004-20260824", "0198f7a11105-7234-9abc-012345670005-20260824"} {
		location, err := reflocation.Resolve("file:///mnt/task-locations", name)
		if err != nil {
			t.Fatal(err)
		}
		if result.RefLocationURIs[name] != location.URI {
			t.Fatalf("location %q = %q, want %q", name, result.RefLocationURIs[name], location.URI)
		}
	}
	if len(result.RefLocationURIs) != 5 {
		t.Fatalf("location map = %#v, want five tenant artifact locations", result.RefLocationURIs)
	}
	// The parent path intentionally does not exist. Success proves the task did
	// not reinterpret flattened FromRefs as snapshot.cfg graph edges.
	if result.RootCfg.FromRefs[0] != "file://parent.snapshot@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef@location:0198f7a11101-7234-9abc-012345670001-20260824" {
		t.Fatalf("root config changed: %+v", result.RootCfg.FromRefs)
	}
	if result.ConfigReadDuration <= 0 || result.PrepareDuration < result.ConfigReadDuration {
		t.Fatalf("durations read=%s prepare=%s", result.ConfigReadDuration, result.PrepareDuration)
	}
}

func TestPrepareLocatedRootBuildsPathMappingBeforeRead(t *testing.T) {
	dir, rootPath := writeTaskSnapshot(t, "resources:\n  capacity: {cpu: 1, memory: 64MiB}\nboot: {}\n")
	base := filepath.Base(rootPath)
	digest := strings.TrimSuffix(base, filepath.Ext(base))
	ref := manifest.Ref{
		Scheme: manifest.RefSchemeFile, Path: base, Location: "0198f7a11107-7234-9abc-012345670007-20260824",
		DigestScheme: "sha256", Digest: digest,
	}
	location, err := reflocation.Resolve("file:///tmp/task-snapshot-locations", ref.Location)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the test isolated while retaining the production derivation suffix.
	parent := "file://" + filepath.Join(t.TempDir(), "locations")
	location, err = reflocation.Resolve(parent, ref.Location)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(location.Path, 0o700); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := os.Rename(filepath.Join(dir, entry.Name()), filepath.Join(location.Path, entry.Name())); err != nil {
			t.Fatal(err)
		}
	}
	result, err := Prepare(context.Background(), configsock.SnapshotPrepareSpec{
		RootRef:           ref.String(),
		RefLocationParent: parent,
		MaxRefs:           4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RefLocationURIs[ref.Location] != location.URI {
		t.Fatalf("root URI = %q, want %q", result.RefLocationURIs[ref.Location], location.URI)
	}
}

func TestPrepareDiscoversFlatRootBundleLocations(t *testing.T) {
	parent := "file://" + filepath.Join(t.TempDir(), "locations")
	rootLocation, err := reflocation.Resolve(parent, "root-20260824")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(rootLocation.Path, 0o700); err != nil {
		t.Fatal(err)
	}
	refs := []string{
		"file://" + strings.Repeat("1", 64) + ".bundle",
		"file://" + strings.Repeat("2", 64) + ".bundle@location:A-20260824",
		"file://" + strings.Repeat("3", 64) + ".bundle@location:B-20260824",
	}
	const snapshotCfg = `resources:
  capacity: {cpu: 2, memory: 512MiB}
from_refs:
  - manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
boot:
  root:
    base_ref: manifest://bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
    overlay:
      base: self
`
	rootPath, rootKey, manifestConfig := writeTaskManifestBundle(t, rootLocation.Path, refs, snapshotCfg)
	rootRef := manifest.Ref{
		Scheme:       manifest.RefSchemeFile,
		Path:         filepath.Base(rootPath),
		DigestScheme: "manifest",
		Digest:       rootKey,
		Location:     "root-20260824",
	}.String()

	result, err := Prepare(context.Background(), configsock.SnapshotPrepareSpec{
		RootRef:           rootRef,
		ManifestConfig:    manifestConfig,
		RefLocationParent: parent,
		MaxRefs:           8,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"root-20260824", "A-20260824", "B-20260824"} {
		location, err := reflocation.Resolve(parent, name)
		if err != nil {
			t.Fatal(err)
		}
		if got := result.RefLocationURIs[name]; got != location.URI {
			t.Fatalf("location %q = %q, want %q", name, got, location.URI)
		}
	}
	if len(result.RefLocationURIs) != 3 {
		t.Fatalf("location map = %#v, want only root/A/B", result.RefLocationURIs)
	}
	if result.RootCfg.FromRefs[0] != "manifest://"+strings.Repeat("a", 64) ||
		result.RootCfg.Boot.Root.BaseRef != "manifest://"+strings.Repeat("b", 64) ||
		result.RootCfg.Boot.Root.Overlay.Base != result.RootCfg.SandboxRef {
		t.Fatalf("logical snapshot graph changed: %+v", result.RootCfg)
	}
	// None of the listed files or location directories exists. Success proves
	// discovery read only the current Bundle's metadata prefix: same-directory
	// refs need no mapping, located refs are not opened, and refs are not
	// followed recursively.
}

func TestPrepareBundleLocationRequiresConfiguredParent(t *testing.T) {
	refs := []string{"file://" + strings.Repeat("1", 64) + ".bundle@location:A-20260824"}
	rootPath, _, _ := writeTaskManifestBundle(t, t.TempDir(), refs, "boot: {}\n")
	_, err := Prepare(context.Background(), configsock.SnapshotPrepareSpec{RootRef: rootPath, MaxRefs: 4})
	if err == nil || !strings.Contains(err.Error(), "Bundle ref") || !strings.Contains(err.Error(), "ref_location_parent is not configured") {
		t.Fatalf("missing Bundle location parent error = %v", err)
	}
}

func TestPrepareMalformedBundleMetadataFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "malformed.bundle")
	if err := os.WriteFile(path, []byte{'P', 'K', 0x03, 0x04, 0x00}, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Prepare(context.Background(), configsock.SnapshotPrepareSpec{RootRef: path, MaxRefs: 4})
	if err == nil || !strings.Contains(err.Error(), "read root Bundle metadata") {
		t.Fatalf("malformed Bundle metadata error = %v", err)
	}
}

func TestRootBundleRefsManifestRootUsesStoreClosure(t *testing.T) {
	refs, err := rootBundleRefs("manifest://"+strings.Repeat("a", 64), "", nil)
	if err != nil || len(refs) != 0 {
		t.Fatalf("manifest Store root Bundle refs = %#v, %v", refs, err)
	}
}

func TestPrepareTruncatedBundleAfterValidMetadataFailsClosed(t *testing.T) {
	rootPath, _, manifestConfig := writeTaskManifestBundle(t, t.TempDir(),
		[]string{"file://" + strings.Repeat("1", 64) + ".bundle"}, "boot: {}\n")
	info, err := os.Stat(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(rootPath, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	_, err = Prepare(context.Background(), configsock.SnapshotPrepareSpec{
		RootRef: rootPath, ManifestConfig: manifestConfig, MaxRefs: 4,
	})
	if err == nil || !strings.Contains(err.Error(), "read root snapshot.cfg") {
		t.Fatalf("truncated Bundle archive error = %v", err)
	}
}

func TestPrepareRelativeAndRawRootResolution(t *testing.T) {
	dir, rootPath := writeTaskSnapshot(t, "resources:\n  capacity: {cpu: 1, memory: 64MiB}\nboot: {}\n")
	base := filepath.Base(rootPath)
	digest := strings.TrimSuffix(base, filepath.Ext(base))
	ref := manifest.Ref{Scheme: manifest.RefSchemeFile, Path: base, DigestScheme: "sha256", Digest: digest}.String()

	if _, err := Prepare(context.Background(), configsock.SnapshotPrepareSpec{RootRef: ref, MaxRefs: 4}); err == nil || !strings.Contains(err.Error(), "relative_dir") {
		t.Fatalf("relative ref without directory error = %v", err)
	}
	if _, err := Prepare(context.Background(), configsock.SnapshotPrepareSpec{RootRef: ref, RelativeDir: dir, MaxRefs: 4}); err != nil {
		t.Fatalf("relative ref with explicit directory: %v", err)
	}
	if _, err := Prepare(context.Background(), configsock.SnapshotPrepareSpec{RootRef: base, MaxRefs: 4}); err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("relative raw root error = %v", err)
	}
}

func TestRequiredRefsDeduplicatesSortsLimitsAndExcludesRuntime(t *testing.T) {
	cfg := &restore.SnapshotCfg{}
	cfg.FromRefs = []string{"z", "", "a", "z"}
	cfg.Boot.RuntimeRef = "runtime"
	cfg.Boot.Root.BaseRef = "disk"
	cfg.Boot.Root.BaseFromRefs = []string{"a"}
	got, err := requiredRefs("root", cfg, 4)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "disk", "root", "z"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("requiredRefs = %#v, want %#v", got, want)
	}
	if _, err := requiredRefs("root", cfg, 3); err == nil || !strings.Contains(err.Error(), "exceed") {
		t.Fatalf("limit error = %v", err)
	}
}

func TestPrepareHonorsCanceledContext(t *testing.T) {
	_, rootPath := writeTaskSnapshot(t, "resources:\n  capacity: {cpu: 1, memory: 64MiB}\nboot: {}\n")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Prepare(ctx, configsock.SnapshotPrepareSpec{RootRef: rootPath, MaxRefs: 4}); err == nil {
		t.Fatal("canceled preparation succeeded")
	}
}

func writeTaskSnapshot(t testing.TB, cfg string) (string, string) {
	t.Helper()
	sandboxSource, fromRefs := taskSandboxSource(t, cfg)
	dir := t.TempDir()
	sink := snapshot.NewFileSink(dir, "task-root", nil, false, nil)
	sandboxRef, _, err := sink.AbsorbSandbox(context.Background(), sandboxSource)
	if err != nil {
		t.Fatal(err)
	}
	_, path, err := sink.AbsorbSnapshot(context.Background(), taskMemorySource(t, sandboxRef, fromRefs))
	if err != nil {
		t.Fatal(err)
	}
	return dir, path
}

func writeTaskManifestBundle(t testing.TB, directory string, refs []string, cfgBody string) (string, string, string) {
	t.Helper()
	customerKey := [32]byte{0x41, 0x42, 0x43}
	manifestCfg := &manifest.Config{
		Manifest: manifest.ManifestSubConfig{Key: hex.EncodeToString(customerKey[:])},
		Chunker:  chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4KiB"}},
		Crypto:   manifestcrypto.Config{Chunk: "aes", Manifest: "aes"},
	}
	admission, err := manifestCfg.WriteAdmission(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sink, err := snapshot.NewPlannedBundleSink(directory, "task-root", manifestCfg,
		func() ([32]byte, error) { return customerKey, nil }, admission, refs, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	sandboxSource, fromRefs := taskSandboxSource(t, cfgBody)
	sandboxRef, _, err := sink.AbsorbSandbox(context.Background(), sandboxSource)
	if err != nil {
		t.Fatal(err)
	}
	rootRef, _, err := sink.AbsorbSnapshot(context.Background(), taskMemorySource(t, sandboxRef, fromRefs))
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.CommitSnapshot(context.Background(), rootRef, ""); err != nil {
		t.Fatal(err)
	}
	rootKey, err := manifest.ParseKeyRef(rootRef)
	if err != nil {
		t.Fatal(err)
	}
	configBody, err := yaml.Marshal(manifestCfg)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "accelerator.yaml")
	if err := os.WriteFile(configPath, configBody, 0o600); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(directory, manifest.HexKey(rootKey)+".bundle"), manifest.HexKey(rootKey), configPath
}

// taskSandboxSource turns the compatibility projection used by these tests
// into the current two-artifact model: portable disk/resource state in Sandbox
// E, with only memory ancestry retained for the later Snapshot S fixture.
func taskSandboxSource(t testing.TB, raw string) (sparse.Source, []string) {
	t.Helper()
	var projected restore.SnapshotCfg
	if err := yaml.Unmarshal([]byte(raw), &projected); err != nil {
		t.Fatal(err)
	}
	cpu := projected.Resources.Capacity.CPU
	if cpu == 0 {
		cpu = 1
	}
	memory := projected.Resources.Capacity.Memory
	if memory == "" {
		memory = "64MiB"
	}
	const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	runtimeRef := projected.Boot.RuntimeRef
	if runtimeRef == "" {
		runtimeRef = "file://sandbox-runtime.bundle@sha256:" + digest
	}
	root := sandboxconfig.PortableRootConfig{
		Base:         projected.Boot.Root.Base,
		BaseFromRefs: append([]string(nil), projected.Boot.Root.BaseFromRefs...),
	}
	if projected.Boot.Root.Overlay != nil {
		root.Base = projected.Boot.Root.BaseRef
		if root.Base == "" {
			root.Base = "self"
		}
		root.BaseFromRefs = nil
		root.Overlay = &sandboxconfig.PortableOverlayConfig{
			Base:         projected.Boot.Root.Overlay.Base,
			BaseFromRefs: append([]string(nil), projected.Boot.Root.Overlay.BaseFromRefs...),
		}
	} else if root.Base == "" {
		root.Base = projected.Boot.Root.BaseRef
		if root.Base == "" {
			root.Base = "self"
		}
	}
	disks := make([]sandboxconfig.PortableDiskConfig, len(projected.Boot.Disks))
	mounts := make([]sandboxconfig.MountConfig, len(projected.Boot.Disks))
	for i := range projected.Boot.Disks {
		projectedDisk := &projected.Boot.Disks[i]
		diskRoot := sandboxconfig.PortableRootConfig{
			Base:         projectedDisk.Base,
			BaseFromRefs: append([]string(nil), projectedDisk.BaseFromRefs...),
		}
		if projectedDisk.Overlay != nil {
			diskRoot.Base = projectedDisk.BaseRef
			diskRoot.BaseFromRefs = nil
			diskRoot.Overlay = &sandboxconfig.PortableOverlayConfig{
				Base:         projectedDisk.Overlay.Base,
				BaseFromRefs: append([]string(nil), projectedDisk.Overlay.BaseFromRefs...),
			}
		} else if diskRoot.Base == "" {
			diskRoot.Base = projectedDisk.BaseRef
		}
		name := fmt.Sprintf("data-%d", i)
		disks[i] = sandboxconfig.PortableDiskConfig{Name: name, PortableRootConfig: diskRoot}
		mounts[i] = sandboxconfig.MountConfig{Target: "/mnt/" + name, Type: "disk", Source: name}
	}
	portable := &sandboxconfig.PortableSandboxConfig{
		Version: sandboxconfig.PortableSandboxConfigVersion,
		Resources: sandboxconfig.PortableResourcesConfig{
			Capacity:    sandboxconfig.CapacityConfig{CPU: cpu, Memory: memory},
			Allocatable: sandboxconfig.AllocatableConfig{CPU: float64(cpu), Memory: memory},
		},
		Boot: sandboxconfig.PortableBootConfig{
			Kernel: "file://vmlinux@sha256:" + digest, Runtime: runtimeRef, Root: root, Disks: disks,
		},
		Launch: sandboxconfig.PortableLaunchConfig{
			Exec: "/bin/true", Workdir: "/", Restart: "never", CgroupControl: projected.Launch.CgroupControl,
		},
		Metadata: projected.Metadata,
		Mounts:   mounts,
	}
	portableRaw, err := sandboxconfig.MarshalPortableSandboxConfig(portable)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x41}, 4096)
	logical, err := sandboxfile.BuildSource(
		sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), nil, portableRaw,
	)
	if err != nil {
		t.Fatal(err)
	}
	return logical, append([]string(nil), projected.FromRefs...)
}

func taskMemorySource(t testing.TB, sandboxRef string, fromRefs []string) sparse.Source {
	t.Helper()
	cfg, err := snapshot.MarshalConfig(&snapshot.Config{
		Version: snapshot.SnapshotConfigVersion, SandboxRef: sandboxRef, FromRefs: fromRefs,
	})
	if err != nil {
		t.Fatal(err)
	}
	memory := bytes.Repeat([]byte{0x5a}, 4096)
	logical, err := snapshotfile.BuildSource(
		sparse.Dense(bytes.NewReader(memory), uint64(len(memory))),
		[]byte("{}"), []byte("{}"), cfg,
	)
	if err != nil {
		t.Fatal(err)
	}
	return logical
}
