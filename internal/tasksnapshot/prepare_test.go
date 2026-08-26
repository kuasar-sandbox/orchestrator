package tasksnapshot

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
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
  - file://parent.snapshot@location:memory-parent
  - file://parent.snapshot@location:memory-parent
boot:
  runtime_ref: file://runtime.bundle@location:platform-runtime
  root:
    base_ref: file://root.image@location:root-image
    base: manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    base_from_refs:
      - file://root-old.overlay@location:root-layer
    overlay:
      base: file://root-new.overlay@location:root-layer
      base_from_refs:
        - manifest://bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  disks:
    - base_ref: file://data.image@location:data-image
      base: file://data.overlay@location:data-layer
      base_from_refs:
        - file://data-old.overlay@location:data-layer
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
	if result.Summary.RequiredRefCount != 10 {
		t.Fatalf("required ref count = %d, want 10", result.Summary.RequiredRefCount)
	}
	for _, name := range []string{"memory-parent", "root-image", "root-layer", "data-image", "data-layer"} {
		location, err := reflocation.Resolve("file:///mnt/task-locations", name)
		if err != nil {
			t.Fatal(err)
		}
		if result.RefLocationURIs[name] != location.URI {
			t.Fatalf("location %q = %q, want %q", name, result.RefLocationURIs[name], location.URI)
		}
	}
	if _, exists := result.RefLocationURIs["platform-runtime"]; exists {
		t.Fatal("Boot.RuntimeRef entered tenant ref locations")
	}
	// The parent path intentionally does not exist. Success proves the task did
	// not reinterpret flattened FromRefs as snapshot.cfg graph edges.
	if result.RootCfg.FromRefs[0] != "file://parent.snapshot@location:memory-parent" {
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
		Scheme: manifest.RefSchemeFile, Path: base, Location: "root-source",
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
	if err := os.Rename(rootPath, filepath.Join(location.Path, base)); err != nil {
		t.Fatal(err)
	}
	_ = dir
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
	rootLocation, err := reflocation.Resolve(parent, "root")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(rootLocation.Path, 0o700); err != nil {
		t.Fatal(err)
	}
	refs := []string{
		"file://" + strings.Repeat("1", 64) + ".bundle",
		"file://" + strings.Repeat("2", 64) + ".bundle@location:A",
		"file://" + strings.Repeat("3", 64) + ".bundle@location:B",
	}
	const snapshotCfg = `resources:
  capacity: {cpu: 2, memory: 512MiB}
from_refs:
  - manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
boot:
  root:
    overlay:
      base: manifest://bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
`
	rootPath, rootKey, manifestConfig := writeTaskManifestBundle(t, rootLocation.Path, refs, snapshotCfg)
	rootRef := manifest.Ref{
		Scheme:       manifest.RefSchemeFile,
		Path:         filepath.Base(rootPath),
		DigestScheme: "manifest",
		Digest:       rootKey,
		Location:     "root",
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
	for _, name := range []string{"root", "A", "B"} {
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
		result.RootCfg.Boot.Root.Overlay.Base != "manifest://"+strings.Repeat("b", 64) {
		t.Fatalf("logical snapshot graph changed: %+v", result.RootCfg)
	}
	// None of the listed files or location directories exists. Success proves
	// discovery read only the current Bundle's metadata prefix: same-directory
	// refs need no mapping, located refs are not opened, and refs are not
	// followed recursively.
}

func TestPrepareBundleLocationRequiresConfiguredParent(t *testing.T) {
	refs := []string{"file://" + strings.Repeat("1", 64) + ".bundle@location:A"}
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

func writeTaskSnapshot(t *testing.T, cfg string) (string, string) {
	t.Helper()
	zipBody, err := snapshot.BuildZIP(map[string][]byte{
		"config.json":  {},
		"snapshot.cfg": []byte(cfg),
		"state.json":   {},
	})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	_, path, err := snapshot.NewFileSink(dir, "task-root", nil, false, nil).AbsorbBundle(
		context.Background(), bytes.NewReader(bytes.Repeat([]byte{0x5a}, 4096)), nil, bytes.NewReader(zipBody),
	)
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
	inner, err := snapshot.BuildZIP(map[string][]byte{
		"config.json":  {},
		"snapshot.cfg": []byte(cfgBody),
		"state.json":   {},
	})
	if err != nil {
		t.Fatal(err)
	}
	rootRef, path, err := sink.AbsorbBundle(context.Background(),
		bytes.NewReader(bytes.Repeat([]byte{0x5a}, 4096)), nil, bytes.NewReader(inner))
	if err != nil {
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
	return path, manifest.HexKey(rootKey), configPath
}
