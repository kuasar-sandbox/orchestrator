package tasksnapshot

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

func TestPrepareReadsOnlyRootAndCollectsFlattenedClosure(t *testing.T) {
	const rootCfg = `resources:
  capacity:
    cpu: 4
    memory: 2GiB
metadata:
  kuasar-sandbox.network: '{"hostname":"inherited"}'
from_refs:
  - file://parent.snapshot@location:0198f7a11101-7234-9abc-012345670001-20260824
  - file://parent.snapshot@location:0198f7a11101-7234-9abc-012345670001-20260824
boot:
  runtime_ref: file://runtime.bundle@location:0198f7a11106-7234-9abc-012345670006-20260824
  root:
    base_ref: file://root.image@location:0198f7a11102-7234-9abc-012345670002-20260824
    base: manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    base_from_refs:
      - file://root-old.overlay@location:0198f7a11103-7234-9abc-012345670003-20260824
    overlay:
      base: file://root-new.overlay@location:0198f7a11103-7234-9abc-012345670003-20260824
      base_from_refs:
        - manifest://bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  disks:
    - base_ref: file://data.image@location:0198f7a11104-7234-9abc-012345670004-20260824
      base: file://data.overlay@location:0198f7a11105-7234-9abc-012345670005-20260824
      base_from_refs:
        - file://data-old.overlay@location:0198f7a11105-7234-9abc-012345670005-20260824
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
	for _, name := range []string{"0198f7a11101-7234-9abc-012345670001-20260824", "0198f7a11102-7234-9abc-012345670002-20260824", "0198f7a11103-7234-9abc-012345670003-20260824", "0198f7a11104-7234-9abc-012345670004-20260824", "0198f7a11105-7234-9abc-012345670005-20260824"} {
		location, err := reflocation.Resolve("file:///mnt/task-locations", name)
		if err != nil {
			t.Fatal(err)
		}
		if result.RefLocationURIs[name] != location.URI {
			t.Fatalf("location %q = %q, want %q", name, result.RefLocationURIs[name], location.URI)
		}
	}
	if _, exists := result.RefLocationURIs["0198f7a11106-7234-9abc-012345670006-20260824"]; exists {
		t.Fatal("Boot.RuntimeRef entered tenant ref locations")
	}
	// The parent path intentionally does not exist. Success proves the task did
	// not reinterpret flattened FromRefs as snapshot.cfg graph edges.
	if result.RootCfg.FromRefs[0] != "file://parent.snapshot@location:0198f7a11101-7234-9abc-012345670001-20260824" {
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
