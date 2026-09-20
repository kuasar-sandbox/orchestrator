package taskartifact

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	acceleratorimage "github.com/kuasar-sandbox/accelerator/pkg/image"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	acceleratorstore "github.com/kuasar-sandbox/accelerator/pkg/store"
	storefs "github.com/kuasar-sandbox/accelerator/pkg/store/fs"
	storepb "github.com/kuasar-sandbox/accelerator/pkg/store/pb"
	storeserver "github.com/kuasar-sandbox/accelerator/pkg/store/server"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	sandboxartifact "github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	sandboxconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
	"google.golang.org/grpc"
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
  - file://parent.snapshot@digest:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef@location:0198f7a11101-7234-9abc-012345670001
boot:
  runtime_ref: file://runtime.bundle@digest:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  root:
    base: self
    base_from_refs:
      - file://root.image@digest:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef@location:0198f7a11102-7234-9abc-012345670002
      - file://root-old.overlay@digest:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef@location:0198f7a11103-7234-9abc-012345670003
      - manifest://bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  disks:
    - base_ref: file://data.image@digest:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef@location:0198f7a11104-7234-9abc-012345670004
      overlay:
        base: file://data.overlay@digest:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef@location:0198f7a11105-7234-9abc-012345670005
        base_from_refs:
          - manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
`
	_, rootPath := writeTaskSnapshot(t, rootCfg)
	result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory),
		RootRef:           rootPath,
		RefLocationParent: "file:///mnt/task-locations",
		MaxRefs:           32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.Capacity != (configsock.ArtifactCapacity{CPU: 4, Memory: "2GiB", AllocatableCPU: 4, AllocatableMemory: "2GiB"}) {
		t.Fatalf("capacity = %+v", result.Summary.Capacity)
	}
	if !reflect.DeepEqual(result.Summary.Network, configsock.ArtifactNetwork{Hostname: "inherited"}) {
		t.Fatalf("network summary = %+v", result.Summary.Network)
	}
	wantTopology := types.ArtifactDiskTopology{
		Root: types.ArtifactDiskShape{Mode: types.ArtifactDiskSingle, HasActiveBase: true},
		Disks: []types.ArtifactDiskShape{
			{Name: "data-0", Mode: types.ArtifactDiskOverlay, HasActiveBase: true},
		},
	}
	if !reflect.DeepEqual(result.Summary.DiskTopology, wantTopology) {
		t.Fatalf("disk topology = %+v, want %+v", result.Summary.DiskTopology, wantTopology)
	}
	if result.Summary.SchemaVersion != configsock.ArtifactPrepareSchemaVersion || len(result.Summary.ResolutionDigest) != 64 {
		t.Fatalf("summary = %+v", result.Summary)
	}
	if result.Summary.RequiredRefCount != 9 {
		t.Fatalf("required ref count = %d, want 9", result.Summary.RequiredRefCount)
	}
	for _, name := range []string{"0198f7a11101-7234-9abc-012345670001", "0198f7a11102-7234-9abc-012345670002", "0198f7a11103-7234-9abc-012345670003", "0198f7a11104-7234-9abc-012345670004", "0198f7a11105-7234-9abc-012345670005"} {
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
	// not reinterpret flattened memory FromRefs as Sandbox disk graph edges.
	if result.PreparedSource.Kind != types.ResumeSourceSnapshot || result.SourceSandboxConfig == nil {
		t.Fatalf("source preparation did not retain its selected E config: %+v", result)
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
		Scheme: manifest.RefSchemeFile, Path: base, Location: "0198f7a11107-7234-9abc-012345670007",
		DigestScheme: "digest", Digest: digest,
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
	result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory),
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

func TestPrepareImageBundlePreflightRunsBeforeSourceScan(t *testing.T) {
	missingRoot := filepath.Join(t.TempDir(), "missing.sandbox")
	base := configsock.ArtifactPrepareSpec{
		RootSourceKind:       string(types.ResumeSourceSandbox),
		RootRef:              missingRoot,
		LaunchMode:           string(types.LaunchCold),
		RelativeDir:          t.TempDir(),
		MaxRefs:              4,
		RefLocationParent:    "file://" + filepath.Join(t.TempDir(), "locations"),
		PreflightImageBundle: true,
	}
	if _, err := Prepare(context.Background(), base); err == nil ||
		!strings.Contains(err.Error(), "preflight requires manifest_config") ||
		strings.Contains(err.Error(), "missing.sandbox") {
		t.Fatalf("missing manifest preflight error = %v", err)
	}

	configBody, err := yaml.Marshal(&manifest.Config{
		Chunker: chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4KiB"}},
		Crypto:  manifestcrypto.Config{Chunk: "aes", Manifest: "aes", Local: manifestcrypto.LocalOff},
	})
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "manifest.yaml")
	if err := os.WriteFile(configPath, configBody, 0o600); err != nil {
		t.Fatal(err)
	}
	base.ManifestConfig = configPath
	t.Setenv(manifest.CustomerKeyEnv, "not-a-customer-key")
	if _, err := Prepare(context.Background(), base); err == nil ||
		!strings.Contains(err.Error(), "customer key") ||
		strings.Contains(err.Error(), "missing.sandbox") {
		t.Fatalf("invalid key preflight error = %v", err)
	}

	t.Setenv(manifest.CustomerKeyEnv, strings.Repeat("a", 64))
	if _, err := Prepare(context.Background(), base); err == nil ||
		!strings.Contains(err.Error(), "missing.sandbox") {
		t.Fatalf("valid preflight did not proceed to source scan: %v", err)
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
		Location:     "root",
	}.String()

	result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory),
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
	if result.PreparedSource.Kind != types.ResumeSourceSnapshot || result.SourceSandboxConfig == nil {
		t.Fatalf("logical snapshot source did not retain selected E config: %+v", result)
	}
	// None of the listed files or location directories exists. Success proves
	// discovery read only the current Bundle's metadata prefix: same-directory
	// refs need no mapping, located refs are not opened, and refs are not
	// followed recursively.
}

func TestPrepareSnapshotColdDropsMemoryOnlyBundleLocations(t *testing.T) {
	parent := "file://" + filepath.Join(t.TempDir(), "locations")
	rootLocation, err := reflocation.Resolve(parent, "root")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(rootLocation.Path, 0o700); err != nil {
		t.Fatal(err)
	}
	refs := []string{
		"file://" + strings.Repeat("1", 64) + ".bundle@location:memory-a",
		"file://" + strings.Repeat("2", 64) + ".bundle@location:memory-b",
	}
	rootPath, rootKey, manifestConfig := writeTaskManifestBundle(t, rootLocation.Path, refs,
		"resources:\n  capacity: {cpu: 2, memory: 512MiB}\nboot: {}\n")
	rootRef := manifest.Ref{
		Scheme: manifest.RefSchemeFile, Path: filepath.Base(rootPath), Location: "root",
		DigestScheme: "manifest", Digest: rootKey,
	}.String()
	result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
		RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchCold),
		RootRef: rootRef, ManifestConfig: manifestConfig, RefLocationParent: parent, MaxRefs: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.PreparedSource.Kind != types.ResumeSourceSandbox {
		t.Fatalf("cold source = %+v", result.PreparedSource)
	}
	if len(result.RefLocationURIs) != 1 || result.RefLocationURIs["root"] == "" {
		t.Fatalf("cold ref locations retained memory closure: %#v", result.RefLocationURIs)
	}
}

func TestPrepareBundleLocationRequiresConfiguredParent(t *testing.T) {
	refs := []string{"file://" + strings.Repeat("1", 64) + ".bundle@location:A"}
	rootPath, _, _ := writeTaskManifestBundle(t, t.TempDir(), refs, "boot: {}\n")
	_, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory), RootRef: rootPath, MaxRefs: 4})
	if err == nil || !strings.Contains(err.Error(), "Bundle ref") || !strings.Contains(err.Error(), "ref_location_parent is not configured") {
		t.Fatalf("missing Bundle location parent error = %v", err)
	}
}

func TestPrepareMalformedBundleMetadataFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "malformed.bundle")
	if err := os.WriteFile(path, []byte{'P', 'K', 0x03, 0x04, 0x00}, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory), RootRef: path, MaxRefs: 4})
	if err == nil || !strings.Contains(err.Error(), "read root Bundle metadata") {
		t.Fatalf("malformed Bundle metadata error = %v", err)
	}
}

func TestRootCarrierManifestUsesStoreClosure(t *testing.T) {
	refs, binding, err := rootCarrier("manifest://"+strings.Repeat("a", 64), "", nil)
	if err != nil || len(refs) != 0 || binding.Format != "" {
		t.Fatalf("manifest Store root carrier = %#v, %+v, %v", refs, binding, err)
	}
}

func TestPrepareLocalSandboxAndSnapshotColdSelectResolvableSandbox(t *testing.T) {
	dir, sandboxRef, sandboxPath, _, snapshotPath := writeTaskLocalArtifacts(t,
		"resources:\n  capacity: {cpu: 3, memory: 768MiB}\nmetadata:\n  kuasar-sandbox.network: '{\"hostname\":\"artifact-host\"}'\nboot: {}\n",
		nil, false,
	)

	direct, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
		RootSourceKind: string(types.ResumeSourceSandbox), LaunchMode: string(types.LaunchCold),
		RootRef: sandboxPath, RelativeDir: dir, MaxRefs: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if direct.PreparedSource != (types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: sandboxPath}) ||
		direct.Summary.PreparedSourceKind != string(types.ResumeSourceSandbox) ||
		direct.Summary.Capacity != (configsock.ArtifactCapacity{CPU: 3, Memory: "768MiB", AllocatableCPU: 3, AllocatableMemory: "768MiB"}) ||
		!reflect.DeepEqual(direct.Summary.Network, configsock.ArtifactNetwork{Hostname: "artifact-host"}) {
		t.Fatalf("direct Sandbox E preparation = %+v", direct)
	}

	cold, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
		RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchCold),
		RootRef: snapshotPath, RelativeDir: dir, MaxRefs: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cold.PreparedSource.Kind != types.ResumeSourceSandbox || cold.Summary.PreparedSourceKind != string(types.ResumeSourceSandbox) {
		t.Fatalf("Snapshot cold prepared kind = %+v", cold)
	}
	selected, err := manifest.ParseRef(cold.PreparedSource.Ref)
	if err != nil {
		t.Fatalf("selected Sandbox ref %q: %v", cold.PreparedSource.Ref, err)
	}
	expected, err := manifest.ParseRef(sandboxRef)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(selected.Path) || filepath.Clean(selected.Path) != filepath.Clean(sandboxPath) ||
		selected.DigestScheme != expected.DigestScheme || selected.Digest != expected.Digest {
		t.Fatalf("selected Sandbox E = %#v, want path %q identity %#v", selected, sandboxPath, expected)
	}
	if cold.SourceSandboxConfig == nil || cold.Summary.Capacity != direct.Summary.Capacity ||
		!reflect.DeepEqual(cold.SourceSandboxConfig, direct.SourceSandboxConfig) {
		t.Fatalf("Snapshot cold did not derive C0 from E: %+v", cold)
	}
}

func TestPrepareSandboxReadsRuntimeConfigFromExternalBaseImage(t *testing.T) {
	dir := t.TempDir()
	flattenedPath := filepath.Join(dir, "flattened.img")
	flattened := taskEROFSBytes()
	if err := os.WriteFile(flattenedPath, flattened, 0o600); err != nil {
		t.Fatal(err)
	}
	want := &acceleratorimage.RuntimeConfig{
		Env: []string{"BUILT=yes"}, WorkingDir: "/home/user", User: "1000:1000",
	}
	if err := acceleratorimage.AppendConfigZip(flattenedPath, want); err != nil {
		t.Fatal(err)
	}
	flattened, err := os.ReadFile(flattenedPath)
	if err != nil {
		t.Fatal(err)
	}
	sink := snapshot.NewFileSink(dir, "external-base", nil, false, nil)
	imageRef, _, err := sink.AbsorbImageSource(context.Background(),
		sparse.Dense(bytes.NewReader(flattened), uint64(len(flattened))))
	if err != nil {
		t.Fatal(err)
	}
	sandboxSource, _ := taskSandboxSource(t, fmt.Sprintf(`resources:
  capacity: {cpu: 2, memory: 512MiB}
boot:
  root:
    base_ref: %s
    overlay: {base: self}
`, imageRef))
	_, sandboxPath, err := sink.AbsorbSandbox(context.Background(), sandboxSource)
	if err != nil {
		t.Fatal(err)
	}

	prepareSpec := configsock.ArtifactPrepareSpec{
		RootSourceKind: string(types.ResumeSourceSandbox), LaunchMode: string(types.LaunchCold),
		RootRef: sandboxPath, RelativeDir: dir, MaxRefs: 16,
	}
	unread, err := Prepare(context.Background(), prepareSpec)
	if err != nil {
		t.Fatal(err)
	}
	if len(unread.SourceImageConfig) != 0 {
		t.Fatalf("ordinary artifact preparation read Build-only image config: %q", unread.SourceImageConfig)
	}
	prepareSpec.ReadSourceImageConfig = true
	result, err := Prepare(context.Background(), prepareSpec)
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.ResolutionDigest == unread.Summary.ResolutionDigest {
		t.Fatal("Build-only image-config capability was omitted from preparation identity")
	}
	var got acceleratorimage.RuntimeConfig
	if err := json.Unmarshal(result.SourceImageConfig, &got); err != nil {
		t.Fatalf("source image config = %q: %v", result.SourceImageConfig, err)
	}
	if !reflect.DeepEqual(got.Env, want.Env) || got.WorkingDir != want.WorkingDir || got.User != want.User {
		t.Fatalf("source image config = %+v, want %+v", got, *want)
	}
}

func TestPrepareSandboxReadsRuntimeConfigThroughDirectEROFSParent(t *testing.T) {
	dir := t.TempDir()
	sink := snapshot.NewFileSink(dir, "parent", nil, false, nil)
	want := acceleratorimage.RuntimeConfig{
		Env: []string{"PARENT=yes"}, WorkingDir: "/srv", User: "1001:1001",
	}
	imageConfig, err := json.Marshal(&want)
	if err != nil {
		t.Fatal(err)
	}
	const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	portable := &sandboxconfig.PortableSandboxConfig{
		Version: sandboxconfig.PortableSandboxConfigVersion,
		Resources: sandboxconfig.PortableResourcesConfig{
			Capacity:    sandboxconfig.CapacityConfig{CPU: 1, Memory: "64MiB"},
			Allocatable: sandboxconfig.AllocatableConfig{CPU: 1, Memory: "64MiB"},
		},
		Boot: sandboxconfig.PortableBootConfig{
			Kernel: "file://vmlinux@digest:" + digest, Runtime: "file://runtime@digest:" + digest,
			Root: sandboxconfig.PortableRootConfig{Base: "self", Overlay: &sandboxconfig.PortableOverlayConfig{}},
		},
		Launch: sandboxconfig.PortableLaunchConfig{Exec: "/bin/true", Workdir: "/", Restart: "never"},
	}
	portableRaw, err := sandboxconfig.MarshalPortableSandboxConfig(portable)
	if err != nil {
		t.Fatal(err)
	}
	payload := taskEROFSBytes()
	parentSource, err := sandboxfile.BuildSource(
		sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), imageConfig, portableRaw,
	)
	if err != nil {
		t.Fatal(err)
	}
	parentRef, _, err := sink.AbsorbSandbox(context.Background(), parentSource)
	if err != nil {
		t.Fatal(err)
	}

	outerSource, _ := taskSandboxSource(t, fmt.Sprintf(`resources:
  capacity: {cpu: 2, memory: 512MiB}
boot:
  root:
    base_ref: %s
    overlay: {base: self}
`, parentRef))
	_, outerPath, err := sink.AbsorbSandbox(context.Background(), outerSource)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
		RootSourceKind: string(types.ResumeSourceSandbox), LaunchMode: string(types.LaunchCold),
		RootRef: outerPath, RelativeDir: dir, MaxRefs: 16, ReadSourceImageConfig: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var got acceleratorimage.RuntimeConfig
	if err := json.Unmarshal(result.SourceImageConfig, &got); err != nil {
		t.Fatalf("source image config = %q: %v", result.SourceImageConfig, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("source image config = %+v, want %+v", got, want)
	}
}

func TestPrepareSingleDiskSandboxDoesNotInterpretExt4BaseAsImage(t *testing.T) {
	dir := t.TempDir()
	sink := snapshot.NewFileSink(dir, "single", nil, false, nil)
	baseRef, _, err := sink.AbsorbOverlay(context.Background(), bytes.NewReader(bytes.Repeat([]byte{0x5a}, 4096)), nil)
	if err != nil {
		t.Fatal(err)
	}
	sandboxSource, _ := taskSandboxSource(t, fmt.Sprintf(`resources:
  capacity: {cpu: 1, memory: 64MiB}
boot:
  root:
    base: self
    base_from_refs: [%s]
`, baseRef))
	_, sandboxPath, err := sink.AbsorbSandbox(context.Background(), sandboxSource)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
		RootSourceKind: string(types.ResumeSourceSandbox), LaunchMode: string(types.LaunchCold),
		RootRef: sandboxPath, RelativeDir: dir, MaxRefs: 16, ReadSourceImageConfig: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.SourceImageConfig) != 0 {
		t.Fatalf("single-disk source unexpectedly exposed image config: %q", result.SourceImageConfig)
	}
}

func TestPrepareEncryptedLocalSandboxAndSnapshot(t *testing.T) {
	key := [32]byte{0x41, 0x42, 0x43}
	t.Setenv("MANIFEST_KEY", hex.EncodeToString(key[:]))
	codec, err := manifestcrypto.NewTarStreamCodec(key)
	if err != nil {
		t.Fatal(err)
	}
	dir, _, sandboxPath, _, snapshotPath := writeTaskLocalArtifacts(t,
		"resources:\n  capacity: {cpu: 2, memory: 512MiB}\nboot: {}\n", codec, true)
	manifestConfig := writeTaskLocalCryptoConfig(t, manifestcrypto.LocalRequired)

	for _, test := range []struct {
		name string
		kind types.ResumeSourceKind
		mode types.LaunchMode
		ref  string
		want types.ResumeSourceKind
	}{
		{name: "sandbox", kind: types.ResumeSourceSandbox, mode: types.LaunchCold, ref: sandboxPath, want: types.ResumeSourceSandbox},
		{name: "snapshot memory", kind: types.ResumeSourceSnapshot, mode: types.LaunchMemory, ref: snapshotPath, want: types.ResumeSourceSnapshot},
		{name: "snapshot cold", kind: types.ResumeSourceSnapshot, mode: types.LaunchCold, ref: snapshotPath, want: types.ResumeSourceSandbox},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
				RootSourceKind: string(test.kind), LaunchMode: string(test.mode), RootRef: test.ref,
				RelativeDir: dir, ManifestConfig: manifestConfig, MaxRefs: 16,
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.PreparedSource.Kind != test.want || result.Summary.PreparedSourceKind != string(test.want) {
				t.Fatalf("prepared encrypted source = %+v", result)
			}
		})
	}
}

func TestPrepareSnapshotColdSelectsSandboxManifestInSameBundle(t *testing.T) {
	bundlePath, snapshotKey, sandboxKey, manifestConfig := writeTaskManifestBundleArtifacts(t, t.TempDir(), nil,
		"resources:\n  capacity: {cpu: 2, memory: 512MiB}\nboot: {}\n")
	rootRef := manifest.Ref{
		Scheme: manifest.RefSchemeFile, Path: bundlePath,
		DigestScheme: "manifest", Digest: snapshotKey,
	}.String()
	result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
		RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchCold),
		RootRef: rootRef, ManifestConfig: manifestConfig, MaxRefs: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := manifest.ParseRef(result.PreparedSource.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if result.PreparedSource.Kind != types.ResumeSourceSandbox || selected.Scheme != manifest.RefSchemeFile ||
		filepath.Clean(selected.Path) != filepath.Clean(bundlePath) || selected.DigestScheme != "manifest" || selected.Digest != sandboxKey {
		t.Fatalf("same-Bundle Sandbox selector = %#v, source=%+v", selected, result.PreparedSource)
	}
	if len(result.CarrierBindings) != 1 || result.CarrierBindings[0].Format != "bundle" ||
		result.CarrierBindings[0].RootManifestKey != snapshotKey {
		t.Fatalf("Bundle binding = %+v", result.CarrierBindings)
	}

	directRef := manifest.Ref{
		Scheme: manifest.RefSchemeFile, Path: bundlePath,
		DigestScheme: "manifest", Digest: sandboxKey,
	}.String()
	direct, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
		RootSourceKind: string(types.ResumeSourceSandbox), LaunchMode: string(types.LaunchCold),
		RootRef: directRef, ManifestConfig: manifestConfig, MaxRefs: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if direct.PreparedSource != (types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: directRef}) {
		t.Fatalf("direct same-Bundle Sandbox = %+v", direct.PreparedSource)
	}
}

func TestPrepareColdSourceReadsBaseImageConfigFromSameBundle(t *testing.T) {
	directory := t.TempDir()
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
		func() ([32]byte, error) { return customerKey, nil }, admission, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sink.Close() })

	want := acceleratorimage.RuntimeConfig{
		Env: []string{"BUNDLED=yes"}, WorkingDir: "/workspace", User: "1002:1002",
	}
	flattenedPath := filepath.Join(t.TempDir(), "flattened.img")
	if err := os.WriteFile(flattenedPath, taskEROFSBytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := acceleratorimage.AppendConfigZip(flattenedPath, &want); err != nil {
		t.Fatal(err)
	}
	flattened, err := os.ReadFile(flattenedPath)
	if err != nil {
		t.Fatal(err)
	}
	imageRef, _, err := sink.AbsorbImageSource(context.Background(),
		sparse.Dense(bytes.NewReader(flattened), uint64(len(flattened))))
	if err != nil {
		t.Fatal(err)
	}
	sandboxSource, fromRefs := taskSandboxSource(t, fmt.Sprintf(`resources:
  capacity: {cpu: 2, memory: 512MiB}
boot:
  root:
    base_ref: %s
    overlay: {base: self}
`, imageRef))
	sandboxRef, _, err := sink.AbsorbSandbox(context.Background(), sandboxSource)
	if err != nil {
		t.Fatal(err)
	}
	snapshotRef, _, err := sink.AbsorbSnapshot(context.Background(), taskMemorySource(t, sandboxRef, fromRefs))
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.CommitSnapshot(context.Background(), snapshotRef, ""); err != nil {
		t.Fatal(err)
	}
	snapshotKey, err := manifest.ParseKeyRef(snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	sandboxKey, err := manifest.ParseKeyRef(sandboxRef)
	if err != nil {
		t.Fatal(err)
	}
	configBody, err := yaml.Marshal(manifestCfg)
	if err != nil {
		t.Fatal(err)
	}
	manifestConfig := filepath.Join(t.TempDir(), "accelerator.yaml")
	if err := os.WriteFile(manifestConfig, configBody, 0o600); err != nil {
		t.Fatal(err)
	}
	bundlePath := filepath.Join(directory, manifest.HexKey(snapshotKey)+".bundle")

	for _, test := range []struct {
		name string
		kind types.ResumeSourceKind
		key  string
	}{
		{name: "sandbox", kind: types.ResumeSourceSandbox, key: manifest.HexKey(sandboxKey)},
		{name: "snapshot", kind: types.ResumeSourceSnapshot, key: manifest.HexKey(snapshotKey)},
	} {
		t.Run(test.name, func(t *testing.T) {
			rootRef := manifest.Ref{
				Scheme: manifest.RefSchemeFile, Path: bundlePath,
				DigestScheme: "manifest", Digest: test.key,
			}.String()
			result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
				RootSourceKind: string(test.kind), LaunchMode: string(types.LaunchCold),
				RootRef: rootRef, ManifestConfig: manifestConfig, MaxRefs: 16, ReadSourceImageConfig: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			var got acceleratorimage.RuntimeConfig
			if err := json.Unmarshal(result.SourceImageConfig, &got); err != nil {
				t.Fatalf("source image config = %q: %v", result.SourceImageConfig, err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("source image config = %+v, want %+v", got, want)
			}
		})
	}
}

func TestPrepareManifestStoreSandboxAndSnapshotModes(t *testing.T) {
	sandboxRef, snapshotRef, manifestConfig := writeTaskManifestStoreArtifacts(t,
		"resources:\n  capacity: {cpu: 6, memory: 1536MiB}\nmetadata:\n  kuasar-sandbox.network: '{\"hostname\":\"remote-host\"}'\nboot: {}\n")

	tests := []struct {
		name       string
		rootKind   types.ResumeSourceKind
		launchMode types.LaunchMode
		rootRef    string
		want       types.ResumeSource
	}{
		{name: "sandbox cold", rootKind: types.ResumeSourceSandbox, launchMode: types.LaunchCold, rootRef: sandboxRef,
			want: types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: sandboxRef}},
		{name: "snapshot memory", rootKind: types.ResumeSourceSnapshot, launchMode: types.LaunchMemory, rootRef: snapshotRef,
			want: types.ResumeSource{SandboxRef: sandboxRef, Kind: types.ResumeSourceSnapshot, Ref: snapshotRef}},
		{name: "snapshot cold", rootKind: types.ResumeSourceSnapshot, launchMode: types.LaunchCold, rootRef: snapshotRef,
			want: types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: sandboxRef}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
				RootSourceKind: string(test.rootKind), LaunchMode: string(test.launchMode),
				RootRef: test.rootRef, ManifestConfig: manifestConfig, MaxRefs: 16,
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.PreparedSource != test.want || result.Summary.PreparedSourceKind != string(test.want.Kind) {
				t.Fatalf("prepared source = %+v, summary=%+v, want %+v", result.PreparedSource, result.Summary, test.want)
			}
			if result.Summary.Capacity != (configsock.ArtifactCapacity{CPU: 6, Memory: "1536MiB", AllocatableCPU: 6, AllocatableMemory: "1536MiB"}) ||
				!reflect.DeepEqual(result.Summary.Network, configsock.ArtifactNetwork{Hostname: "remote-host"}) {
				t.Fatalf("remote summary = %+v", result.Summary)
			}
			if len(result.CarrierBindings) != 0 {
				t.Fatalf("remote carrier bindings = %+v, want none", result.CarrierBindings)
			}
		})
	}
}

func TestPrepareRejectsUntrustedNetworkMetadataInsideTask(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
	}{
		{name: "malformed", raw: `{malformed`},
		{name: "duplicate key", raw: `{"hostname":"first","hostname":"second"}`},
		{name: "unknown field", raw: `{"credential":"must-not-cross-task-boundary"}`},
		{name: "null document", raw: `null`},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := fmt.Sprintf("resources:\n  capacity: {cpu: 1, memory: 64MiB}\nmetadata:\n  kuasar-sandbox.network: %q\nboot: {}\n", test.raw)
			dir, _, sandboxPath, _, _ := writeTaskLocalArtifacts(t, cfg, nil, false)
			_, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
				RootSourceKind: string(types.ResumeSourceSandbox), LaunchMode: string(types.LaunchCold),
				RootRef: sandboxPath, RelativeDir: dir, MaxRefs: 16,
			})
			if err == nil || !strings.Contains(err.Error(), "network metadata") {
				t.Fatalf("Prepare error = %v", err)
			}
		})
	}
}

func TestSummarizeDiskTopologyMarksDirectEROFSAsNeedingActiveUpper(t *testing.T) {
	const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	cfg := &sandboxconfig.PortableSandboxConfig{
		Version: sandboxconfig.PortableSandboxConfigVersion,
		Resources: sandboxconfig.PortableResourcesConfig{
			Capacity:    sandboxconfig.CapacityConfig{CPU: 1, Memory: "64MiB"},
			Allocatable: sandboxconfig.AllocatableConfig{CPU: 1, Memory: "64MiB"},
		},
		Boot: sandboxconfig.PortableBootConfig{
			Kernel: "file://vmlinux@digest:" + digest, Runtime: "file://runtime@digest:" + digest,
			Root: sandboxconfig.PortableRootConfig{Base: "self", Overlay: &sandboxconfig.PortableOverlayConfig{}},
		},
		Launch: sandboxconfig.PortableLaunchConfig{Exec: "/bin/true", Workdir: "/", Restart: "never"},
	}
	got, err := summarizeDiskTopology(cfg)
	if err != nil {
		t.Fatal(err)
	}
	wantRoot := types.ArtifactDiskShape{Mode: types.ArtifactDiskOverlay, HasActiveBase: false}
	if got.Root != wantRoot || len(got.Disks) != 0 {
		t.Fatalf("direct EROFS topology = %+v, want root %+v with no data disks", got, wantRoot)
	}
}

func TestPrepareRejectsSourceRoleAndModeMismatch(t *testing.T) {
	_, _, sandboxPath, _, snapshotPath := writeTaskLocalArtifacts(t,
		"resources:\n  capacity: {cpu: 1, memory: 64MiB}\nboot: {}\n", nil, false)
	for _, test := range []struct {
		name string
		kind types.ResumeSourceKind
		mode types.LaunchMode
		ref  string
	}{
		{name: "sandbox memory", kind: types.ResumeSourceSandbox, mode: types.LaunchMemory, ref: sandboxPath},
		{name: "snapshot declared sandbox", kind: types.ResumeSourceSandbox, mode: types.LaunchCold, ref: snapshotPath},
		{name: "sandbox declared snapshot", kind: types.ResumeSourceSnapshot, mode: types.LaunchMemory, ref: sandboxPath},
	} {
		t.Run(test.name, func(t *testing.T) {
			if result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
				RootSourceKind: string(test.kind), LaunchMode: string(test.mode), RootRef: test.ref, MaxRefs: 16,
			}); err == nil || result != nil {
				t.Fatalf("mismatched source prepared = %+v, %v", result, err)
			}
		})
	}
}

func TestPrepareSnapshotFailsClosedWhenReferencedSandboxIsMalformed(t *testing.T) {
	for _, mode := range []types.LaunchMode{types.LaunchMemory, types.LaunchCold} {
		t.Run(string(mode), func(t *testing.T) {
			dir, _, sandboxPath, _, snapshotPath := writeTaskLocalArtifacts(t,
				"resources:\n  capacity: {cpu: 1, memory: 64MiB}\nboot: {}\n", nil, false)
			if err := os.Truncate(sandboxPath, 17); err != nil {
				t.Fatal(err)
			}
			result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
				RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(mode),
				RootRef: snapshotPath, RelativeDir: dir, MaxRefs: 16,
			})
			if err == nil || result != nil {
				t.Fatalf("Snapshot with malformed E prepared = %+v, %v", result, err)
			}
			if !strings.Contains(err.Error(), "sandbox_ref") {
				t.Fatalf("malformed referenced E error = %v", err)
			}
		})
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
	_, err = Prepare(context.Background(), configsock.ArtifactPrepareSpec{RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory),
		RootRef: rootPath, ManifestConfig: manifestConfig, MaxRefs: 4,
	})
	if err == nil || !strings.Contains(err.Error(), "read Snapshot S") {
		t.Fatalf("truncated Bundle archive error = %v", err)
	}
}

func TestPrepareRelativeAndRawRootResolution(t *testing.T) {
	dir, rootPath := writeTaskSnapshot(t, "resources:\n  capacity: {cpu: 1, memory: 64MiB}\nboot: {}\n")
	base := filepath.Base(rootPath)
	digest := strings.TrimSuffix(base, filepath.Ext(base))
	ref := manifest.Ref{Scheme: manifest.RefSchemeFile, Path: base, DigestScheme: "digest", Digest: digest}.String()

	if _, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory), RootRef: ref, MaxRefs: 4}); err == nil || !strings.Contains(err.Error(), "relative_dir") {
		t.Fatalf("relative ref without directory error = %v", err)
	}
	if _, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory), RootRef: ref, RelativeDir: dir, MaxRefs: 4}); err != nil {
		t.Fatalf("relative ref with explicit directory: %v", err)
	}
	if _, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory), RootRef: base, MaxRefs: 4}); err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("relative raw root error = %v", err)
	}
}

func TestRequiredRefsDeduplicatesSortsLimitsAndExcludesRuntime(t *testing.T) {
	got, err := canonicalRefs([]string{"root", "z", "", "a", "z", "disk", "a"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "disk", "root", "z"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("requiredRefs = %#v, want %#v", got, want)
	}
	if _, err := canonicalRefs([]string{"root", "z", "a", "disk"}, 3); err == nil || !strings.Contains(err.Error(), "exceed") {
		t.Fatalf("limit error = %v", err)
	}
}

func TestPrepareHonorsCanceledContext(t *testing.T) {
	_, rootPath := writeTaskSnapshot(t, "resources:\n  capacity: {cpu: 1, memory: 64MiB}\nboot: {}\n")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Prepare(ctx, configsock.ArtifactPrepareSpec{RootSourceKind: string(types.ResumeSourceSnapshot), LaunchMode: string(types.LaunchMemory), RootRef: rootPath, MaxRefs: 4}); err == nil {
		t.Fatal("canceled preparation succeeded")
	}
}

func writeTaskSnapshot(t testing.TB, cfg string) (string, string) {
	t.Helper()
	dir, _, _, _, path := writeTaskLocalArtifacts(t, cfg, nil, false)
	return dir, path
}

func writeTaskLocalArtifacts(t testing.TB, cfg string, codec tarstream.Codec, required bool) (dir, sandboxRef, sandboxPath, snapshotRef, snapshotPath string) {
	t.Helper()
	sandboxSource, fromRefs := taskSandboxSource(t, cfg)
	dir = t.TempDir()
	sink := snapshot.NewFileSink(dir, "task-root", codec, required, nil)
	var err error
	sandboxRef, sandboxPath, err = sink.AbsorbSandbox(context.Background(), sandboxSource)
	if err != nil {
		t.Fatal(err)
	}
	snapshotRef, snapshotPath, err = sink.AbsorbSnapshot(context.Background(), taskMemorySource(t, sandboxRef, fromRefs))
	if err != nil {
		t.Fatal(err)
	}
	return dir, sandboxRef, sandboxPath, snapshotRef, snapshotPath
}

func writeTaskManifestBundle(t testing.TB, directory string, refs []string, cfgBody string) (string, string, string) {
	t.Helper()
	path, snapshotKey, _, configPath := writeTaskManifestBundleArtifacts(t, directory, refs, cfgBody)
	return path, snapshotKey, configPath
}

func writeTaskManifestBundleArtifacts(t testing.TB, directory string, refs []string, cfgBody string) (string, string, string, string) {
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
	sandboxKey, err := manifest.ParseKeyRef(sandboxRef)
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
	return filepath.Join(directory, manifest.HexKey(rootKey)+".bundle"), manifest.HexKey(rootKey), manifest.HexKey(sandboxKey), configPath
}

func writeTaskManifestStoreArtifacts(t testing.TB, cfgBody string) (string, string, string) {
	t.Helper()
	backend, err := storefs.New(storefs.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	server, err := storeserver.New(storeserver.Options{
		Backend: backend, VerifyKey: true,
		Generations: func() []acceleratorstore.Generation { return []acceleratorstore.Generation{"G1"} },
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	storepb.RegisterStoreServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	customerKey := [32]byte{0x51, 0x52, 0x53}
	t.Setenv("MANIFEST_KEY", hex.EncodeToString(customerKey[:]))
	manifestCfg := &manifest.Config{
		Manifest: manifest.ManifestSubConfig{Key: hex.EncodeToString(customerKey[:]), WriteGeneration: "G1"},
		Store:    manifest.StoreConfig{Endpoint: listener.Addr().String(), Pool: 2, Timeout: "5s"},
		Chunker:  chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4KiB"}},
		Crypto:   manifestcrypto.Config{Chunk: "aes", Manifest: "aes", Local: manifestcrypto.LocalOff},
	}
	storage, err := sandboxartifact.NewProcessStorage(manifestCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	ingester, err := manifestCfg.NewIngester(storage.CustomerKeyFunc(), nil)
	if err != nil {
		t.Fatal(err)
	}
	sink := snapshot.NewIngestSink(ingester, nil)
	sandboxSource, fromRefs := taskSandboxSource(t, cfgBody)
	sandboxRef, _, err := sink.AbsorbSandbox(context.Background(), sandboxSource)
	if err != nil {
		_ = sink.Close()
		t.Fatal(err)
	}
	snapshotRef, _, err := sink.AbsorbSnapshot(context.Background(), taskMemorySource(t, sandboxRef, fromRefs))
	closeErr := sink.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("write remote artifacts: absorb=%v close=%v", err, closeErr)
	}

	configBody, err := yaml.Marshal(manifestCfg)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "accelerator-remote.yaml")
	if err := os.WriteFile(configPath, configBody, 0o600); err != nil {
		t.Fatal(err)
	}
	return sandboxRef, snapshotRef, configPath
}

func writeTaskLocalCryptoConfig(t testing.TB, policy manifestcrypto.LocalPolicy) string {
	t.Helper()
	body, err := yaml.Marshal(&manifest.Config{Crypto: manifestcrypto.Config{Local: policy}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "accelerator-local.yaml")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
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
		runtimeRef = "file://sandbox-runtime.bundle@digest:" + digest
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
			Kernel: "file://vmlinux@digest:" + digest, Runtime: runtimeRef, Root: root, Disks: disks,
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

func taskEROFSBytes() []byte {
	payload := make([]byte, 4096)
	binary.LittleEndian.PutUint32(payload[1024:1028], 0xE0F5E1E2)
	payload[1024+12] = 12
	binary.LittleEndian.PutUint32(payload[1024+36:1024+40], 1)
	return payload
}
