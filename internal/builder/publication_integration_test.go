package builder

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	rtsandbox "github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

const publicationFixtureDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type publicationFixture struct {
	ctx            context.Context
	cfg            *rtconfig.ManifestConfig
	manifestPath   string
	storeRoot      string
	key            [32]byte
	keyHex         string
	localImagePath string
	localImageRef  string
	imageConfig    []byte
	kernel         string
	runtime        string
	overlay        string
	locationParent string
}

func newPublicationFixture(t *testing.T) *publicationFixture {
	t.Helper()
	storeRoot := t.TempDir()
	backend, err := storefs.New(storefs.Config{Root: storeRoot})
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

	key := [32]byte{0x31, 0x32, 0x33, 0x34}
	cfg := &rtconfig.ManifestConfig{
		Manifest: manifest.ManifestSubConfig{Key: hex.EncodeToString(key[:]), WriteGeneration: "G1"},
		Store:    manifest.StoreConfig{Endpoint: listener.Addr().String(), Pool: 2, Timeout: "5s"},
		Chunker:  chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4KiB"}},
		Crypto:   manifestcrypto.Config{Chunk: "aes", Manifest: "aes", Local: manifestcrypto.LocalOff},
	}
	manifestBody, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(t.TempDir(), "manifest.yaml")
	if err := os.WriteFile(manifestPath, manifestBody, 0o600); err != nil {
		t.Fatal(err)
	}

	assets := t.TempDir()
	kernel := filepath.Join(assets, "vmlinux")
	if err := os.WriteFile(kernel, []byte("kernel fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtime := filepath.Join(assets, "sandbox-runtime.bundle")
	writePublicationRuntimeBundle(t, runtime)
	overlay := filepath.Join(assets, "overlay.ext4")
	if err := os.WriteFile(overlay, []byte("formatted upper fixture"), 0o600); err != nil {
		t.Fatal(err)
	}

	imageDir := filepath.Join(t.TempDir(), "base", "checkpoint")
	if err := os.MkdirAll(imageDir, 0o700); err != nil {
		t.Fatal(err)
	}
	flattenedPath := filepath.Join(t.TempDir(), "flattened.erofs")
	payload := make([]byte, 3*4096)
	binary.LittleEndian.PutUint32(payload[1024:1028], 0xE0F5E1E2)
	payload[1024+12] = 12
	binary.LittleEndian.PutUint32(payload[1024+36:1024+40], 3)
	if err := os.WriteFile(flattenedPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := acceleratorimage.AppendConfigZip(flattenedPath, &acceleratorimage.RuntimeConfig{
		Cmd: []string{"/bin/service", "serve"}, Env: []string{"IMAGE=yes"}, WorkingDir: "/srv",
	}); err != nil {
		t.Fatal(err)
	}
	flattened, err := os.ReadFile(flattenedPath)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := sparse.NewSource(bytes.NewReader(flattened), uint64(len(flattened)), []sparse.Extent{{Offset: 4096, Size: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	localImagePath := filepath.Join(imageDir, "image.img")
	f, err := os.OpenFile(localImagePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	scheme, digest, writeErr := tarstream.WriteTo(context.Background(), f, "image", logical)
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("write local image: %v; close: %v", writeErr, closeErr)
	}
	localImageRef := manifest.Ref{
		Scheme: manifest.RefSchemeFile, Path: localImagePath, DigestScheme: scheme, Digest: digest,
	}.String()
	storage, err := artifact.NewProcessStorageWithCustomerKey(cfg, func() ([32]byte, error) { return key, nil })
	if err != nil {
		t.Fatal(err)
	}
	image, err := rtsandbox.OpenFlattenedImage(context.Background(), localImageRef, storage, nil)
	if err != nil {
		_ = storage.Close()
		t.Fatal(err)
	}
	imageConfig := append([]byte(nil), image.ImageConfig...)
	if err := errors.Join(image.Close(), storage.Close()); err != nil {
		t.Fatal(err)
	}

	return &publicationFixture{
		ctx: context.Background(), cfg: cfg, manifestPath: manifestPath,
		storeRoot: storeRoot, key: key, keyHex: hex.EncodeToString(key[:]),
		localImagePath: localImagePath, localImageRef: localImageRef, imageConfig: imageConfig,
		kernel: kernel, runtime: runtime, overlay: overlay,
		locationParent: "file://" + filepath.Join(t.TempDir(), "locations"),
	}
}

func writePublicationRuntimeBundle(t *testing.T, path string) {
	t.Helper()
	var footer bytes.Buffer
	zw := zip.NewWriter(&footer)
	header := &zip.FileHeader{
		Name: tarstream.DigestMarkerPrefix + publicationFixtureDigest, Method: zip.Store,
		Modified: time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	header.SetMode(0o444)
	if _, err := zw.CreateHeader(header); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 2<<20)
	copy(body[len(body)-footer.Len():], footer.Bytes())
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
}

func (f *publicationFixture) pipeline(t *testing.T, buildID, imageRef string, locations map[string]string, manifestBundle bool) *buildPipeline {
	t.Helper()
	runDir := t.TempDir()
	baseDir := filepath.Dir(filepath.Dir(f.localImagePath))
	resources := rtconfig.ResourcesConfig{
		Capacity:    rtconfig.CapacityConfig{CPU: 1, Memory: "256MiB"},
		Allocatable: rtconfig.AllocatableConfig{CPU: 1, Memory: "256MiB"},
	}
	p := &buildPipeline{
		parent: f.ctx, ctx: f.ctx, profile: types.ProfileBare,
		spec: &configsock.BuildSpec{
			BuildID: buildID, Profile: string(types.ProfileBare), RunID: "run-" + buildID,
			RunDir: runDir, BaseDir: baseDir, RefLocations: clonePublicationLocations(locations),
			CheckpointMode:              configsockCheckpointLocal,
			CheckpointRefLocationParent: f.locationParent,
			CheckpointRemoteManifest:    manifestBundle,
			Env:                         map[string]string{manifest.CustomerKeyEnv: f.keyHex},
			Paths: configsock.BuildPaths{
				Kernel: f.kernel, Runtime: f.runtime, OverlayDiffTpl: f.overlay,
				ManifestConfig: f.manifestPath,
			},
			Net:              configsock.BuildNet{TapFD: configsock.TapFDConfig{Exec: []string{"/bin/true"}}},
			Resources:        resources,
			SandboxResources: resources,
		},
		baseRef: imageRef, target: types.BuildTarget{Kind: types.BuildTargetImage},
		log: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}
	if imageRef != f.localImageRef {
		p.baseImageRef = imageRef
	}
	if err := p.preparePublication(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.artifacts.Close(); err != nil {
			t.Errorf("close publication resources: %v", err)
		}
	})
	return p
}

// Keep this fixture independent from the config package's spelling while
// exercising the production default accepted by sandbox-ctl publish.
const configsockCheckpointLocal = "local"

func clonePublicationLocations(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for name, location := range in {
		out[name] = location
	}
	return out
}

func (f *publicationFixture) publishManifestImage(t *testing.T) string {
	t.Helper()
	storage, err := artifact.NewProcessStorageWithCustomerKey(f.cfg, func() ([32]byte, error) { return f.key, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	image, err := rtsandbox.OpenFlattenedImage(f.ctx, f.localImageRef, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer image.Close()
	publisher, err := artifact.NewManifestPublisher(storage, f.cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, publishErr := publisher.PublishSource(f.ctx, artifact.RoleImage, image.FullStream)
	closeErr := publisher.Close()
	if publishErr != nil || closeErr != nil {
		t.Fatalf("publish fixture image: %v; close: %v", publishErr, closeErr)
	}
	return result.Ref
}

func (f *publicationFixture) publishLocatedImage(t *testing.T) (string, map[string]string) {
	t.Helper()
	name := "source-image-20260903"
	directory := t.TempDir()
	storage, err := artifact.NewProcessStorageWithCustomerKey(f.cfg, func() ([32]byte, error) { return f.key, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	image, err := rtsandbox.OpenFlattenedImage(f.ctx, f.localImageRef, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer image.Close()
	admission, err := f.cfg.WriteAdmission(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := artifact.NewSingleRootBundlePublisher(
		f.cfg, func() ([32]byte, error) { return f.key, nil }, admission, name, directory, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, publishErr := publisher.PublishSource(f.ctx, artifact.RoleImage, image.FullStream)
	closeErr := publisher.Close()
	if publishErr != nil || closeErr != nil {
		t.Fatalf("publish located fixture image: %v; close: %v", publishErr, closeErr)
	}
	return result.Ref, map[string]string{name: "file://" + directory}
}

func TestPublishSandboxTargetStreamsLocalImageDirectlyToFinalPolicy(t *testing.T) {
	for _, test := range []struct {
		name           string
		manifestBundle bool
		wantScheme     string
	}{
		{name: "Manifest store despite checkpoint parent", wantScheme: manifest.RefSchemeManifest},
		{name: "named-location Bundle", manifestBundle: true, wantScheme: manifest.RefSchemeFile},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPublicationFixture(t)
			pipeline := fixture.pipeline(t, "build-top-level-e", fixture.localImageRef, nil, test.manifestBundle)
			pipeline.target = types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: false}

			ref, err := pipeline.publishSandboxTarget()
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := manifest.ParseRef(ref)
			if err != nil || parsed.Scheme != test.wantScheme {
				t.Fatalf("top-level Sandbox E ref = %q, parsed=%+v err=%v", ref, parsed, err)
			}
			if pipeline.baseRef != fixture.localImageRef || pipeline.baseImageRef != "" {
				t.Fatalf("top-level E created an intermediate image publication: base=%q image=%q", pipeline.baseRef, pipeline.baseImageRef)
			}
			assertStrictPublishedSandbox(t, pipeline, ref, fixture.imageConfig)

			manifestRoots := publicationManifestRoots(t, fixture.storeRoot)
			if test.manifestBundle {
				if len(manifestRoots) != 0 {
					t.Fatalf("Bundle policy wrote Manifest store roots: %v", manifestRoots)
				}
				assertOnlyBundleFiles(t, strings.TrimPrefix(fixture.locationParent, "file://"), 1)
			} else {
				if len(manifestRoots) != 1 || !strings.HasSuffix(ref, manifestRoots[0]) {
					t.Fatalf("Manifest roots = %v, final ref=%q; want only final E", manifestRoots, ref)
				}
				if count := countPublicationFiles(t, strings.TrimPrefix(fixture.locationParent, "file://")); count != 0 {
					t.Fatalf("manifest=false placed top-level E in checkpoint location (%d files)", count)
				}
			}
			for _, root := range []string{pipeline.spec.RunDir, pipeline.spec.BaseDir} {
				assertNoCompleteSandboxStaging(t, root)
			}
		})
	}
}

func TestPublishSandboxTargetAcceptsPortableImageCarriers(t *testing.T) {
	fixture := newPublicationFixture(t)
	manifestRef := fixture.publishManifestImage(t)
	locatedRef, locatedMappings := fixture.publishLocatedImage(t)
	for _, test := range []struct {
		name           string
		buildID        string
		ref            string
		locations      map[string]string
		manifestBundle bool
		wantScheme     string
	}{
		{
			name: "Manifest image to Sandbox Bundle", buildID: "build-sandbox-manifest", ref: manifestRef,
			manifestBundle: true, wantScheme: manifest.RefSchemeFile,
		},
		{
			name: "located image Bundle to Sandbox Manifest", buildID: "build-sandbox-located", ref: locatedRef,
			locations: locatedMappings, wantScheme: manifest.RefSchemeManifest,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			pipeline := fixture.pipeline(t, test.buildID, test.ref, test.locations, test.manifestBundle)
			pipeline.target = types.BuildTarget{Kind: types.BuildTargetSandbox}
			ref, err := pipeline.publishSandboxTarget()
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := manifest.ParseRef(ref)
			if err != nil || parsed.Scheme != test.wantScheme {
				t.Fatalf("top-level Sandbox E ref = %q, parsed=%+v err=%v", ref, parsed, err)
			}
			assertStrictPublishedSandbox(t, pipeline, ref, fixture.imageConfig)
		})
	}
}

func TestPublishImageTargetBundlesEverySupportedCarrier(t *testing.T) {
	fixture := newPublicationFixture(t)
	manifestRef := fixture.publishManifestImage(t)
	locatedRef, locatedMappings := fixture.publishLocatedImage(t)
	storeRootsBefore := publicationManifestRoots(t, fixture.storeRoot)

	for _, test := range []struct {
		name      string
		ref       string
		locations map[string]string
	}{
		{name: "local digest-qualified file", ref: fixture.localImageRef},
		{name: "Manifest image", ref: manifestRef},
		{name: "located Bundle image", ref: locatedRef, locations: locatedMappings},
	} {
		t.Run(test.name, func(t *testing.T) {
			buildID := "build-image-" + strings.ReplaceAll(test.name, " ", "-")
			pipeline := fixture.pipeline(t, buildID, test.ref, test.locations, true)
			ref, err := pipeline.publishImageTarget()
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := manifest.ParseRef(ref)
			if err != nil || parsed.Scheme != manifest.RefSchemeFile || parsed.Location == "" ||
				parsed.DigestScheme != "manifest" || filepath.Ext(parsed.Path) != ".bundle" {
				t.Fatalf("published image ref = %q, parsed=%+v err=%v", ref, parsed, err)
			}
			opened, err := rtsandbox.OpenFlattenedImage(fixture.ctx, ref, pipeline.artifacts.storage, pipeline.artifacts.locations)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(opened.ImageConfig, fixture.imageConfig) {
				_ = opened.Close()
				t.Fatal("Bundle publication changed config.json")
			}
			if err := opened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	if got := publicationManifestRoots(t, fixture.storeRoot); !sameStrings(got, storeRootsBefore) {
		t.Fatalf("image Bundle publications changed Manifest store: before=%v after=%v", storeRootsBefore, got)
	}
	assertOnlyBundleFiles(t, strings.TrimPrefix(fixture.locationParent, "file://"), 3)
}

func TestReadBaseRuntimeConfigSupportsEveryImageCarrier(t *testing.T) {
	fixture := newPublicationFixture(t)
	manifestRef := fixture.publishManifestImage(t)
	locatedRef, locatedMappings := fixture.publishLocatedImage(t)

	for _, test := range []struct {
		name      string
		ref       string
		locations map[string]string
	}{
		{name: "local digest-qualified file", ref: fixture.localImageRef},
		{name: "Manifest image", ref: manifestRef},
		{name: "located Bundle image", ref: locatedRef, locations: locatedMappings},
	} {
		t.Run(test.name, func(t *testing.T) {
			pipeline := fixture.pipeline(t, "build-read-"+strings.ReplaceAll(test.name, " ", "-"), test.ref, test.locations, true)
			got, err := pipeline.readBaseRuntimeConfig()
			if err != nil {
				t.Fatal(err)
			}
			cmd, env := anyStrings(got["Cmd"]), anyStrings(got["Env"])
			if len(cmd) != 2 || cmd[0] != "/bin/service" || cmd[1] != "serve" ||
				len(env) != 1 || env[0] != "IMAGE=yes" || got["WorkingDir"] != "/srv" {
				t.Fatalf("runtime config = %#v", got)
			}
		})
	}
}

func TestManifestImagePolicyPublishesPhaseCBaseToStore(t *testing.T) {
	fixture := newPublicationFixture(t)
	pipeline := fixture.pipeline(t, "build-memory-manifest", fixture.localImageRef, nil, false)
	pipeline.target = types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true}
	if err := pipeline.publishPhaseCImage(); err != nil {
		t.Fatal(err)
	}
	parsed, err := manifest.ParseRef(pipeline.baseRef)
	if err != nil || parsed.Scheme != manifest.RefSchemeManifest || parsed.Location != "" {
		t.Fatalf("Phase-C Manifest image = %q, parsed=%+v err=%v", pipeline.baseRef, parsed, err)
	}
	if roots := publicationManifestRoots(t, fixture.storeRoot); len(roots) != 1 || !strings.HasSuffix(pipeline.baseRef, roots[0]) {
		t.Fatalf("Phase-C image Manifest roots = %v, ref=%q", roots, pipeline.baseRef)
	}
	if count := countPublicationFiles(t, strings.TrimPrefix(fixture.locationParent, "file://")); count != 0 {
		t.Fatalf("manifest=false placed Phase-C image in checkpoint location (%d files)", count)
	}
	args, err := publishCheckpointArtifactArgs(
		pipeline.spec, CheckpointClassRefLocation, "/base/capture.snapshot",
	)
	if err != nil {
		t.Fatal(err)
	}
	if indexOf(args, "--to-ref-location") < 0 {
		t.Fatalf("checkpoint graph did not retain its independent location policy: %#v", args)
	}
}

func TestPublishPhaseCImageInstallsPortableMappingForRunAndCheckpoint(t *testing.T) {
	fixture := newPublicationFixture(t)
	pipeline := fixture.pipeline(t, "build-memory", fixture.localImageRef, nil, true)
	pipeline.target = types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true}
	if err := pipeline.publishPhaseCImage(); err != nil {
		t.Fatal(err)
	}
	parsed, err := manifest.ParseRef(pipeline.baseRef)
	if err != nil || parsed.Location == "" || pipeline.spec.RefLocations[parsed.Location] == "" {
		t.Fatalf("Phase-C image = %q mappings=%v err=%v", pipeline.baseRef, pipeline.spec.RefLocations, err)
	}
	args := phaseSandboxRunArgs(pipeline.spec, "c", "phase-c", "/run/c/sandbox.yaml", nil, pipeline.baseRef, true)
	if indexOf(args, "--replace-boot") < 0 || !argumentPair(args, "--from", pipeline.baseRef) ||
		!argumentPair(args, "--ref-location", parsed.Location+"="+pipeline.spec.RefLocations[parsed.Location]) {
		t.Fatalf("Phase-C argv does not carry portable image mapping: %#v", args)
	}

	checkpointArgs, err := publishCheckpointArtifactArgs(
		pipeline.spec, CheckpointClassRefLocation, "/base/capture.snapshot",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !argumentPair(checkpointArgs, "--ref-location", parsed.Location+"="+pipeline.spec.RefLocations[parsed.Location]) {
		t.Fatalf("checkpoint publisher lost image mapping: %#v", checkpointArgs)
	}
	checkpointName := reflocation.PublicationName(pipeline.spec.BuildID)
	if !argumentPrefix(checkpointArgs, "--to-ref-location", checkpointName+"=") {
		t.Fatalf("checkpoint publication did not mint its own name: image=%q argv=%#v", parsed.Location, checkpointArgs)
	}
}

func TestImageBundlePublicationRetryReusesAndRejectsCorruptFinal(t *testing.T) {
	fixture := newPublicationFixture(t)
	first := fixture.pipeline(t, "build-retry", fixture.localImageRef, nil, true)
	firstRef, err := first.publishImageTarget()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := manifest.ParseRef(firstRef)
	if err != nil {
		t.Fatal(err)
	}
	finalPath, err := first.artifacts.locations.ResolveFile(parsed, "")
	if err != nil {
		t.Fatal(err)
	}

	retry := fixture.pipeline(t, "build-retry", fixture.localImageRef, nil, true)
	reusedRef, err := retry.publishImageTarget()
	if err != nil || reusedRef != firstRef {
		t.Fatalf("valid same-key retry = %q, %v; want %q", reusedRef, err, firstRef)
	}
	if err := os.WriteFile(finalPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	corrupt := fixture.pipeline(t, "build-retry", fixture.localImageRef, nil, true)
	corruptCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	corrupt.ctx = corruptCtx
	ref, err := corrupt.publishImageTarget()
	if err == nil || ref != "" {
		t.Fatalf("corrupt existing final publication = ref %q err %v", ref, err)
	}
	assertNoPartialPublicationFiles(t, strings.TrimPrefix(fixture.locationParent, "file://"))
}

func TestCanceledImagePublicationReturnsNoRef(t *testing.T) {
	fixture := newPublicationFixture(t)
	pipeline := fixture.pipeline(t, "build-canceled", fixture.localImageRef, nil, false)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	pipeline.ctx = canceled
	ref, err := pipeline.publishImageTarget()
	if !errors.Is(err, context.Canceled) || ref != "" {
		t.Fatalf("canceled image publication = ref %q err %v", ref, err)
	}
}

func assertStrictPublishedSandbox(t *testing.T, pipeline *buildPipeline, raw string, wantImageConfig []byte) {
	t.Helper()
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		t.Fatal(err)
	}
	var stream interface {
		sparse.Source
		Close() error
	}
	switch ref.Scheme {
	case manifest.RefSchemeManifest:
		key, err := manifest.ParseKeyRef(ref.Path)
		if err != nil {
			t.Fatal(err)
		}
		stream, err = pipeline.artifacts.storage.Fetcher().OpenManifest(pipeline.ctx, key)
		if err != nil {
			t.Fatal(err)
		}
	case manifest.RefSchemeFile:
		path, err := pipeline.artifacts.locations.ResolveFile(ref, "")
		if err != nil {
			t.Fatal(err)
		}
		stream, err = pipeline.artifacts.storage.OpenFileWithLocations(pipeline.ctx, path, ref, pipeline.artifacts.locations)
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported test ref %q", raw)
	}
	root, err := sandboxfile.Open(pipeline.ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if root.ArchiveBase != 3*4096 || !bytes.Equal(root.ImageConfig, wantImageConfig) ||
		root.Portable.Boot.Root.Base != "self" || root.Portable.Boot.Root.Overlay == nil {
		t.Fatalf("strict top-level Sandbox E = base %d root %+v", root.ArchiveBase, root.Portable.Boot.Root)
	}
	run, err := root.Payload.RunAt(4096, 4096)
	if err != nil || run.Kind() != sparse.Hole {
		t.Fatalf("top-level E sparse hole = %v, err=%v", run, err)
	}
}

func publicationManifestRoots(t *testing.T, storeRoot string) []string {
	t.Helper()
	root := filepath.Join(storeRoot, string(acceleratorstore.PartitionManifest), "G1")
	var names []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			names = append(names, entry.Name())
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func countPublicationFiles(t *testing.T, root string) int {
	t.Helper()
	count := 0
	err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			count++
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func assertOnlyBundleFiles(t *testing.T, root string, want int) {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			files = append(files, path)
			if filepath.Ext(path) != ".bundle" {
				t.Errorf("named image-class location contains non-Bundle %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != want {
		t.Fatalf("named image-class files = %v, want %d Bundles", files, want)
	}
}

func assertNoCompleteSandboxStaging(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			switch filepath.Ext(path) {
			case ".sandbox", ".snapshot", ".bundle":
				t.Errorf("complete artifact staged under Build directory: %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertNoPartialPublicationFiles(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.Contains(entry.Name(), ".partial") {
			t.Errorf("visible partial publication remains: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func argumentPair(args []string, flag, value string) bool {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == flag && args[index+1] == value {
			return true
		}
	}
	return false
}

func argumentPrefix(args []string, flag, prefix string) bool {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == flag && strings.HasPrefix(args[index+1], prefix) {
			return true
		}
	}
	return false
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, value := range a {
		seen[value]++
	}
	for _, value := range b {
		seen[value]--
	}
	for _, count := range seen {
		if count != 0 {
			return false
		}
	}
	return true
}

func anyStrings(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil
		}
		out = append(out, text)
	}
	return out
}
