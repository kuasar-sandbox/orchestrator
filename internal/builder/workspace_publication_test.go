package builder

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"github.com/kuasar-sandbox/orchestrator/internal/workspacedisk"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

func TestWorkspacePublishedGraphKeepsEveryLayer(t *testing.T) {
	for _, target := range []string{"bundle", "store"} {
		t.Run(target, func(t *testing.T) {
			fixture := newPublicationFixture(t)
			p := fixture.pipeline(t, "workspace-publication", fixture.localImageRef, nil, target == "bundle")
			ctx, a := p.ctx, p.artifacts
			image, err := p.openCurrentImage()
			if err != nil {
				t.Fatal(err)
			}
			defer image.Close()
			payload := make([]byte, image.Payload.Size())
			if _, err = image.Payload.ReadAt(ctx, payload, 0); err != nil {
				t.Fatal(err)
			}
			sourceDir := t.TempDir()
			input := snapshot.NewFileSink(sourceDir, "source", nil, false, nil)
			digest := strings.Repeat("a", 64)
			cold := &rtconfig.PortableSandboxConfig{
				Version: 2,
				Boot: rtconfig.PortableBootConfig{Kernel: "file://kernel@digest:" + digest, Runtime: "file://runtime@digest:" + digest,
					Root: rtconfig.PortableRootConfig{Base: "self", Overlay: &rtconfig.PortableOverlayConfig{}}, Disks: []rtconfig.PortableDiskConfig{{Name: "cache"}, {Name: "empty", Slot: &rtconfig.DiskSlotConfig{}}}},
				Resources: rtconfig.PortableResourcesConfig{Capacity: rtconfig.CapacityConfig{CPU: 1, Memory: "128MiB"}, Allocatable: rtconfig.AllocatableConfig{CPU: 1, Memory: "128MiB"}},
				Launch:    rtconfig.PortableLaunchConfig{Exec: "/bin/true", Workdir: "/", Restart: "never"},
				Mounts:    []rtconfig.MountConfig{{Type: "disk", Source: "cache", Target: "/cache"}, {Type: "disk", Source: "empty", Target: "/empty"}}, Metadata: map[string]string{"fixture": "workspace-view"},
			}
			host := &rtconfig.SandboxConfig{}
			host.Boot.Disks = make([]rtconfig.DiskConfig, 2)
			tags := []byte{0x29, 0x43}
			var refs []string
			for _, tag := range tags {
				layer, path, err := input.AbsorbOverlay(ctx, bytes.NewReader(bytes.Repeat([]byte{tag}, 4096)), nil)
				if err != nil {
					t.Fatal(err)
				}
				refs = append(refs, layer)
				full, err := manifest.ParseRef(layer)
				if err != nil {
					t.Fatal(err)
				}
				full.Path = path
				if len(refs) == 1 {
					host.Boot.Disks[0].Base = full.String()
				} else {
					host.Boot.Disks[0].BaseFromRefs = []string{full.String()}
				}
			}
			cold.Boot.Disks[0].Base = refs[0]
			cold.Boot.Disks[0].BaseFromRefs = refs[1:]
			result, err := p.publishWorkspaceTarget(image, host, cold)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(sourceDir); err != nil {
				t.Fatal(err)
			}
			ref, err := manifest.ParseRef(result)
			if err != nil {
				t.Fatal(err)
			}
			var parent fetch.Stream
			var parentFile *artifact.OpenedFile
			var path string
			if target == "store" {
				if ref.Scheme != manifest.RefSchemeManifest {
					t.Fatalf("store E %q", result)
				}
				key, err := manifest.ParseKeyRef(ref.Path)
				if err != nil {
					t.Fatal(err)
				}
				parent, err = a.storage.Fetcher().OpenManifest(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				if roots := publicationManifestRoots(t, fixture.storeRoot); len(roots) != 3 {
					t.Fatalf("store manifests %v, want one E and two layer members", roots)
				}
			} else {
				path, err = a.locations.ResolveFile(ref, "")
				if err != nil {
					t.Fatal(err)
				}
				entries, err := os.ReadDir(filepath.Dir(path))
				if err != nil || len(entries) != 1 || entries[0].Name() != filepath.Base(path) || ref.Location != p.spec.BuildID || ref.DigestScheme != "manifest" {
					t.Fatalf("named bundle ref %q, entries %v: %v", result, entries, err)
				}
				if err := os.RemoveAll(p.spec.RunDir); err != nil {
					t.Fatal(err)
				}
				parentFile, err = a.storage.OpenFileWithLocations(ctx, path, ref, a.locations)
				if err != nil {
					t.Fatal(err)
				}
				parent = parentFile
			}
			root, err := sandboxfile.Open(ctx, parent)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			got := make([]byte, len(payload))
			if _, err = root.Payload.ReadAt(ctx, got, 0); err != nil || !bytes.Equal(got, payload) || !bytes.Equal(root.ImageConfig, image.ImageConfig) {
				t.Fatalf("root payload: %v", err)
			}
			portable := root.Portable
			if len(portable.Boot.Disks) != 2 || portable.Boot.Disks[0].Name != "cache" || !portable.Boot.Disks[1].Unbound() || !reflect.DeepEqual(portable.Mounts, cold.Mounts) || !reflect.DeepEqual(portable.Metadata, cold.Metadata) {
				t.Fatalf("reopened topology %+v", portable)
			}
			disk := portable.Boot.Disks[0]
			if len(disk.BaseFromRefs) != 1 {
				t.Fatalf("reopened cache layers %+v", disk)
			}
			for i, raw := range []string{disk.Base, disk.BaseFromRefs[0]} {
				ref, err := manifest.ParseRef(raw)
				if err != nil {
					t.Fatal(err)
				}
				var stream fetch.Stream
				if ref.Scheme != manifest.RefSchemeManifest {
					t.Fatalf("disk member %q", raw)
				}
				key, err := manifest.ParseKeyRef(ref.Path)
				if err != nil {
					t.Fatal(err)
				}
				if target == "bundle" {
					stream, err = parentFile.ManifestFetcher().OpenManifest(ctx, key)
				} else {
					stream, err = a.storage.Fetcher().OpenManifest(ctx, key)
				}
				if err != nil {
					t.Fatal(err)
				}
				buf := make([]byte, stream.Size())
				_, err = stream.ReadAt(ctx, buf, 0)
				if closeErr := stream.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
				if err != nil || !bytes.Equal(buf, bytes.Repeat([]byte{tags[i]}, 4096)) {
					t.Fatalf("disk layer %d content: %v", i, err)
				}
			}
		})
	}
}

func TestWorkspaceMemoryTargetKeepsPublishedImageInCheckpointRoot(t *testing.T) {
	for _, bundle := range []bool{false, true} {
		name := "store-image"
		if bundle {
			name = "located-image"
		}
		t.Run(name, func(t *testing.T) {
			f := newPublicationFixture(t)
			p := f.pipeline(t, "workspace-memory", f.localImageRef, nil, bundle)
			p.target = types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true}
			upper := bytes.Repeat([]byte{0x60}, 8192)
			upper[1080], upper[1081] = 0x53, 0xef
			if err := os.WriteFile(f.overlay, upper, 0600); err != nil {
				t.Fatal(err)
			}
			// An already bound native slot has no equivalent host YAML fields.
			// Supply the same staged E a derived memory target would inherit.
			sourceDir := p.phaseRunDir("workspace")
			if err := os.MkdirAll(sourceDir, 0700); err != nil {
				t.Fatal(err)
			}
			sink := snapshot.NewFileSink(sourceDir, "source", nil, false, nil)
			diskRef, _, err := sink.AbsorbOverlay(p.ctx, bytes.NewReader(bytes.Repeat([]byte{0x42}, 4096)), nil)
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := p.buildColdConfig()
			if err != nil {
				t.Fatal(err)
			}
			ids, err := sandbox.ResolvePortableProjection(cfg)
			if err != nil {
				t.Fatal(err)
			}
			parentConfig, err := rtconfig.NewPortableEROFS(cfg, ids)
			if err != nil {
				t.Fatal(err)
			}
			binding := rtconfig.SlotBinding{ID: "initial", SourceID: "original", SizeBytes: 4096}
			parentConfig.Version = 2
			parentConfig.Boot.Disks = []rtconfig.PortableDiskConfig{
				{Name: "empty", Slot: &rtconfig.DiskSlotConfig{}},
				{Name: "ready", Slot: &rtconfig.DiskSlotConfig{}, PureBlock: true, Binding: &binding, PortableRootConfig: rtconfig.PortableRootConfig{Base: diskRef}},
			}
			plan := []workspace.Prepared{
				{Name: "empty", Mounts: []workspace.Mount{{Path: "/empty"}}, Slot: &rtconfig.DiskSlotConfig{}},
				{Name: "ready", Mounts: []workspace.Mount{{Path: "/ready"}}, Slot: &rtconfig.DiskSlotConfig{}},
			}
			parentConfig.Mounts = []rtconfig.MountConfig{{Type: "disk", Source: "empty", Target: "/empty"}, {Type: "disk", Source: "ready", Target: "/ready"}}
			parentConfig.Metadata = map[string]string{workspace.Namespace: workspace.Topology(plan)}
			p.spec.SandboxSpec.WorkspacePlan = plan
			p.spec.SourceSandboxConfig = parentConfig
			if err := p.publishPhaseCImage(); err != nil {
				t.Fatal(err)
			}
			immutableImage := p.baseRef
			doc, from, replace, err := p.templateBoot()
			if err != nil || replace {
				t.Fatalf("template boot from=%q replace=%t: %v", from, replace, err)
			}
			host, presence, err := rtconfig.LoadConfigBytesWithPresence(doc)
			if err != nil {
				t.Fatal(err)
			}
			source, err := sandbox.OpenRunSource(p.ctx, from, p.artifacts.storage, p.artifacts.locations)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			_, launch, err := source.Apply(host, presence, rtconfig.ApplyFromOptions{})
			if err != nil {
				t.Fatal(err)
			}
			parent := launch.SourceBinding.SandboxRef
			portable := launch.PortableConfig
			if portable.Boot.Root.Base != immutableImage || portable.Boot.Root.Overlay.Base != "self" || !portable.Boot.Disks[0].Unbound() || !reflect.DeepEqual(portable.Boot.Disks[1].Binding, &binding) {
				t.Fatalf("phase C disk graph = %+v; immutable image = %q", portable.Boot, immutableImage)
			}
			for _, mergeUpper := range []bool{false, true} {
				captured, err := portable.Exported(parent, []string{"", diskRef}, []bool{mergeUpper, false, true})
				if err != nil {
					t.Fatal(err)
				}
				if captured.Boot.Root.Base != immutableImage || captured.Boot.Root.Overlay.Base != "self" || !captured.Boot.Disks[0].Unbound() || !reflect.DeepEqual(captured.Boot.Disks[1].Binding, &binding) {
					t.Fatalf("checkpoint root after merge=%v: %+v", mergeUpper, captured.Boot)
				}
				var want []string
				if !mergeUpper {
					want = []string{parent}
				}
				if got := captured.Boot.Root.Overlay.BaseFromRefs; !slices.Equal(got, want) {
					t.Fatalf("upper chain after merge=%v = %v, want %v", mergeUpper, got, want)
				}
				// Publish the paired E and S with the real checkpoint publisher,
				// using separate image and checkpoint destinations.
				checkWorkspaceSnapshotPublication(t, p, captured, launch.SourceBinding.RelativeDir, immutableImage, mergeUpper)

			}
			ref, err := manifest.ParseRef(from)
			if err != nil {
				t.Fatal(err)
			}
			file, err := p.artifacts.storage.OpenFileWithLocations(p.ctx, ref.Path, ref, p.artifacts.locations)
			if err != nil {
				t.Fatal(err)
			}
			root, err := sandboxfile.Open(p.ctx, file)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			got := make([]byte, root.Payload.Size())
			if _, err := root.Payload.ReadAt(p.ctx, got, 0); err != nil || !bytes.Equal(got, upper) {
				t.Fatalf("private E did not retain the initial root upper: %v", err)
			}
		})
	}
}

func checkWorkspaceSnapshotPublication(t *testing.T, p *buildPipeline, cfg *rtconfig.PortableSandboxConfig, dir, immutableImage string, merged bool) {
	t.Helper()
	sink := snapshot.NewFileSink(dir, "final", nil, false, nil)
	c1, err := rtconfig.MarshalPortableSandboxConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := sparse.NewSource(bytes.NewReader(make([]byte, 4096)), 4096, nil)
	if err != nil {
		t.Fatal(err)
	}
	e, err := sandboxfile.BuildSourceContext(p.ctx, payload, nil, c1)
	if err != nil {
		t.Fatal(err)
	}
	eRef, _, err := sink.AbsorbSandbox(p.ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := snapshot.MarshalConfig(&snapshot.Config{Version: snapshot.SnapshotConfigVersion, SandboxRef: eRef})
	if err != nil {
		t.Fatal(err)
	}
	s, err := snapshotfile.BuildSource(payload, []byte("{}"), []byte("{}"), metadata)
	if err != nil {
		t.Fatal(err)
	}
	_, sPath, err := sink.AbsorbSnapshot(p.ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	a := p.artifacts
	loc := "checkpoints"
	a.locations[loc] = t.TempDir()
	publisher, err := artifact.NewLocationPublisher(a.storage, loc, a.locations[loc], a.locations, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	result, err := publisher.Publish(p.ctx, sPath)
	if err != nil {
		t.Fatal(err)
	}
	openRef := func(raw string) fetch.Stream {
		t.Helper()
		ref, err := manifest.ParseRef(raw)
		if err != nil {
			t.Fatal(err)
		}
		stream, err := workspacedisk.OpenLayer(p.ctx, a.storage, ref, a.locations[loc], a.locations)
		if err != nil {
			t.Fatal(err)
		}
		return stream
	}
	publishedS, err := snapshotfile.Open(p.ctx, openRef(result.Ref))
	if err != nil {
		t.Fatal(err)
	}
	defer publishedS.Close()
	meta, err := snapshot.ParseConfig(publishedS.SnapshotConfig)
	if err != nil {
		t.Fatal(err)
	}
	publishedE, err := sandboxfile.Open(p.ctx, openRef(meta.SandboxRef))
	if err != nil {
		t.Fatal(err)
	}
	defer publishedE.Close()
	if got := publishedE.Portable.Boot.Root.Base; got != immutableImage {
		t.Fatalf("published SNP image (merge=%t) = %q, want %q", merged, got, immutableImage)
	}
	disk := publishedE.Portable.Boot.Disks[1]
	if disk.Binding == nil || disk.Binding.ID != "initial" {
		t.Fatalf("published SNP lost its native binding: %+v", disk)
	}
	data := openRef(disk.Base)
	defer data.Close()
	got := make([]byte, 4096)
	if _, err := data.ReadAt(p.ctx, got, 0); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{0x42}, len(got))) {
		t.Fatalf("published SNP disk bytes: %v", err)
	}
}
