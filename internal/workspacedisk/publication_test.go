package workspacedisk

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

func TestWorkspaceExportRetainsPublishedLayersWithoutReadingThem(t *testing.T) {
	for _, format := range []string{"manifest", "digest", "hmac"} {
		t.Run(format, func(t *testing.T) {
			ctx := t.Context()
			const page = 4096
			configPath, key := workspacePublicationConfig(t)
			storage, err := Storage(configPath, key)
			requireWorkspaceIO(t, err)
			defer storage.Close()
			parent := "file://" + t.TempDir()
			location, err := reflocation.Resolve(parent, "published")
			requireWorkspaceIO(t, err)
			requireWorkspaceIO(t, os.MkdirAll(location.Path, 0700))
			parentData := bytes.Repeat([]byte{'P'}, 3*page)
			var parentRef manifest.Ref
			if format == "manifest" {
				sink, err := snapshot.NewBundleSink(ctx, location.Path, "parent", storage.ManifestConfig(), storage.CustomerKeyFunc(), nil)
				requireWorkspaceIO(t, err)
				defer sink.Close()
				raw, _, err := sink.AbsorbOverlay(ctx, bytes.NewReader(parentData), nil)
				requireWorkspaceIO(t, err)
				path, err := sink.CommitDisk(ctx, []string{raw})
				requireWorkspaceIO(t, err)
				selected, err := manifest.ParseRef(raw)
				requireWorkspaceIO(t, err)
				parentRef = manifest.Ref{Scheme: manifest.RefSchemeFile, Path: filepath.Base(path), DigestScheme: "manifest", Digest: selected.Path}
			} else {
				sink := snapshot.NewFileSink(location.Path, "parent", nil, false, nil)
				if format == "hmac" {
					keyBytes, err := manifest.ParseHexKey(key)
					requireWorkspaceIO(t, err)
					codec, err := manifestcrypto.NewTarStreamCodec([32]byte(keyBytes))
					requireWorkspaceIO(t, err)
					sink = snapshot.NewFileSink(location.Path, "parent", codec, true, nil)
				}
				raw, _, err := sink.AbsorbOverlay(ctx, bytes.NewReader(parentData), nil)
				requireWorkspaceIO(t, err)
				parentRef, err = manifest.ParseRef(raw)
				requireWorkspaceIO(t, err)
				requireWorkspaceIO(t, sink.Close())
			}
			parentRef.Location = "published"
			original := parentRef.String()
			source := t.TempDir()
			sink := snapshot.NewFileSink(source, "source", nil, false, nil)
			defer sink.Close()
			data := make([]byte, 3*page)
			copy(data[page:2*page], bytes.Repeat([]byte{'T'}, page))
			top, _, err := sink.AbsorbOverlay(ctx, bytes.NewReader(data), []sparse.Extent{{Offset: 0, Size: page}})
			requireWorkspaceIO(t, err)
			req := savedWorkspaceRequest(t, source, rtconfig.PortableDiskConfig{Name: "work", PureBlock: true,
				PortableRootConfig: rtconfig.PortableRootConfig{Base: top, BaseFromRefs: []string{original}}})
			req.ManifestConfig, req.RefLocationParent = configPath, parent
			// A missing published directory makes every attempted parent open fail.
			// Export must still publish the private top and keep the parent identity.
			requireWorkspaceIO(t, os.Rename(location.Path, location.Path+".away"))
			result, err := execute(ctx, req, key)
			requireWorkspaceIO(t, err)
			if result.Disk == nil || !slices.Equal(result.Disk.BaseFromRefs, []string{original}) || !strings.HasSuffix(result.Disk.Base, "@location:source") {
				t.Fatalf("export rewrote a published parent: %+v", result.Disk)
			}
			published, err := manifest.ParseRef(result.Disk.Base)
			requireWorkspaceIO(t, err)
			if published.Scheme != manifest.RefSchemeFile || published.DigestScheme != "manifest" || filepath.Ext(published.Path) != ".bundle" || filepath.Base(published.Path) != published.Path {
				t.Fatalf("named export changed the Bundle representation: %s", result.Disk.Base)
			}
			output, err := reflocation.Resolve(parent, "source")
			requireWorkspaceIO(t, err)
			files, err := os.ReadDir(output.Path)
			requireWorkspaceIO(t, err)
			if len(files) != 1 || files[0].Name() != published.Path {
				t.Fatalf("expected one committed Bundle, got %v", files)
			}
			material := migrationtoken.KeyMaterial{APISecret: strings.Repeat("b", 64), ManifestKey: key}
			token, err := migrationtoken.SealWorkspace(material, *result.Disk)
			requireWorkspaceIO(t, err)
			// Source ownership can end after publication. Published parents retain
			// their own lifetime and become available at the receiving node.
			requireWorkspaceIO(t, os.RemoveAll(source))
			requireWorkspaceIO(t, os.Rename(location.Path+".away", location.Path))
			input, err := json.Marshal(map[string]string{"migrationToken": token})
			requireWorkspaceIO(t, err)
			options := Options{APISecret: material.APISecret, ManifestKey: key, ManifestConfig: configPath, RefLocationParent: parent}
			disk, locations, check, err := Plan(input, options)
			requireWorkspaceIO(t, err)
			if check || disk.Base != result.Disk.Base || !slices.Equal(disk.BaseFromRefs, []string{original}) {
				t.Fatalf("authenticated token lost its graph: %+v check=%t", disk, check)
			}
			// Use the native source opener shared by Create and runtime BindDisk.
			reader, size, template, err := sandbox.OpenDiskV1Source(ctx, disk, storage, locations)
			requireWorkspaceIO(t, err)
			defer reader.Close()
			if size != 3*page || template != "" {
				t.Fatalf("imported geometry/template: %d %q", size, template)
			}
			got := make([]byte, size)
			n, err := reader.ReadAt(got, 0)
			requireWorkspaceIO(t, err)
			copy(data[:page], parentData[:page])
			if n != len(data) || !bytes.Equal(got, data) {
				t.Fatal("handoff lost inherited bytes, new writes or explicit zeroes")
			}
			inline, err := json.Marshal(disk)
			requireWorkspaceIO(t, err)
			if format != "manifest" {
				if _, _, _, err := Plan(inline, options); !errors.Is(err, ErrUnauthorized) {
					t.Fatalf("untrusted inline tarstream bypassed authorization: %v", err)
				}
			} else {
				_, _, err := Resolve(ctx, inline, options)
				requireWorkspaceIO(t, err)
			}
			options.APISecret = strings.Repeat("c", 64)
			if _, _, _, err := Plan(input, options); !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("token accepted another tenant: %v", err)
			}
		})
	}
}

func TestWorkspaceExportRetainedGraphNeedsNoLocationOrStore(t *testing.T) {
	configPath, key := workspacePublicationConfig(t)
	base := "manifest://" + strings.Repeat("d", 64)
	parents := []string{
		"file://remote.bundle@manifest:" + strings.Repeat("e", 64) + "@location:remote",
		"file://old.overlay@hmac:" + strings.Repeat("f", 64) + "@location:old",
	}
	req := savedWorkspaceRequest(t, t.TempDir(), rtconfig.PortableDiskConfig{Name: "work", PureBlock: true,
		Slot: &rtconfig.DiskSlotConfig{}, Binding: &rtconfig.SlotBinding{ID: "bound", SourceID: "saved", SizeBytes: 8192},
		PortableRootConfig: rtconfig.PortableRootConfig{Base: base, BaseFromRefs: parents}})
	req.ManifestConfig = configPath // No Store endpoint.
	for _, named := range []bool{false, true} {
		label := "store"
		if named {
			label = "named"
		}
		t.Run(label, func(t *testing.T) {
			request := req
			var destination string
			if named {
				destination = filepath.Join(t.TempDir(), "not-created")
				request.RefLocationParent = "file://" + destination
			}
			result, err := execute(t.Context(), request, key)
			requireWorkspaceIO(t, err)
			if result.Disk == nil || result.Disk.Base != base || !slices.Equal(result.Disk.BaseFromRefs, parents) || *result.Disk.Size != 8192 {
				t.Fatalf("already published disk changed: %+v", result.Disk)
			}
			if named {
				if _, err := os.Stat(destination); !os.IsNotExist(err) {
					t.Fatalf("retained graph created a publication directory: %v", err)
				}
			}
		})
	}
}

func workspacePublicationConfig(t *testing.T) (string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "manifest.yaml")
	requireWorkspaceIO(t, os.WriteFile(path, []byte("chunker: {mode: fixed, fixed: {size: 4KiB}}\ncrypto: {chunk: aes, manifest: aes}\n"), 0600))
	return path, strings.Repeat("a", 64)
}

func savedWorkspaceRequest(t *testing.T, source string, disk rtconfig.PortableDiskConfig) Request {
	t.Helper()
	key := strings.Repeat("a", 64)
	cfg := rtconfig.PortableSandboxConfig{
		Version: 2, Resources: rtconfig.PortableResourcesConfig{Capacity: rtconfig.CapacityConfig{CPU: 1, Memory: "1GiB"}, Allocatable: rtconfig.AllocatableConfig{CPU: 1, Memory: "1GiB"}},
		Boot: rtconfig.PortableBootConfig{Kernel: "file://kernel@digest:" + key, Runtime: "file://runtime@digest:" + key,
			Root: rtconfig.PortableRootConfig{Base: "self"}, Disks: []rtconfig.PortableDiskConfig{disk}},
		Launch:   rtconfig.PortableLaunchConfig{Workdir: "/", Restart: "never"},
		Mounts:   []rtconfig.MountConfig{{Type: "disk", Source: "work", Target: "/work"}},
		Metadata: map[string]string{workspace.Namespace: `[{"name":"work","mounts":[{"path":"/work"}]}]`},
	}
	raw, err := rtconfig.MarshalPortableSandboxConfig(&cfg)
	requireWorkspaceIO(t, err)
	e, err := sandboxfile.BuildSource(sparse.Dense(bytes.NewReader(make([]byte, 4096)), 4096), nil, raw)
	requireWorkspaceIO(t, err)
	sink := snapshot.NewFileSink(source, "source", nil, false, nil)
	defer sink.Close()
	ref, path, err := sink.AbsorbSandbox(t.Context(), e)
	requireWorkspaceIO(t, err)
	requireWorkspaceIO(t, sink.CommitSandbox(t.Context(), ref, path))
	return Request{Operation: "export", Source: ref, SourceDir: source, Name: "work", Publication: "source"}
}

func requireWorkspaceIO(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
