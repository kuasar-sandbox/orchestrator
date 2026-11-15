package taskartifact

import (
	"bytes"
	"context"
	"encoding/hex"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

func TestFreshSBXAppendsDevicesAndPreservesSavedDiskVersions(t *testing.T) {
	for _, carrier := range []string{"ordinary", "ordinary-bundle", "checkpoint", "checkpoint-bundle"} {
		t.Run(carrier, func(t *testing.T) {
			ctx := context.Background()
			output := t.TempDir()
			payload := bytes.Repeat([]byte{0x73}, 8192)
			data := bytes.Repeat([]byte{0x29}, 4096)
			digest := strings.Repeat("a", 64)
			cfg := &rtconfig.PortableSandboxConfig{
				Version: 2,
				Boot: rtconfig.PortableBootConfig{
					Kernel:  "file://kernel@digest:" + digest,
					Runtime: "file://runtime@digest:" + digest,
					Root:    rtconfig.PortableRootConfig{Base: "self"},
					Disks:   []rtconfig.PortableDiskConfig{{Name: "cache"}, {Name: "empty", Slot: &rtconfig.DiskSlotConfig{}}},
				},
				Resources: rtconfig.PortableResourcesConfig{
					Capacity: rtconfig.CapacityConfig{CPU: 1, Memory: "128MiB"}, Allocatable: rtconfig.AllocatableConfig{CPU: 1, Memory: "128MiB"},
				},
				Launch:   rtconfig.PortableLaunchConfig{Exec: "/bin/true", Workdir: "/", Restart: "never"},
				Mounts:   []rtconfig.MountConfig{{Type: "disk", Source: "cache", Target: "/cache"}, {Type: "disk", Source: "empty", Target: "/empty"}},
				Metadata: map[string]string{workspace.Namespace: `[{"name":"empty","mounts":[{"path":"/empty"}]}]`},
			}
			originalView := cfg.Metadata[workspace.Namespace]
			parentDir := t.TempDir()
			var sink snapshot.ArtifactSink = snapshot.NewFileSink(parentDir, "template", nil, false, nil)
			var source sparse.Source
			var input, cfgPath string
			var err error
			if carrier == "ordinary-bundle" {
				key := [32]byte{0x41, 0x42, 0x43}
				m := &manifest.Config{Manifest: manifest.ManifestSubConfig{Key: hex.EncodeToString(key[:])}, Chunker: chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4KiB"}}, Crypto: manifestcrypto.Config{Chunk: "aes", Manifest: "aes"}}
				b, err := snapshot.NewBundleSink(ctx, parentDir, "packed", m, func() ([32]byte, error) { return key, nil }, nil)
				if err != nil {
					t.Fatal(err)
				}
				sink = b
				t.Cleanup(func() { b.Close() })
				cfgPath = filepath.Join(output, "manifest.yaml")
				body, err := yaml.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(cfgPath, body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if strings.HasPrefix(carrier, "checkpoint") {
				cpCfg, e := cfg.Checkpointed("")
				if e != nil {
					t.Fatal(e)
				}
				cp := &sandboxfile.Checkpoint{Version: 2, CheckpointID: "selected", Root: sandboxfile.CheckpointDisk{DiffID: "root"}, Disks: []sandboxfile.CheckpointNamedDisk{{Name: "cache", DiffID: "data"}, {Name: "empty"}}}
				source, err = sandboxfile.BuildCheckpointSource(ctx, cpCfg, cp)
				if err != nil {
					t.Fatal(err)
				}
				if carrier == "checkpoint-bundle" {
					parentDir, input, _, cfgPath = writeTaskCheckpointArtifacts(t, "bundle", source)
				} else {
					_, input, err = sink.AbsorbSandbox(ctx, source)
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := os.MkdirAll(filepath.Join(parentDir, "diffs"), 0700); err != nil {
					t.Fatal(err)
				}
				for i, b := range [][]byte{payload, data} {
					id := []string{"root", "data"}[i]
					base := cpCfg.Boot.Root
					if i > 0 {
						base = cpCfg.Boot.Disks[i-1].PortableRootConfig
					}
					store, e := diffstore.Create(filepath.Join(parentDir, "diffs", id+".diff"), diffstore.Options{DiffID: id, BaseID: artifact.DiffCheckpointBaseID(base), LogicalSize: int64(len(b))})
					if e != nil {
						t.Fatal(e)
					}
					if _, e = store.WriteAt(b, 0); e != nil {
						t.Fatal(e)
					}
					if e = store.Checkpoint(ctx, "selected"); e != nil {
						t.Fatal(e)
					}
					if _, e = store.WriteAt(bytes.Repeat([]byte{0xFF}, len(b)), 0); e != nil {
						t.Fatal(e)
					}
					if e = store.Close(); e != nil {
						t.Fatal(e)
					}
				}
			} else {
				cfg.Boot.Disks[0].Base, _, err = sink.AbsorbOverlay(ctx, bytes.NewReader(data), nil)
				if err != nil {
					t.Fatal(err)
				}
				raw, e := rtconfig.MarshalPortableSandboxConfig(cfg)
				if e != nil {
					t.Fatal(e)
				}
				source, e = sandboxfile.BuildSource(sparse.Dense(bytes.NewReader(payload), uint64(len(payload))), nil, raw)
				if e != nil {
					t.Fatal(e)
				}
				ref, path, e := sink.AbsorbSandbox(ctx, source)
				if e != nil {
					t.Fatal(e)
				}
				input = path
				if carrier == "ordinary-bundle" {
					if err = sink.CommitSandbox(ctx, ref, path); err != nil {
						t.Fatal(err)
					}
					key, e := manifest.ParseKeyRef(ref)
					if e != nil {
						t.Fatal(e)
					}
					input = filepath.Join(parentDir, manifest.HexKey(key)+".bundle")
				}
			}
			seedPath := filepath.Join(output, "seed")
			seed := bytes.Repeat([]byte{0x47}, 16384)
			if err := os.WriteFile(seedPath, seed, 0600); err != nil {
				t.Fatal(err)
			}
			st := rtconfig.DiskV1{Kind: "disk", Version: 1, DiffTemplate: "file://" + seedPath}
			req := configsock.ArtifactPrepareSpec{RootSourceKind: "sandbox", LaunchMode: "cold", RootRef: input, RelativeDir: output, ManifestConfig: cfgPath, SBXCreateWorkspaces: []workspace.Prepared{{Name: "empty", Disk: &st}, {Name: "project", Disk: &st, Mounts: []workspace.Mount{{Path: "/project"}}}, {Name: "future", Mounts: []workspace.Mount{{Path: "/future"}}}}, SBXCreateMounts: []rtconfig.MountConfig{{Type: "tmpfs", Target: "/scratch"}}}
			prepared, e := Prepare(ctx, req)
			if e != nil {
				t.Fatal(e)
			}
			if !types.IsPreparedSandboxRef(prepared.Summary.DerivedSandboxRef) || prepared.Summary.RootSource.Ref != input {
				t.Fatalf("original and derived E: %+v", prepared.Summary)
			}
			parentCfg, e := Prepare(ctx, configsock.ArtifactPrepareSpec{RootSourceKind: "sandbox", LaunchMode: "cold", RootRef: input, RelativeDir: output, ManifestConfig: cfgPath})
			if e != nil || len(parentCfg.SourceSandboxConfig.Boot.Disks) != 2 || parentCfg.SourceSandboxConfig.Metadata[workspace.Namespace] != originalView {
				t.Fatalf("modified source template: %v", e)
			}
			if e = os.Rename(seedPath, seedPath+".offline"); e != nil {
				t.Fatal(e)
			}
			if e = os.Rename(parentDir, parentDir+".offline"); e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { os.RemoveAll(parentDir + ".offline") })
			resume, e := Prepare(ctx, configsock.ArtifactPrepareSpec{RootSourceKind: "sandbox", LaunchMode: "cold", RootRef: prepared.Summary.DerivedSandboxRef, RelativeDir: output, ManifestConfig: cfgPath})
			if e != nil {
				t.Fatal(e)
			}
			child := resume.SourceSandboxConfig
			var ordered []string
			for _, d := range child.Boot.Disks {
				ordered = append(ordered, d.Name)
			}
			if !reflect.DeepEqual(ordered, []string{"cache", "empty", "project", "future"}) || !child.Boot.Disks[1].Unbound() || !child.Boot.Disks[3].Unbound() || !child.Boot.Disks[2].PureBlock || child.Boot.Disks[2].Slot != nil || child.Mounts[1].Source != "cache" {
				t.Fatalf("appended graph %#v %+v", ordered, child)
			}
			var storageCfg *rtconfig.ManifestConfig
			if cfgPath != "" {
				storageCfg, e = rtconfig.LoadManifestConfig(cfgPath)
				if e != nil {
					t.Fatal(e)
				}
			}
			store, e := artifact.NewProcessStorage(storageCfg)
			if e != nil {
				t.Fatal(e)
			}
			defer store.Close()
			stream, e := openArtifactStream(ctx, resume.PreparedSource.Ref, output, store, nil)
			if e != nil {
				t.Fatal(e)
			}
			childRoot, e := sandboxfile.Open(ctx, stream)
			if e != nil {
				t.Fatal(e)
			}
			defer childRoot.Close()
			got := make([]byte, len(payload))
			if _, e = childRoot.Payload.ReadAt(ctx, got, 0); e != nil || !bytes.Equal(payload, got) {
				t.Fatalf("wrong saved root version: %v", e)
			}
			if carrier == "ordinary-bundle" {
				checkAppendedSavedArtifacts(t, prepared.Summary.DerivedSandboxRef, child, store, output, cfgPath, seed)
			}
			for _, v := range []struct {
				i    int
				want []byte
			}{{0, data}, {2, seed}} {
				def := child.Boot.Disks[v.i]
				fix := func(raw string) string {
					r, e := manifest.ParseRef(raw)
					if e != nil {
						t.Fatal(e)
					}
					if r.Scheme == manifest.RefSchemeFile && r.Location == "" && !filepath.IsAbs(r.Path) {
						r.Path = filepath.Join(output, r.Path)
					}
					return r.String()
				}
				d := rtconfig.DiskV1{Kind: "disk", Version: 1, Base: fix(def.Base)}
				for _, raw := range def.BaseFromRefs {
					d.BaseFromRefs = append(d.BaseFromRefs, fix(raw))
				}
				disk, n, _, e := sandbox.OpenDiskV1Source(ctx, d, store, nil)
				if e != nil {
					t.Fatal(e)
				}
				got := make([]byte, n)
				_, e = disk.ReadAt(got, 0)
				disk.Close()
				if e != nil || !bytes.Equal(v.want, got) {
					t.Fatalf("%s wrong data: %v", def.Name, e)
				}
			}
		})
	}
}

// A later capture writes child E's actual graph with one version per bound
// writable disk. Reopen its S/E pair, replace it using offline Reset, then
// hand the named disk to an independent consumer through ExportDisk.
func checkAppendedSavedArtifacts(t *testing.T, childRef string, child *rtconfig.PortableSandboxConfig, storage *artifact.ProcessStorage, dir, manifestConfig string, seed []byte) {
	t.Helper()
	ctx := context.Background()
	baseline, err := child.Checkpointed(childRef)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Join(dir, "diffs"), 0700); err != nil {
		t.Fatal(err)
	}
	var retained []artifact.CurrentDisk
	cp := &sandboxfile.Checkpoint{Version: 2, CheckpointID: "paused"}
	var project *diffstore.Store
	for i, name := range []string{"root", "cache", "empty", "project", "future"} {
		declaration := baseline.Boot.Root
		record := artifact.CurrentDisk{}
		if i > 0 {
			d := baseline.Boot.Disks[i-1]
			declaration = d.PortableRootConfig
			record.Name = name
			if d.Unbound() {
				retained = append(retained, record)
				cp.Disks = append(cp.Disks, sandboxfile.CheckpointNamedDisk{Name: name})
				continue
			}
		}
		record.DiffID, record.BaseID, record.Capacity = name+"-private", artifact.DiffCheckpointBaseID(declaration), []int64{8192, 4096, 0, int64(len(seed)), 0}[i]
		writer, e := diffstore.Create(filepath.Join(dir, "diffs", record.DiffID+".diff"), diffstore.Options{DiffID: record.DiffID, BaseID: record.BaseID, LogicalSize: record.Capacity})
		if e != nil {
			t.Fatal(e)
		}
		if name == "project" {
			project = writer
			if _, e = writer.WriteAt(bytes.Repeat([]byte{'P'}, 4096), 4096); e != nil {
				t.Fatal(e)
			}
		}
		if e = writer.Checkpoint(ctx, cp.CheckpointID); e != nil {
			t.Fatal(e)
		}
		if name != "project" {
			if e = writer.Close(); e != nil {
				t.Fatal(e)
			}
		}
		if i == 0 {
			cp.Root = sandboxfile.CheckpointDisk{DiffID: record.DiffID}
		} else {
			cp.Disks = append(cp.Disks, sandboxfile.CheckpointNamedDisk{Name: name, DiffID: record.DiffID})
		}
		retained = append(retained, record)
	}
	owner, e := diffstore.LockDirectory(dir)
	if e != nil {
		t.Fatal(e)
	}
	run := artifact.CurrentRun{SandboxID: "child", InstanceID: "child-instance", RunID: "current"}
	if e = storage.RetainCurrentDisks(ctx, owner, run, baseline, retained, nil); e != nil {
		t.Fatal(e)
	}
	if e = owner.Close(); e != nil {
		t.Fatal(e)
	}
	sink := snapshot.NewFileSink(dir, "save", storage.LocalCodec(), storage.LocalRequired(), nil)
	source, e := sandboxfile.BuildCheckpointSource(ctx, baseline, cp)
	if e != nil {
		t.Fatal(e)
	}
	paused, _, e := sink.AbsorbSandbox(ctx, source)
	if e != nil {
		t.Fatal(e)
	}
	snap, _, e := sink.AbsorbSnapshot(ctx, taskMemorySource(t, paused, nil))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = project.WriteAt(bytes.Repeat([]byte{'R'}, 4096), 4096); e != nil {
		t.Fatal(e)
	}
	if e = project.Close(); e != nil {
		t.Fatal(e)
	}
	reset, e := storage.ResetCurrentDisks(ctx, dir, run, "reset-child")
	if e != nil {
		t.Fatal(e)
	}
	for _, c := range []struct {
		ref, kind, mode string
		marker          byte
	}{{snap, "snapshot", "memory", 'P'}, {reset, "sandbox", "cold", 'R'}} {
		got, e := Prepare(ctx, configsock.ArtifactPrepareSpec{RootSourceKind: c.kind, LaunchMode: c.mode, RootRef: c.ref, RelativeDir: dir, ManifestConfig: manifestConfig})
		if e != nil {
			t.Fatal(e)
		}
		if len(got.Summary.DiskTopology.Disks) != 4 || got.Summary.DiskTopology.Disks[2].Name != "project" || !got.SourceSandboxConfig.Boot.Disks[3].Unbound() {
			t.Fatalf("lost appended devices from %s", c.mode)
		}
		if c.kind == "snapshot" {
			if got.Summary.RootSource.SandboxRef != paused {
				t.Fatalf("incorrect paused E: %s", got.Summary.RootSource.SandboxRef)
			}
		}
		d, e := storage.ExportDisk(ctx, artifact.ExportDiskRequest{SourceRef: c.ref, SourceDir: dir, DiskName: "project", OutputDir: t.TempDir()})
		if e != nil {
			t.Fatal(e)
		}
		disk, n, _, e := sandbox.OpenDiskV1Source(ctx, d, storage, nil)
		if e != nil {
			t.Fatal(e)
		}
		out := make([]byte, n)
		_, e = disk.ReadAt(out, 0)
		disk.Close()
		want := bytes.Clone(seed)
		copy(want[4096:8192], bytes.Repeat([]byte{c.marker}, 4096))
		if e != nil || !bytes.Equal(out, want) {
			t.Fatalf("wrong exported child bytes from %s: %v", c.mode, e)
		}
	}
}
