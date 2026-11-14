package workspacedisk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

func TestWorkspaceSavedDiskHandoff(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	key := strings.Repeat("a", 64)
	source := filepath.Join(dir, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "manifest.yaml")
	if err := os.WriteFile(configPath, []byte("chunker: {mode: fixed, fixed: {size: 4KiB}}\ncrypto: {chunk: aes, manifest: aes}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	sink := snapshot.NewFileSink(source, "owner", nil, false, nil)
	defer sink.Close()
	data := bytes.Repeat([]byte{0x31}, 8192)
	base, _, err := sink.AbsorbOverlay(ctx, bytes.NewReader(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	mounts := []rtconfig.MountConfig{{Type: "disk", Source: "work", Target: "/data", Subpath: "project"}, {Type: "disk", Source: "work", Target: "/output", Subpath: "out"}, {Type: "disk", Source: "reserved", Target: "/reserved"}}
	declarations := `[{"name":"work","mounts":[{"path":"/data","subpath":"project"},{"path":"/output","subpath":"out"}]},{"name":"reserved","mounts":[{"path":"/reserved"}]}]`
	cfg := rtconfig.PortableSandboxConfig{
		Version: 2, Resources: rtconfig.PortableResourcesConfig{Capacity: rtconfig.CapacityConfig{CPU: 1, Memory: "1GiB"}, Allocatable: rtconfig.AllocatableConfig{CPU: 1, Memory: "1GiB"}},
		Boot:   rtconfig.PortableBootConfig{Kernel: "file://kernel@digest:" + key, Runtime: "file://runtime@digest:" + key, Root: rtconfig.PortableRootConfig{Base: "self"}, Disks: []rtconfig.PortableDiskConfig{{Name: "work", PureBlock: true, PortableRootConfig: rtconfig.PortableRootConfig{Base: base}}, {Name: "reserved", Slot: &rtconfig.DiskSlotConfig{}}}},
		Launch: rtconfig.PortableLaunchConfig{Workdir: "/", Restart: "never"}, Mounts: mounts, Metadata: map[string]string{workspace.Namespace: declarations},
	}
	raw, err := rtconfig.MarshalPortableSandboxConfig(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	e, err := sandboxfile.BuildSource(sparse.Dense(bytes.NewReader(data), uint64(len(data))), nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	ref, path, err := sink.AbsorbSandbox(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	if err = sink.CommitSandbox(ctx, ref, path); err != nil {
		t.Fatal(err)
	}
	req := Request{Operation: "list", Source: ref, SourceDir: source, ManifestConfig: configPath, RefLocationParent: "file://" + filepath.Join(dir, "node-a"), Publication: "sandbox-a"}
	list, err := execute(ctx, req, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.States) != 2 || list.States[0].Name != "work" || list.States[0].SizeBytes == nil || *list.States[0].SizeBytes != 8192 || list.States[1].State != "unbound" || list.States[1].SizeBytes != nil {
		t.Fatal(list)
	}
	req.Operation = "export"
	req.Name = "reserved"
	if _, err := execute(ctx, req, key); !errors.Is(err, artifact.ErrDiskUnbound) {
		t.Fatal(err)
	}
	req.Name = "ordinary"
	if _, err := execute(ctx, req, key); !errors.Is(err, artifact.ErrDiskNotFound) {
		t.Fatal(err)
	}
	req.Name = "work"
	result, err := execute(ctx, req, key)
	if err != nil {
		t.Fatal(err)
	}
	d := *result.Disk
	if strings.Contains(d.Base, dir) || !strings.HasSuffix(d.Base, "@location:sandbox-a") {
		t.Fatal(d)
	}
	m := migrationtoken.KeyMaterial{APISecret: strings.Repeat("b", 64), ManifestKey: key}
	token, err := migrationtoken.SealWorkspace(m, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(source); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "node-a"), filepath.Join(dir, "node-b")); err != nil {
		t.Fatal(err)
	}
	opt := Options{APISecret: m.APISecret, ManifestKey: key, ManifestConfig: configPath, RefLocationParent: "file://" + filepath.Join(dir, "node-b")}
	in, _ := json.Marshal(map[string]string{"migrationToken": token})
	got, loc, err := Resolve(ctx, in, opt)
	if err != nil || got.Base != d.Base {
		t.Fatal(got, err)
	}
	inline, _ := json.Marshal(d)
	if _, _, err := Resolve(ctx, inline, opt); err != nil {
		t.Fatal(err)
	}
	invalid := opt
	invalid.ManifestKey = strings.Repeat("c", 64)
	if _, _, err := Resolve(ctx, inline, invalid); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("another tenant accessed bundle", err)
	}
	for i := 0; i < 2; i++ {
		st, err := Storage(configPath, key)
		if err != nil {
			t.Fatal(err)
		}
		r, err := manifest.ParseRef(got.Base)
		if err != nil {
			t.Fatal(err)
		}
		layer, err := OpenLayer(ctx, st, r, "", loc)
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, len(data))
		n, err := layer.ReadAt(ctx, buf, 0)
		if n != len(data) || !bytes.Equal(buf, data) {
			t.Fatal(n, err)
		}
		if err := errors.Join(layer.Close(), st.Close()); err != nil {
			t.Fatal(err)
		}
	}
}
