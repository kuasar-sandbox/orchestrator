package conductorapp

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

func TestTemplateDiskMBUsesLogicalCapacityWithoutTenantCapability(t *testing.T) {
	const size = 16 << 20
	if got := templateDiskMB("mkfs://ext4?size=16MiB", ""); got != 16 {
		t.Fatalf("generated disk capacity = %d", got)
	}
	path := filepath.Join(t.TempDir(), "raw.ext4")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []string{path, "file://" + path} {
		if got := templateDiskMB(ref, ""); got != 16 {
			t.Fatalf("raw disk capacity = %d", got)
		}
	}
	parent := "file://" + t.TempDir()
	location, err := reflocation.Resolve(parent, "seeds")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(location.Path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(path, filepath.Join(location.Path, "raw.ext4")); err != nil {
		t.Fatal(err)
	}
	if got := templateDiskMB("file://raw.ext4@location:seeds", parent); got != 16 {
		t.Fatalf("named raw disk capacity = %d", got)
	}
	source, err := sparse.NewSource(bytes.NewReader(nil), size, []sparse.Extent{{Size: size}})
	if err != nil {
		t.Fatal(err)
	}
	key := [32]byte{0x42}
	codec, err := manifestcrypto.NewTarStreamCodec(key)
	if err != nil {
		t.Fatal(err)
	}
	// Even a valid ambient tenant key is not a conductor API capability.
	t.Setenv(manifest.CustomerKeyEnv, hex.EncodeToString(key[:]))
	for _, encrypted := range []bool{false, true} {
		var selected tarstream.Codec
		if encrypted {
			selected = codec
		}
		sink := snapshot.NewFileSink(t.TempDir(), "capacity", selected, encrypted, nil)
		raw, carrier, err := sink.AbsorbOverlaySource(t.Context(), source)
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(carrier)
		if err != nil || info.Size() >= 1<<20 {
			t.Fatalf("fixture is not sparse: %v %v", info, err)
		}
		ref, err := manifest.ParseRef(raw)
		if err != nil {
			t.Fatal(err)
		}
		ref.Path = carrier
		want := 16
		if encrypted {
			want = 0
		}
		if got := templateDiskMB(ref.String(), ""); got != want {
			t.Fatalf("encrypted=%t logical capacity=%d, want %d (carrier=%d)", encrypted, got, want, info.Size())
		}
	}
}
