package workspacedisk

import (
	"bytes"
	"context"
	"os"
	"slices"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

func TestFreezeSeedAtNativeLayerLimit(t *testing.T) {
	const (
		block = 4096
		// The current base is in addition to the parent list.
		layers      = 1 + rtconfig.MaxPortableLayerRefs
		overlap     = layers
		zeroed      = layers + 1
		transparent = layers + 2
		size        = (transparent + 1) * block
	)
	ctx := context.Background()
	storage, err := artifact.NewProcessStorage(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	dir := t.TempDir()
	sink := snapshot.NewFileSink(dir, "lower", nil, false, nil)
	defer sink.Close()
	refs := make([]string, layers)
	want := make([]byte, size)
	fill := func(data []byte, page int, value byte) {
		copy(data[page*block:], bytes.Repeat([]byte{value}, block))
	}
	// Every layer owns one distinguishable block. The oldest two also write
	// the same block so a reversed merge changes the resulting bytes.
	for i := layers - 1; i >= 0; i-- {
		data := make([]byte, size)
		var holes []sparse.Extent
		for page := 0; page <= transparent; page++ {
			owned := page == i || (page == overlap && i >= layers-2) || (page == zeroed && i == layers-1)
			if !owned {
				holes = append(holes, sparse.Extent{Offset: uint64(page * block), Size: block})
				continue
			}
			fill(data, page, byte(i+1))
			fill(want, page, byte(i+1))
		}
		raw, path, err := sink.AbsorbOverlay(ctx, bytes.NewReader(data), holes)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := manifest.ParseRef(raw)
		if err != nil {
			t.Fatal(err)
		}
		ref.Path = path
		refs[i] = ref.String()
	}

	// A raw seed owns its allocated blocks, even when all bytes are zero.
	// All remaining pages must read through to the original parent graph.
	seed, err := os.CreateTemp(dir, "template-")
	if err != nil {
		t.Fatal(err)
	}
	if err = seed.Truncate(size); err != nil {
		t.Fatal(err)
	}
	for page, value := range map[int]byte{0: 0xf3, zeroed: 0} {
		data := bytes.Repeat([]byte{value}, block)
		if _, err = seed.WriteAt(data, int64(page*block)); err != nil {
			t.Fatal(err)
		}
		fill(want, page, value)
	}
	if err = seed.Close(); err != nil {
		t.Fatal(err)
	}
	original := rtconfig.DiskV1{Kind: "disk", Version: 1, Base: refs[0], BaseFromRefs: slices.Clone(refs[1:]), DiffTemplate: "file://" + seed.Name()}
	frozen, err := FreezeSeed(ctx, original, storage, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = frozen.Validate(); err != nil {
		t.Fatal(err)
	}
	if frozen.Size == nil || *frozen.Size != size || frozen.DiffTemplate != "" || len(frozen.BaseFromRefs) != rtconfig.MaxPortableLayerRefs {
		t.Fatalf("frozen size=%v, template=%q, parent count=%d", frozen.Size, frozen.DiffTemplate, len(frozen.BaseFromRefs))
	}
	last := len(frozen.BaseFromRefs) - 1
	if !slices.Equal(frozen.BaseFromRefs[:last], refs[:layers-2]) || !slices.Equal(original.BaseFromRefs, refs[1:]) {
		t.Fatal("freezing a seed reordered or mutated the retained parents")
	}
	// Remove everything that the seed and merged parent replace. Reopen
	// solely from the returned native descriptor after the originals are gone.
	if err = os.Remove(seed.Name()); err != nil {
		t.Fatal(err)
	}
	for _, raw := range refs[layers-2:] {
		ref, err := manifest.ParseRef(raw)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.Remove(ref.Path); err != nil {
			t.Fatal(err)
		}
	}
	// The replacement is sparse too. A zero-filled result at a hole alone
	// cannot establish that the merged layer still falls through there.
	for _, raw := range []string{frozen.Base, frozen.BaseFromRefs[last]} {
		ref, err := manifest.ParseRef(raw)
		if err != nil {
			t.Fatal(err)
		}
		source, err := OpenLayer(ctx, storage, ref, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		run, err := source.RunAt(transparent*block, block)
		if err != nil {
			t.Fatal(err)
		}
		if run.Kind() != sparse.Hole {
			t.Fatalf("%s final page is %v, want Hole", ref.Path, run.Kind())
		}
		if err = source.Close(); err != nil {
			t.Fatal(err)
		}
	}
	view, length, template, err := sandbox.OpenDiskV1Source(ctx, frozen, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	if length != size || template != "" {
		t.Fatalf("reopened size=%d template=%q", length, template)
	}
	got := make([]byte, size)
	if n, err := view.ReadAt(got, 0); n != len(got) || err != nil {
		t.Fatalf("read %d of %d: %v", n, len(got), err)
	}
	for page := 0; page <= transparent; page++ {
		start, end := page*block, (page+1)*block
		if !bytes.Equal(got[start:end], want[start:end]) {
			t.Fatalf("page %d first byte %#x, want %#x", page, got[start], want[start])
		}
	}
}
