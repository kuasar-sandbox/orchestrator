package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"io"
	"strings"
	"testing"
)

type diagnosticRAM map[uint64]byte

func (r diagnosticRAM) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, io.EOF
	}
	for i := range b {
		b[i] = r[uint64(off)+uint64(i)]
	}
	return len(b), nil
}
func (r diagnosticRAM) entry(at, value uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], value)
	for i, v := range b {
		r[at+uint64(i)] = v
	}
}
func tableFixture(t *testing.T, levels, leafLevel int) (diagnosticRAM, sparse.Source, diagnosticCPU, uint64) {
	t.Helper()
	r := diagnosticRAM{}
	const va = uint64(0x7fd0baa00010)
	cpu := diagnosticCPU{CR3: 0x1045}
	if levels == 5 {
		cpu.CR4 = 1 << 12
	}
	table := uint64(0x1000)
	for level := levels; level >= leafLevel; level-- {
		at := table + ((va>>uint(12+9*(level-1)))&511)*8
		if level == leafLevel {
			base := uint64(0x8000)
			flags := uint64(7)
			if level == 2 {
				base = 0x200000
				flags |= 1 << 7
			}
			if level == 3 {
				base = 0
				flags |= 1 << 7
			}
			r.entry(at, base|flags)
			size := uint64(1) << uint(12+9*(level-1))
			data := base | (va & (size - 1))
			for i := uint64(0); i < 16; i++ {
				r[data+i] = 255
			}
		} else {
			r.entry(at, (table+0x1000)|7)
			table += 0x1000
		}
	}
	source, e := sparse.NewSource(r, 1<<30, nil)
	if e != nil {
		t.Fatal(e)
	}
	return r, source, cpu, va
}
func TestSnapshotPageTableWalk(t *testing.T) {
	for _, v := range [][2]int{{4, 1}, {5, 1}, {4, 2}, {4, 3}} {
		r, source, cpu, va := tableFixture(t, v[0], v[1])
		w, e := walkSnapshotTable(context.Background(), source, cpu, va)
		if e != nil || w.Status != "saved_mapping_resolved" || w.Data.FFBytes != 16 || len(w.Entries) != v[0]-v[1]+1 {
			t.Fatalf("levels%v report=%+v err=%v", v, w, e)
		}
		at := w.Entries[len(w.Entries)-1].Address
		r.entry(at, 0)
		w, e = walkSnapshotTable(context.Background(), source, cpu, va)
		if e != nil || w.Status != "saved_entry_not_present" || w.Data != nil {
			t.Fatalf("missing leaf: %+v %v", w, e)
		}
	}
}
func TestSnapshotPageTableRejectBadGeometry(t *testing.T) {
	r, s, cpu, va := tableFixture(t, 4, 1)
	if _, e := walkSnapshotTable(context.Background(), s, cpu, 1<<47); e == nil {
		t.Fatal("accepted non-user VA")
	}
	r.entry(0x1000+((va>>39)&511)*8, (2<<30)|7)
	if _, e := walkSnapshotTable(context.Background(), s, cpu, va); e == nil {
		t.Fatal("accepted out-of-RAM table")
	}
	r.entry(0x1000+((va>>39)&511)*8, 0x1087)
	if _, e := walkSnapshotTable(context.Background(), s, cpu, va); e == nil {
		t.Fatal("accepted PML4 large page")
	}
}
func TestSnapshotPagingRootsPreserveIntegerIdentity(t *testing.T) {
	nested := `{"sregs":{"cr3":4503599627366407,"cr4":4096}}`
	raw, e := json.Marshal(map[string]any{"snapshot_data": map[string]any{"state": nested}})
	if e != nil {
		t.Fatal(e)
	}
	roots, e := diagnosticCPUs(raw)
	if e != nil || len(roots) != 1 || roots[0].CR3 != 4503599627366407 || roots[0].CR4 != 4096 {
		t.Fatalf("roots=%v err=%v", roots, e)
	}
	for _, bad := range []string{`{}`, `{"cr3":1}`, `{"cr3":-1,"cr4":0}`, strings.Repeat("x", (8<<20)+1)} {
		if _, e := diagnosticCPUs([]byte(bad)); e == nil {
			t.Fatal("accepted invalid state")
		}
	}
}
