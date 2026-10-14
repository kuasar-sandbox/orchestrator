package main

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

const fixtureDirect = uint64(0xffff888000000000)
const fixtureTask = fixtureDirect + 0x10000

func primaryFixture(t *testing.T) (diagnosticRAM, sparse.Source, uint64) {
	t.Helper()
	r, s, _, va := tableFixture(t, 4, 1)
	put := func(at uint64, b []byte) {
		for i, v := range b {
			r[at+uint64(i)] = v
		}
	}
	for _, sig := range primarySignatures {
		b, _ := hex.DecodeString(sig.code)
		put(sig.va-primaryImage, b)
	}
	r.entry(0x1b914c0, fixtureDirect)
	put(primaryInit-primaryImage+0x640, []byte("swapper/0"))
	head := primaryInit + 0x368
	r.entry(head-primaryImage, fixtureTask+0x368)
	r.entry(head-primaryImage+8, fixtureTask+0x368)
	r.entry(0x10368, head)
	r.entry(0x10370, head)
	put(0x10640, []byte("python3"))
	r.entry(0x10448, 42)
	r.entry(0x104b0, fixtureDirect+0x20000)
	r.entry(0x20004, 1)
	r.entry(0x20060, 42)
	r.entry(0x20068, fixtureDirect+0x30000)
	r.entry(0x20070, 1)
	r.entry(0x20078, fixtureDirect+0x31000)
	r.entry(0x103b8, fixtureDirect+0x40000)
	r.entry(0x40038, fixtureDirect+0x1000)
	return r, s, va
}
func TestSnapshotPrimaryCapturedMM(t *testing.T) {
	r, s, va := primaryFixture(t)
	got := primarySnapshot(context.Background(), s, "python3", 1, primaryRelease, va)
	if got.Status != "observed" || got.PID != 42 || got.NamespacePID != 1 || got.PIDLevel != 1 || got.PGDPhysical != 0x1000 || got.Walk == nil || got.Walk.Data == nil || got.Walk.Data.FFBytes != 16 || got.Walk.Data.Nonzero != 16 || len(got.Walk.Data.Digest) != 64 || len(got.Walk.Entries) != 4 {
		t.Fatalf("%+v", got)
	}
	// An unrelated saved CPU root has no mapping; selection uses task.mm only.
	cpuWalk, e := walkSnapshotTable(context.Background(), s, diagnosticCPU{CR3: 0x9000}, va)
	if e != nil || cpuWalk.Status != "saved_entry_not_present" {
		t.Fatal(cpuWalk, e)
	}
	r.entry(got.Walk.Entries[3].Address, 0)
	got = primarySnapshot(context.Background(), s, "python3", 1, primaryRelease, va)
	if got.Status != "observed" || got.Walk.Status != "saved_entry_not_present" || got.Walk.Data != nil || len(got.Walk.Entries) != 4 {
		t.Fatalf("missing captured PTE: %+v", got)
	}
}
func TestSnapshotPrimaryRejectsCorruption(t *testing.T) {
	cases := map[string]func(diagnosticRAM){
		"signature":            func(r diagnosticRAM) { r[primarySignatures[0].va-primaryImage] = 0 },
		"l5":                   func(r diagnosticRAM) { r.entry(0x1b914d0, 1) },
		"direct-map":           func(r diagnosticRAM) { r.entry(0x1b914c0, 0xffff000000000000) },
		"unaligned-direct-map": func(r diagnosticRAM) { r.entry(0x1b914c0, fixtureDirect+1) },
		"init-mm":              func(r diagnosticRAM) { r.entry(primaryInit-primaryImage+0x3b8, fixtureDirect) },
		"init-comm":            func(r diagnosticRAM) { r[primaryInit-primaryImage+0x640] = 'x' },
		"cycle":                func(r diagnosticRAM) { r.entry(0x10368, fixtureTask+0x368) },
		"prev":                 func(r diagnosticRAM) { r.entry(0x10370, 0) },
		"noncanonical":         func(r diagnosticRAM) { r.entry(0x103b8, 0x800000000000) },
		"overflow":             func(r diagnosticRAM) { r.entry(0x103b8, ^uint64(0)-8) },
		"outside-ram":          func(r diagnosticRAM) { r.entry(0x103b8, fixtureDirect+(1<<30)-16) },
		"null-mm":              func(r diagnosticRAM) { r.entry(0x103b8, 0) },
		"unaligned-pgd":        func(r diagnosticRAM) { r.entry(0x40038, fixtureDirect+0x1001) },
		"pid-level":            func(r diagnosticRAM) { r.entry(0x20004, 33) },
		"outer-pid":            func(r diagnosticRAM) { r.entry(0x20060, 43) },
		"negative-pid":         func(r diagnosticRAM) { r.entry(0x20070, 0xffffffff) },
		"namespace-pointer":    func(r diagnosticRAM) { r.entry(0x20078, 1) },
		"duplicate-namespace":  func(r diagnosticRAM) { r.entry(0x20078, fixtureDirect+0x30000) },
		"absent":               func(r diagnosticRAM) { r.entry(0x20070, 2) },
		"duplicate": func(r diagnosticRAM) {
			for off := uint64(0); off < 0x650; off++ {
				r[0x50000+off] = r[0x10000+off]
			}
			head := primaryInit + 0x368
			second := fixtureDirect + 0x50368
			r.entry(0x10368, second)
			r.entry(0x50370, fixtureTask+0x368)
			r.entry(0x50368, head)
			r.entry(head-primaryImage+8, second)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r, s, va := primaryFixture(t)
			mutate(r)
			got := primarySnapshot(context.Background(), s, "python3", 1, primaryRelease, va)
			if got.Status != "diagnostic-unavailable" || got.Walk != nil {
				t.Fatalf("%+v", got)
			}
		})
	}
}
func TestSnapshotPrimaryIdentityAndBounds(t *testing.T) {
	_, s, va := primaryFixture(t)
	for _, v := range []struct {
		comm    string
		pid     uint64
		release string
	}{{"", 1, primaryRelease}, {"python3", 0, primaryRelease}, {"python3", 1, "other"}, {"python3", 1, ""}, {"abcdefghijklmnop", 1, primaryRelease}} {
		if got := primarySnapshot(context.Background(), s, v.comm, v.pid, v.release, va); got.Status != "diagnostic-unavailable" {
			t.Fatal(got)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := primarySnapshot(ctx, s, "python3", 1, primaryRelease, va); got.Status != "diagnostic-unavailable" {
		t.Fatal(got)
	}
	m := &primaryMemory{Source: s, ctx: context.Background(), remaining: 8, direct: fixtureDirect}
	if _, e := m.RunAt(0, 9); e == nil {
		t.Fatal("byte budget ignored")
	}
	for _, at := range []uint64{0, 0x800000000000, fixtureDirect + (1 << 30), ^uint64(0) - 7} {
		if _, e := m.physical(at, 16); e == nil {
			t.Fatalf("accepted %x", at)
		}
	}
	small, _ := sparse.NewSource(diagnosticRAM{}, 1<<29, nil)
	if got := primarySnapshot(context.Background(), small, "python3", 1, primaryRelease, va); got.Status != "diagnostic-unavailable" {
		t.Fatal(got)
	}
}

type primaryBadSource struct {
	sparse.Source
	kind  sparse.RunKind
	split bool
}
type primaryBadRun struct {
	sparse.Run
	kind  sparse.RunKind
	split bool
}

func (s primaryBadSource) RunAt(at, n uint64) (sparse.Run, error) {
	r, e := s.Source.RunAt(at, n)
	return primaryBadRun{r, s.kind, s.split}, e
}
func (r primaryBadRun) Kind() sparse.RunKind { return r.kind }
func (r primaryBadRun) End() uint64 {
	if r.split {
		return r.Run.End() - 1
	}
	return r.Run.End()
}
func TestSnapshotPrimaryUnknownMetadata(t *testing.T) {
	_, s, va := primaryFixture(t)
	for _, bad := range []primaryBadSource{{s, sparse.Hole, false}, {s, sparse.RunKind(99), false}, {s, sparse.Data, true}} {
		if got := primarySnapshot(context.Background(), bad, "python3", 1, primaryRelease, va); got.Status != "diagnostic-unavailable" {
			t.Fatal(got)
		}
	}
}
func TestSnapshotPrimaryTaskBound(t *testing.T) {
	r, s, va := primaryFixture(t)
	head := primaryInit + 0x368
	// All nodes are valid but the unique match cannot be declared before termination.
	previous := fixtureTask + 0x368
	for i := uint64(0); i < 4096; i++ {
		task := fixtureDirect + 0x4000000 + i*0x1000
		node := task + 0x368
		for off := uint64(0); off < 0x650; off++ {
			if value := r[0x10000+off]; value != 0 {
				r[task-fixtureDirect+off] = value
			}
		}
		r[task-fixtureDirect+0x640] = 'x'
		r.entry(previous-fixtureDirect, node)
		r.entry(node-fixtureDirect+8, previous)
		previous = node
	}
	r.entry(previous-fixtureDirect, head)
	r.entry(head-primaryImage+8, previous)
	got := primarySnapshot(context.Background(), s, "python3", 1, primaryRelease, va)
	if got.Status != "diagnostic-unavailable" || got.Reason != "task list cycle or bound" {
		t.Fatal(got)
	}
}

// Alter only the captured leaf metadata after a successful identity lookup.
type primaryLeafHole struct {
	sparse.Source
	at uint64
}

func (s primaryLeafHole) RunAt(at, n uint64) (sparse.Run, error) {
	r, err := s.Source.RunAt(at, n)
	if err == nil && at == s.at {
		return primaryBadRun{r, sparse.Hole, false}, nil
	}
	return r, err
}
func TestSnapshotPrimaryCapturedUnknownPTE(t *testing.T) {
	_, s, va := primaryFixture(t)
	good := primarySnapshot(context.Background(), s, "python3", 1, primaryRelease, va)
	if good.Walk == nil {
		t.Fatal(good)
	}
	bad := primaryLeafHole{s, good.Walk.Entries[3].Address}
	got := primarySnapshot(context.Background(), bad, "python3", 1, primaryRelease, va)
	if got.Status != "diagnostic-unavailable" || got.Walk != nil {
		t.Fatal(got)
	}
}
func TestSnapshotPrimaryEveryABISignatureRequired(t *testing.T) {
	for _, sig := range primarySignatures {
		r, s, va := primaryFixture(t)
		r[sig.va-primaryImage] ^= 1
		got := primarySnapshot(context.Background(), s, "python3", 1, primaryRelease, va)
		if got.Status != "diagnostic-unavailable" {
			t.Fatalf("accepted signature %x", sig.va)
		}
	}
}

func TestSnapshotPrimaryCapturedPayloadHole(t *testing.T) {
	r, s, va := primaryFixture(t)
	good := primarySnapshot(context.Background(), s, "python3", 1, primaryRelease, va)
	if good.Walk == nil {
		t.Fatal(good)
	}
	// The decoded metadata remains Data. A canonical unlayered Hole in the
	// selected data page must remain visible rather than discard the mapping.
	start := good.Walk.Physical &^ uint64(4095)
	holeSource, err := sparse.NewSource(r, 1<<30, []sparse.Extent{{Offset: start, Size: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	got := primarySnapshot(context.Background(), holeSource, "python3", 1, primaryRelease, va)
	if got.Status != "observed" || got.Walk == nil || got.Walk.Data == nil || got.Walk.Data.Nonzero != 0 || got.Walk.Data.FFBytes != 0 || got.Walk.Data.Kinds["Hole"].Bytes != 16 {
		t.Fatalf("%+v", got)
	}
}
