// Failure-only, offline diagnostic. These offsets are NOT a supported kernel API.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

const primaryRelease = "6.1.169-sandbox-g21f1e158f70e"
const primaryImage = uint64(0xffffffff80000000)
const primaryInit = uint64(0xffffffff81c16580)

// Verified against ELF sha256 a39ce0ea58631663956653cffae2d2722bfa26f0fdddcc3e0962f3700fda96cb.
// Saved instruction checks identify this explicit ABI; they do not verify the payload digest.
var primarySignatures = []struct {
	va   uint64
	code string
}{
	{0xffffffff8106f350, "4d8bb42468030000"},
	{0xffffffff8104a0fc, "488b9db8030000"},
	{0xffffffff8106f2e0, "8b9348040000"},
	{0xffffffff8106f262, "488db340060000"},
	{0xffffffff81062f80, "4881c5b0040000"},
	{0xffffffff81044703, "488b7b38"},
	{0xffffffff81062e64, "48c1e0044801c7483b7768"},
	{0xffffffff81062e71, "8b5760"},
}

type primaryObservation struct {
	Status       string          `json:"status"`
	Reason       string          `json:"reason,omitempty"`
	Profile      string          `json:"abi_profile,omitempty"`
	Comm         string          `json:"comm,omitempty"`
	PID          uint32          `json:"global_pid,omitempty"`
	NamespacePID uint32          `json:"namespace_pid,omitempty"`
	PIDLevel     uint32          `json:"pid_level,omitempty"`
	Task         uint64          `json:"task_va,omitempty"`
	MM           uint64          `json:"mm_va,omitempty"`
	PGD          uint64          `json:"pgd_va,omitempty"`
	PGDPhysical  uint64          `json:"pgd_gpa,omitempty"`
	Walk         *diagnosticWalk `json:"walk,omitempty"`
}

// Also wraps the existing walker so missing/split metadata cannot become a PTE.
type primaryMemory struct {
	sparse.Source
	ctx       context.Context
	remaining uint64
	direct    uint64
}

func (m *primaryMemory) RunAt(at, length uint64) (sparse.Run, error) {
	if m.ctx.Err() != nil || length == 0 || length > m.remaining || at >= m.Size() || length > m.Size()-at {
		return nil, errors.New("diagnostic read bound")
	}
	m.remaining -= length
	r, e := m.Source.RunAt(at, length)
	if e != nil {
		return nil, errors.New("source unavailable")
	}
	if r.Offset() != at || r.End() != at+length || (r.Kind() != sparse.Data && r.Kind() != sparse.Zero) {
		return nil, errors.New("unknown or split metadata")
	}
	return r, nil
}
func (m *primaryMemory) physical(va, n uint64) (uint64, error) {
	var base uint64
	switch {
	case va >= primaryImage:
		base = primaryImage
	case m.direct != 0 && va >= m.direct && va-m.direct < 1<<30:
		base = m.direct
	default:
		return 0, errors.New("invalid kernel pointer")
	}
	at := va - base
	if n == 0 || at >= m.Size() || n > m.Size()-at {
		return 0, errors.New("pointer outside RAM")
	}
	return at, nil
}
func (m *primaryMemory) read(va, n uint64) ([]byte, error) {
	at, e := m.physical(va, n)
	if e != nil {
		return nil, e
	}
	r, e := m.RunAt(at, n)
	if e != nil {
		return nil, e
	}
	b := make([]byte, n)
	count, e := r.ReadAt(m.ctx, b, 0)
	if e != nil || count != len(b) {
		return nil, errors.New("metadata read unavailable")
	}
	return b, nil
}
func (m *primaryMemory) field(va, off, n uint64) ([]byte, error) {
	// Check the whole span before adding, including overflow and object bounds.
	if _, e := m.physical(va, off+n); e != nil {
		return nil, e
	}
	return m.read(va+off, n)
}
func (m *primaryMemory) word(va, off uint64) (uint64, error) {
	b, e := m.field(va, off, 8)
	if e != nil {
		return 0, e
	}
	return binary.LittleEndian.Uint64(b), nil
}
func primarySnapshot(ctx context.Context, source sparse.Source, comm string, nsPID uint64, release string, va uint64) primaryObservation {
	out, e := decodePrimarySnapshot(ctx, source, comm, nsPID, release, va)
	if e != nil {
		return primaryObservation{Status: "diagnostic-unavailable", Reason: e.Error()}
	}
	return out
}
func decodePrimarySnapshot(ctx context.Context, source sparse.Source, comm string, nsPID uint64, release string, va uint64) (primaryObservation, error) {
	var out primaryObservation
	fail := func(s string) (primaryObservation, error) { return out, errors.New(s) }
	if release != primaryRelease || nsPID != 1 || len(comm) == 0 || len(comm) > 15 || strings.ContainsAny(comm, "\x00\n\r") {
		return fail("missing identity or unsupported profile")
	}
	if source.Size() != 1<<30 {
		return fail("unsupported RAM geometry")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	m := &primaryMemory{Source: source, ctx: ctx, remaining: 4 << 20}
	for _, s := range primarySignatures {
		want, _ := hex.DecodeString(s.code)
		got, e := m.read(s.va, uint64(len(want)))
		if e != nil || !bytes.Equal(got, want) {
			return fail("ABI instruction signature unavailable or mismatched")
		}
	}
	b, e := m.read(0xffffffff81b914d0, 4)
	if e != nil || binary.LittleEndian.Uint32(b) != 0 {
		return fail("unsupported paging geometry")
	}
	direct, e := m.word(0xffffffff81b914c0, 0)
	// Four-level Linux direct map region, 1GiB aligned; exclude vmalloc/image regions.
	if e != nil || direct < 0xffff888000000000 || direct >= 0xffffc88000000000 || direct&((1<<30)-1) != 0 {
		return fail("unsupported direct-map geometry")
	}
	m.direct = direct
	initMM, e := m.word(primaryInit, 0x3b8)
	if e != nil || initMM != 0 {
		return fail("invalid init task mm")
	}
	b, e = m.field(primaryInit, 0x640, 16)
	if e != nil || !bytes.HasPrefix(b, []byte("swapper")) {
		return fail("invalid init task comm")
	}
	b, e = m.field(primaryInit, 0x448, 4)
	if e != nil || binary.LittleEndian.Uint32(b) != 0 {
		return fail("invalid init task pid")
	}
	// The fixed image root must itself be accessible in this geometry.
	if _, e = m.read(0xffffffff81c0c000, 4096); e != nil {
		return fail("initial page table unavailable")
	}
	head := primaryInit + 0x368
	node := head
	seen := map[uint64]bool{}
	type namespacePosition struct {
		level  uint32
		parent uint64
	}
	hierarchy := map[uint64]namespacePosition{}
	var outerNamespace uint64
	matches := 0
	for {
		if len(seen) >= 4096 || seen[node] {
			return fail("task list cycle or bound")
		}
		seen[node] = true
		if node < 0x368 {
			return fail("invalid list pointer")
		}
		task := node - 0x368
		next, e := m.word(node, 0)
		if e != nil {
			return fail("invalid next link")
		}
		prev, e := m.word(node, 8)
		if e != nil {
			return fail("invalid prev link")
		}
		back, e := m.word(next, 8)
		if e != nil || back != node {
			return fail("inconsistent next link")
		}
		forward, e := m.word(prev, 0)
		if e != nil || forward != node {
			return fail("inconsistent prev link")
		}
		b, e := m.field(task, 0x640, 16)
		if e != nil {
			return fail("task comm unavailable")
		}
		end := bytes.IndexByte(b, 0)
		if end < 0 {
			return fail("unterminated task comm")
		}
		matchesComm := string(b[:end]) == comm
		if task != primaryInit {
			pidptr, e := m.word(task, 0x4b0)
			if e != nil {
				return fail("thread pid unavailable")
			}
			b, e = m.field(pidptr, 4, 4)
			if e != nil {
				return fail("pid level unavailable")
			}
			level := binary.LittleEndian.Uint32(b)
			if level > 32 {
				return fail("impossible pid hierarchy")
			}
			b, e = m.field(task, 0x448, 4)
			if e != nil {
				return fail("global pid unavailable")
			}
			global := binary.LittleEndian.Uint32(b)
			var inner uint32
			var parent uint64
			namespaces := map[uint64]bool{}
			for i := uint32(0); i <= level; i++ {
				b, e = m.field(pidptr, 0x60+uint64(i)*16, 16)
				if e != nil {
					return fail("pid numbers unavailable")
				}
				nr := binary.LittleEndian.Uint32(b)
				ns := binary.LittleEndian.Uint64(b[8:])
				if nr == 0 || nr > 0x7fffffff || (i == 0 && nr != global) || namespaces[ns] {
					return fail("inconsistent pid hierarchy")
				}
				if _, e = m.physical(ns, 8); e != nil {
					return fail("invalid namespace pointer")
				}
				position := namespacePosition{i, parent}
				if prior, ok := hierarchy[ns]; ok && prior != position {
					return fail("inconsistent namespace ancestry")
				}
				if i == 0 {
					if outerNamespace != 0 && ns != outerNamespace {
						return fail("inconsistent outer namespace")
					}
					outerNamespace = ns
				}
				hierarchy[ns] = position
				parent = ns
				namespaces[ns] = true
				inner = nr
			}
			if matchesComm && inner == uint32(nsPID) {
				matches++
				mm, e := m.word(task, 0x3b8)
				if e != nil {
					return fail("mm unavailable")
				}
				pgd, e := m.word(mm, 0x38)
				if e != nil {
					return fail("pgd unavailable")
				}
				physical, e := m.physical(pgd, 4096)
				if e != nil || physical&4095 != 0 {
					return fail("invalid pgd")
				}
				out = primaryObservation{Status: "observed", Profile: "explicit 6.1.169 ABI instruction signatures; payload digest not verified", Comm: comm, PID: global, NamespacePID: inner, PIDLevel: level, Task: task, MM: mm, PGD: pgd, PGDPhysical: physical}
			}
		}
		node = next
		if node == head {
			break
		}
	}
	if matches != 1 {
		return fail("primary task absent or ambiguous")
	}
	w, e := walkSnapshotTableWithPayload(ctx, m, source, diagnosticCPU{Path: "captured primary task mm.pgd", CR3: out.PGDPhysical}, va)
	if e != nil {
		return primaryObservation{}, errors.New("captured primary walk unavailable")
	}
	out.Walk = &w
	return out, nil
}
