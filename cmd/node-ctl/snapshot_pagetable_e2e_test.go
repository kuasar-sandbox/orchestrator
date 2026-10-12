// Offline diagnostic of the saved x86 page tables; test executable only.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

type diagnosticCPU struct {
	Path string `json:"path"`
	CR3  uint64 `json:"cr3"`
	CR4  uint64 `json:"cr4"`
}
type diagnosticPTE struct {
	Level   int    `json:"level"`
	Address uint64 `json:"entry_address"`
	Value   string `json:"entry_hex"`
	Kind    string `json:"source_kind"`
}
type diagnosticWalk struct {
	CPU       diagnosticCPU             `json:"cpu"`
	VA        uint64                    `json:"virtual_address"`
	Status    string                    `json:"status"`
	Entries   []diagnosticPTE           `json:"entries"`
	Physical  uint64                    `json:"physical_address,omitempty"`
	PageBytes uint64                    `json:"page_bytes,omitempty"`
	Data      *snapshotDiagnosticReport `json:"data,omitempty"`
}

func diagnosticCPUs(raw []byte) ([]diagnosticCPU, error) {
	if len(raw) > 8<<20 {
		return nil, errors.New("state exceeds diagnostic bound")
	}
	var found []diagnosticCPU
	budget := 100000
	var visit func(any, string, int) error
	visit = func(node any, path string, depth int) error {
		budget--
		if budget < 0 || depth > 40 {
			return errors.New("state tree exceeds diagnostic bound")
		}
		switch v := node.(type) {
		case map[string]any:
			// kvm-bindings 0.12.1 serializes kvm_sregs as 312 opaque ABI bytes.
			// x86 UAPI offsets: CR3=240, CR4=248 (not JSON named fields).
			if fields, ok := v["sregs"].([]any); ok {
				if len(fields) != 312 {
					return errors.New("unsupported serialized kvm_sregs size")
				}
				var raw [312]byte
				for i, field := range fields {
					number, ok := field.(json.Number)
					if !ok {
						return errors.New("non-integer kvm_sregs byte")
					}
					value, err := strconv.ParseUint(string(number), 10, 8)
					if err != nil {
						return err
					}
					raw[i] = byte(value)
				}
				found = append(found, diagnosticCPU{path + "/sregs-abi", binary.LittleEndian.Uint64(raw[240:248]), binary.LittleEndian.Uint64(raw[248:256])})
				if len(found) > 16 {
					return errors.New("too many CPU roots")
				}
			}
			if n, ok := v["cr3"].(json.Number); ok {
				cr3, e := strconv.ParseUint(string(n), 10, 64)
				if e != nil {
					return e
				}
				n4, ok := v["cr4"].(json.Number)
				if !ok {
					return errors.New("CR3 without CR4")
				}
				cr4, e := strconv.ParseUint(string(n4), 10, 64)
				if e != nil {
					return e
				}
				found = append(found, diagnosticCPU{path, cr3, cr4})
				if len(found) > 16 {
					return errors.New("too many CPU roots")
				}
			}
			for k, x := range v {
				if e := visit(x, path+"/"+k, depth+1); e != nil {
					return e
				}
			}
		case []any:
			for i, x := range v {
				if e := visit(x, fmt.Sprintf("%s/%d", path, i), depth+1); e != nil {
					return e
				}
			}
		case string:
			if strings.HasPrefix(v, "{") || strings.HasPrefix(v, "[") {
				d := json.NewDecoder(strings.NewReader(v))
				d.UseNumber()
				var x any
				if d.Decode(&x) == nil {
					return visit(x, path+"/json", depth+1)
				}
			}
		}
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var node any
	if err := d.Decode(&node); err != nil {
		return nil, err
	}
	if err := visit(node, "state", 0); err != nil {
		return nil, err
	}
	if len(found) == 0 {
		return nil, errors.New("no saved CPU paging root")
	}
	return found, nil
}

func walkSnapshotTable(ctx context.Context, memory sparse.Source, cpu diagnosticCPU, va uint64) (diagnosticWalk, error) {
	out := diagnosticWalk{CPU: cpu, VA: va, Status: "unresolved"}
	// This diagnostic is deliberately restricted to the one-GiB x86 test.
	if memory.Size() != 1<<30 || va >= 1<<47 {
		return out, errors.New("unsupported memory geometry or non-user VA")
	}
	const addrMask = uint64(0x000ffffffffff000)
	table := cpu.CR3 & addrMask
	levels := 4
	if cpu.CR4&(1<<12) != 0 {
		levels = 5
	}
	for level := levels; level >= 1; level-- {
		at := table + ((va>>uint(12+9*(level-1)))&511)*8
		if at > memory.Size()-8 {
			return out, errors.New("page table outside saved RAM")
		}
		run, err := memory.RunAt(at, 8)
		if err != nil {
			return out, err
		}
		if run.Offset() != at || run.End() != at+8 {
			return out, errors.New("split page-table entry")
		}
		var raw [8]byte
		n, err := run.ReadAt(ctx, raw[:], 0)
		if err != nil && err != io.EOF {
			return out, err
		}
		if n != 8 {
			return out, io.ErrUnexpectedEOF
		}
		value := binary.LittleEndian.Uint64(raw[:])
		kind := map[sparse.RunKind]string{sparse.Data: "Data", sparse.Zero: "Zero", sparse.Hole: "Hole"}[run.Kind()]
		out.Entries = append(out.Entries, diagnosticPTE{level, at, fmt.Sprintf("0x%016x", value), kind})
		if value&1 == 0 {
			out.Status = "saved_entry_not_present"
			return out, nil
		}
		huge := level > 1 && value&(1<<7) != 0
		if huge && level != 2 && level != 3 {
			return out, errors.New("invalid large-page level")
		}
		if level == 1 || huge {
			shift := uint(12 + 9*(level-1))
			size := uint64(1) << shift
			physical := (value & addrMask &^ (size - 1)) | (va & (size - 1))
			if physical > memory.Size()-16 {
				return out, errors.New("mapped address outside saved RAM")
			}
			data, err := snapshotDiagnosticWindow(ctx, memory, physical, 16)
			if err != nil {
				return out, err
			}
			out.Status = "saved_mapping_resolved"
			out.Physical = physical
			out.PageBytes = size
			out.Data = &data
			return out, nil
		}
		table = value & addrMask
	}
	return out, errors.New("unresolved paging walk")
}

func runSnapshotPageTableDiagnostic(args []string) int {
	fs := flag.NewFlagSet("snapshot-pagetable", flag.ContinueOnError)
	raw := fs.String("ref", "", "retained local snapshot")
	va := fs.Uint64("virtual-address", 0, "observed user virtual address")
	if fs.Parse(args) != nil || fs.NArg() != 0 || *va == 0 {
		return 2
	}
	ref, err := manifest.ParseRef(*raw)
	if err != nil || ref.Scheme != manifest.RefSchemeFile || ref.DigestScheme != tarstream.DigestScheme || ref.Location != "" || !strings.HasPrefix(ref.Path, "/") {
		fmt.Fprintln(os.Stderr, "unsupported snapshot reference")
		return 2
	}
	stream, err := fetch.OpenTarStream(ref.Path, tarstream.WithExpectedDigest(ref.DigestScheme, ref.Digest))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	root, err := snapshotfile.Open(context.Background(), stream)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer root.Close()
	cfg, err := snapshot.ParseConfig(root.SnapshotConfig)
	if err != nil || len(cfg.FromRefs) != 0 {
		fmt.Fprintln(os.Stderr, "unresolved snapshot graph")
		return 1
	}
	cpus, err := diagnosticCPUs(root.StateJSON)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	walks := make([]diagnosticWalk, 0, len(cpus))
	for _, cpu := range cpus {
		w, e := walkSnapshotTable(context.Background(), root.Memory, cpu, *va)
		if e != nil {
			w.Status = "unavailable: " + e.Error()
		}
		walks = append(walks, w)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"basis": "saved CPU CR3; primary process identity not assumed", "walks": walks}); err != nil {
		return 1
	}
	return 0
}
