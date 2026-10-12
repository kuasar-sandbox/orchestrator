// Observation-only local Snapshot S reader. No extraction, fetcher, or VM side effects.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

const snapshotDiagnosticMaxWindow = 64 << 10

type snapshotDiagnosticCounts struct {
	Runs    uint64 `json:"runs"`
	Bytes   uint64 `json:"bytes"`
	Nonzero uint64 `json:"nonzero"`
}
type snapshotDiagnosticReport struct {
	Ref           string                              `json:"ref"`
	IdentityCheck string                              `json:"identity_check"`
	MemoryBytes   uint64                              `json:"memory_bytes"`
	Offset        uint64                              `json:"offset"`
	Length        uint64                              `json:"length"`
	Nonzero       uint64                              `json:"nonzero"`
	Digest        string                              `json:"sha256"`
	FFBytes       uint64                              `json:"ff_bytes"`
	Kinds         map[string]snapshotDiagnosticCounts `json:"source_runs"`
}

func snapshotDiagnosticWindow(ctx context.Context, source sparse.Source, offset, length uint64) (snapshotDiagnosticReport, error) {
	out := snapshotDiagnosticReport{MemoryBytes: source.Size(), Offset: offset, Length: length, Kinds: make(map[string]snapshotDiagnosticCounts)}
	if length == 0 || length > snapshotDiagnosticMaxWindow || offset >= source.Size() || length > source.Size()-offset {
		return out, errors.New("snapshotDiagnosticWindow must be 1..65536 bytes entirely inside Snapshot memory (ZIP tail excluded)")
	}
	buf := make([]byte, length)
	for pos := offset; pos < offset+length; {
		run, err := source.RunAt(pos, offset+length-pos)
		if err != nil {
			return out, err
		}
		if run.Offset() != pos || run.End() <= pos || run.End() > offset+length {
			return out, errors.New("invalid source run bounds")
		}
		kind := ""
		switch run.Kind() {
		case sparse.Hole:
			kind = "Hole"
		case sparse.Zero:
			kind = "Zero"
		case sparse.Data:
			kind = "Data"
		default:
			return out, errors.New("unsupported source run kind")
		}
		part := buf[pos-offset : run.End()-offset]
		n, err := run.ReadAt(ctx, part, 0)
		if err != nil {
			return out, err
		}
		if n != len(part) {
			return out, errors.New("short source run read")
		}
		c := out.Kinds[kind]
		c.Runs++
		c.Bytes += uint64(n)
		for _, b := range part {
			if b != 0 {
				c.Nonzero++
				out.Nonzero++
			}
		}
		out.Kinds[kind] = c
		pos = run.End()
	}
	out.Digest = fmt.Sprintf("%x", sha256.Sum256(buf))
	for _, b := range buf {
		if b == 255 {
			out.FFBytes++
		}
	}
	return out, nil
}

func inspectSnapshotDiagnostic(ctx context.Context, raw string, offset, length uint64) (snapshotDiagnosticReport, error) {
	var empty snapshotDiagnosticReport
	if length == 0 || length > snapshotDiagnosticMaxWindow {
		return empty, errors.New("length must be 1..65536")
	}
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		return empty, err
	}
	if ref.Scheme != manifest.RefSchemeFile || ref.DigestScheme != tarstream.DigestScheme || ref.Location != "" || !filepath.IsAbs(ref.Path) {
		return empty, errors.New("require file:///absolute/path@digest:<64hex>; encrypted, manifest, and located inputs unsupported")
	}
	info, err := os.Stat(ref.Path)
	if err != nil {
		return empty, err
	}
	if !info.Mode().IsRegular() {
		return empty, errors.New("input must be a local regular file")
	}
	// No codec means encrypted carriers fail closed. Manifest bundles also fail
	// this production tarstream parser; there is no format or remote fallback.
	stream, err := fetch.OpenTarStream(ref.Path, tarstream.WithExpectedDigest(ref.DigestScheme, ref.Digest))
	if err != nil {
		return empty, fmt.Errorf("plaintext canonical tarstream required (encrypted/manifest inputs unsupported): %w", err)
	}
	root, err := snapshotfile.Open(ctx, stream) // closes stream on error
	if err != nil {
		return empty, err
	}
	defer root.Close()
	cfg, err := snapshot.ParseConfig(root.SnapshotConfig)
	if err != nil {
		return empty, err
	}
	canonical, err := snapshot.MarshalConfig(cfg)
	if err != nil {
		return empty, err
	}
	if !bytes.Equal(canonical, root.SnapshotConfig) {
		return empty, errors.New("snapshot.cfg is not canonical")
	}
	// Reject before reading ANY memory snapshotDiagnosticWindow, even if it looks like Data.
	if len(cfg.FromRefs) != 0 {
		return empty, fmt.Errorf("unresolved memory parents: %d from_refs; layered Snapshots unsupported", len(cfg.FromRefs))
	}
	out, err := snapshotDiagnosticWindow(ctx, root.Memory, offset, length)
	if err != nil {
		return empty, err
	}
	out.Ref = ref.String()
	out.IdentityCheck = "production expected carrier identity matched; payload not fully rehashed"
	return out, nil
}

// snapshotWindowProcess exists only in the prepared Go test executable.
func snapshotWindowProcess() {
	if len(os.Args) > 1 && os.Args[1] == "__e2e_snapshot_pagetable" {
		os.Exit(runSnapshotPageTableDiagnostic(os.Args[2:]))
	}
	if len(os.Args) < 2 || os.Args[1] != "__e2e_snapshot_window" {
		return
	}
	os.Exit(runSnapshotWindowDiagnostic(os.Args[2:]))
}

func runSnapshotWindowDiagnostic(args []string) int {
	flags := flag.NewFlagSet("snapshot-snapshotDiagnosticWindow", flag.ContinueOnError)
	ref := flags.String("ref", "", "local snapshot reference")
	offset := flags.Uint64("offset", 0, "explicit logical memory byte offset")
	length := flags.Uint64("length", 0, "bounded snapshotDiagnosticWindow bytes")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	explicitOffset := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "offset" {
			explicitOffset = true
		}
	})
	if !explicitOffset || flags.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "explicit snapshot reference, offset and length required")
		return 2
	}
	out, err := inspectSnapshotDiagnostic(context.Background(), *ref, *offset, *length)
	if err == nil {
		err = json.NewEncoder(os.Stdout).Encode(out)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "snapshot snapshotDiagnosticWindow unavailable:", err)
		return 1
	}
	return 0
}
