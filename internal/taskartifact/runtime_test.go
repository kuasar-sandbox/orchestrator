package taskartifact

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

// Runtime admission intentionally reads the real SDK footer contract only; VM
// execution and a real EROFS payload are outside this metadata unit test.
func writeAdmissionRuntime(t *testing.T, path, digest string) string {
	t.Helper()
	var footer bytes.Buffer
	zw := zip.NewWriter(&footer)
	if _, err := zw.CreateHeader(&zip.FileHeader{Name: tarstream.DigestMarkerPrefix + digest, Method: zip.Store}); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	const size = 2 << 20
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(footer.Bytes(), int64(size-footer.Len())); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	ref, err := sandbox.ResolveRuntimeProjection(&rtconfig.SandboxConfig{Boot: rtconfig.BootConfig{Runtime: "file://" + path}})
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func runtimeAdmissionArtifacts(t *testing.T, required string) (configsock.SnapshotRuntimeCheckRequest, string) {
	t.Helper()
	// The unavailable disk parent must not be opened by runtime admission.
	raw := "boot:\n  runtime_ref: " + required + "\n  root:\n    base: self\n    base_from_refs:\n      - manifest://" + strings.Repeat("d", 64) + "\n"
	dir, e, ePath, s, sPath := writeTaskLocalArtifacts(t, raw, nil, false)
	er, err := manifest.ParseRef(e)
	if err != nil {
		t.Fatal(err)
	}
	er.Path = ePath
	sr, err := manifest.ParseRef(s)
	if err != nil {
		t.Fatal(err)
	}
	sr.Path = sPath
	return configsock.SnapshotRuntimeCheckRequest{Source: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: sr.String(), SandboxRef: er.String()}, RelativeDir: dir}, ePath
}

func TestSnapshotRuntimeAdmissionUsesActualPairedRuntime(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "runtime-old.bundle")
	newPath := filepath.Join(dir, "runtime-new.bundle")
	required := writeAdmissionRuntime(t, oldPath, strings.Repeat("a", 64))
	writeAdmissionRuntime(t, newPath, strings.Repeat("b", 64))
	spec, ePath := runtimeAdmissionArtifacts(t, required)
	spec.Runtime = "file://" + newPath
	if err := CheckSnapshotRuntime(context.Background(), spec); err != nil {
		t.Fatalf("matching sibling candidate rejected: %v", err)
	}
	fresh := configsock.ArtifactPrepareSpec{RootSourceKind: string(types.ResumeSourceSnapshot), RootRef: spec.Source.Ref, RelativeDir: spec.RelativeDir, LaunchMode: string(types.LaunchMemory), SnapshotRuntime: spec.Runtime}
	if _, err := Prepare(context.Background(), fresh); err != nil {
		t.Fatalf("fresh snp preparation: %v", err)
	}
	if err := os.Remove(oldPath); err != nil {
		t.Fatal(err)
	}
	if err := CheckSnapshotRuntime(context.Background(), spec); !errors.Is(err, restore.ErrRuntimeIncompatible) {
		t.Fatalf("wrong runtime error=%v", err)
	}
	if _, err := Prepare(context.Background(), fresh); !errors.Is(err, restore.ErrRuntimeIncompatible) {
		t.Fatalf("fresh snp wrong runtime error=%v", err)
	}
	// Existing memory resumes add no platform preflight; restore remains responsible.
	fresh.SnapshotRuntime = ""
	if _, err := Prepare(context.Background(), fresh); err != nil {
		t.Fatalf("existing preparation added compatibility gate: %v", err)
	}
	if err := os.Remove(ePath); err != nil {
		t.Fatal(err)
	}
	if err := CheckSnapshotRuntime(context.Background(), spec); err == nil || errors.Is(err, restore.ErrRuntimeIncompatible) {
		t.Fatalf("missing E misclassified: %v", err)
	}
}

func TestSnapshotRuntimeAdmissionAuthenticatesExactPair(t *testing.T) {
	dir := t.TempDir()
	runtimePath := filepath.Join(dir, "runtime.bundle")
	required := writeAdmissionRuntime(t, runtimePath, strings.Repeat("a", 64))
	spec, _ := runtimeAdmissionArtifacts(t, required)
	spec.Runtime = "file://" + runtimePath
	spec.Source.SandboxRef = "manifest://" + strings.Repeat("e", 64)
	if err := CheckSnapshotRuntime(context.Background(), spec); !errors.Is(err, ErrSnapshotPairMismatch) {
		t.Fatalf("pair mismatch error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := CheckSnapshotRuntime(ctx, spec); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
}
