package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

func snapshotDiagnosticFixture(t *testing.T, parents bool) string {
	t.Helper()
	data := make([]byte, 8192)
	data[0] = 255
	data[8191] = 7
	memory, err := sparse.NewSource(bytes.NewReader(data), uint64(len(data)), []sparse.Extent{{Offset: 4096, Size: 2048}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &snapshot.Config{Version: 1, SandboxRef: "file://sandbox@digest:" + strings.Repeat("1", 64)}
	if parents {
		cfg.FromRefs = []string{"file://parent@digest:" + strings.Repeat("2", 64)}
	}
	raw, err := snapshot.MarshalConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	src, err := snapshotfile.BuildSource(memory, []byte(`{}`), []byte(`{}`), raw)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "s.tar")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	scheme, digest, err := tarstream.WriteTo(context.Background(), f, "snapshot", src)
	closeErr := f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	return "file://" + path + "@" + scheme + ":" + digest
}

func TestSnapshotDiagnosticWindow(t *testing.T) {
	ref := snapshotDiagnosticFixture(t, false)
	out, err := inspectSnapshotDiagnostic(context.Background(), ref, 0, 8192)
	if err != nil {
		t.Fatal(err)
	}
	if out.MemoryBytes != 8192 || out.Nonzero != 2 || out.Kinds["Hole"].Bytes != 2048 || out.Kinds["Data"].Bytes != 6144 {
		t.Fatalf("unexpected report: %+v", out)
	}
	for _, w := range [][2]uint64{{0, 0}, {0, 65537}, {8192, 1}, {8191, 2}, {^uint64(0), 2}} {
		if _, err := inspectSnapshotDiagnostic(context.Background(), ref, w[0], w[1]); err == nil {
			t.Fatalf("accepted bad bounds %v", w)
		}
	}
	badRef := ref[:len(ref)-64] + strings.Repeat("0", 64)
	if _, err := inspectSnapshotDiagnostic(context.Background(), badRef, 0, 1); err == nil {
		t.Fatal("accepted wrong identity")
	}
}

func TestSnapshotDiagnosticRejectParentsAndUnsupportedRefs(t *testing.T) {
	_, err := inspectSnapshotDiagnostic(context.Background(), snapshotDiagnosticFixture(t, true), 0, 1)
	if err == nil || !strings.Contains(err.Error(), "unresolved memory parents") {
		t.Fatalf("parent rejection: %v", err)
	}
	for _, ref := range []string{"manifest://" + strings.Repeat("1", 64), "file:///tmp/s@hmac:" + strings.Repeat("1", 64), "file:///tmp/s@manifest:" + strings.Repeat("1", 64), "file:///tmp/s"} {
		if _, err := inspectSnapshotDiagnostic(context.Background(), ref, 0, 1); err == nil {
			t.Fatalf("accepted %s", ref)
		}
	}
}
