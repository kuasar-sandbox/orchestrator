package builder

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
)

func TestCaptureSnapshotReferencePreservesActualResult(t *testing.T) {
	directory := t.TempDir()
	for _, file := range []struct{ name, scheme string }{{"captured.bundle", "manifest"}, {"captured.snapshot", "digest"}} {
		if err := os.WriteFile(filepath.Join(directory, file.name), nil, 0600); err != nil {
			t.Fatal(err)
		}
		for _, annotation := range []string{"", "@runtime:captured-runtime.bundle:digest:" + strings.Repeat("b", 64)} {
			ref := "file://" + file.name + "@" + file.scheme + ":" + strings.Repeat("a", 64) + annotation
			output, err := json.Marshal(artifact.PublishReport{
				SnapshotRef: ref, SandboxRef: "manifest://" + strings.Repeat("c", 64), RemovedRefs: []string{},
			})
			if err != nil {
				t.Fatal(err)
			}
			got, err := capturedSnapshotRef(output, directory)
			want, _ := manifest.ParseRef(ref)
			want.Path = filepath.Join(directory, want.Path)
			if err != nil || got != want.String() {
				t.Fatalf("capture reference lost: %q, %v; want %s", got, err, want.String())
			}
		}
	}
	for _, output := range []string{
		"snapshot complete",
		`{"snapshotRef":"file://missing.snapshot@digest:` + strings.Repeat("a", 64) + `","sandboxRef":"manifest://` + strings.Repeat("b", 64) + `","removedRefs":[]}`,
		`{"snapshotRef":"file://captured.snapshot@runtime:","sandboxRef":"manifest://` + strings.Repeat("b", 64) + `","removedRefs":[]}`,
	} {
		if _, err := capturedSnapshotRef([]byte(output), directory); err == nil {
			t.Fatalf("invalid capture result accepted: %s", output)
		}
	}
}
