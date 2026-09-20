package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckpointPublicationReport(t *testing.T) {
	s := "manifest://" + strings.Repeat("a", 64)
	e := "manifest://" + strings.Repeat("b", 64)
	valid := `{"snapshotRef":"` + s + `","sandboxRef":"` + e + `","removedRefs":[]}`
	for _, test := range []struct {
		name, body string
		invalid    bool
	}{
		{"valid", valid, false}, {"missing E", `{"snapshotRef":"` + s + `","removedRefs":[]}`, true},
		{"wrong role", `{"sandboxRef":"` + e + `","removedRefs":[]}`, true}, {"trailing", valid + "{}", true}, {"polluted", "log\n" + valid, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec := publicationSpec("", false)
			spec.Paths.SandboxCtl = filepath.Join(t.TempDir(), "sandbox-ctl")
			script := "#!/bin/sh\nprintf '%s\\n' '" + test.body + "'\nprintf '%s\\n' 'stderr diagnostics' >&2\n"
			if err := os.WriteFile(spec.Paths.SandboxCtl, []byte(script), 0755); err != nil {
				t.Fatal(err)
			}
			p := &buildPipeline{ctx: context.Background(), spec: spec, publication: BuildPublicationPlan{CheckpointClassTarget: CheckpointClassManifestStore}}
			ref, err := p.publishSnapshot("checkpoint.snapshot")
			if (err != nil) != test.invalid {
				t.Fatalf("ref=%q err=%v", ref, err)
			}
			if !test.invalid && ref != s {
				t.Fatalf("ref=%q", ref)
			}
		})
	}
}
