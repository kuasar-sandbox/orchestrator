package taskartifact

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestPrepareReturnsExactRootPairAndBindsRun(t *testing.T) {
	const body = "resources:\n  capacity: {cpu: 2, memory: 512MiB}\nboot: {}\n"
	for _, carrier := range []string{"local", "located", "bundle", "upload"} {
		t.Run(carrier, func(t *testing.T) {
			var root, wantE, dir, cfg, parent string
			switch carrier {
			case "local", "located":
				dir, wantE, _, root, _ = writeTaskLocalArtifacts(t, body, nil, false)
			case "bundle":
				dir = t.TempDir()
				path, s, e, c := writeTaskManifestBundleArtifacts(t, dir, nil, body)
				cfg = c
				root = "file://" + filepath.Base(path) + "@manifest:" + s
				wantE = "file://" + filepath.Base(path) + "@manifest:" + e
			case "upload":
				e, s, c := writeTaskManifestStoreArtifacts(t, body)
				root = s
				wantE = e
				cfg = c
			}
			if carrier == "located" {
				parent = t.TempDir()
				location, err := reflocation.Resolve("file://"+parent, "initial-source")
				if err != nil {
					t.Fatal(err)
				}
				located := location.Path
				if err := os.MkdirAll(filepath.Dir(located), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(dir, located); err != nil {
					t.Fatal(err)
				}
				dir = located
				root += "@location:initial-source"
				wantE += "@location:initial-source"
				parent = "file://" + parent
			}
			spec := configsock.ArtifactPrepareSpec{RunID: "run-current", RootSourceKind: string(types.ResumeSourceSnapshot), RootRef: root,
				LaunchMode: string(types.LaunchMemory), RelativeDir: dir, RefLocationParent: parent, ManifestConfig: cfg, MaxRefs: 16}
			first, err := Prepare(context.Background(), spec)
			if err != nil {
				t.Fatal(err)
			}
			want := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: root, SandboxRef: wantE}
			if first.Summary.RootSource != want {
				t.Fatalf("pair=%+v want=%+v", first.Summary.RootSource, want)
			}
			again, err := Prepare(context.Background(), spec)
			if err != nil || !configsock.EqualArtifactPrepareSummary(first.Summary, again.Summary) {
				t.Fatalf("retry differs: %v", err)
			}
			spec.RunID = "run-other"
			other, err := Prepare(context.Background(), spec)
			if err != nil || other.Summary.ResolutionDigest == first.Summary.ResolutionDigest {
				t.Fatalf("digest did not bind RunID: %v", err)
			}
			spec.RunID = "run-current"
			spec.RootSandboxRef = wantE
			accepted, err := Prepare(context.Background(), spec)
			if err != nil || accepted.Summary.RootSource != want {
				t.Fatalf("persisted pair changed: %v", err)
			}
			spec.LaunchMode = string(types.LaunchCold)
			cold, err := Prepare(context.Background(), spec)
			if err != nil {
				t.Fatal(err)
			}
			if cold.Summary.RootSource != want || cold.PreparedSource.Kind != types.ResumeSourceSandbox || cold.PreparedSource.SandboxRef != "" || cold.Summary.ResolutionDigest == first.Summary.ResolutionDigest {
				t.Fatalf("cold source=%+v summary=%+v", cold.PreparedSource, cold.Summary)
			}
			spec.RootSandboxRef = "manifest://" + strings.Repeat("f", 64)
			if _, err := Prepare(context.Background(), spec); err == nil || !strings.Contains(err.Error(), "accepted pair") {
				t.Fatalf("conflicting E accepted: %v", err)
			}
		})
	}
}
