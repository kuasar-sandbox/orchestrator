package taskartifact

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestPrepareSummarizesBuildCommandsWithoutReturningCommandText(t *testing.T) {
	for _, command := range []string{"", "e2b.start_cmd", "e2b.ready_cmd"} {
		t.Run(command, func(t *testing.T) {
			cfg := "resources:\n  capacity: {cpu: 2, memory: 1GiB}\nboot: {}\n"
			if command != "" {
				cfg += "metadata:\n  " + command + ": tenant-command-text\n"
			}
			dir, _, sbx, _, snp := writeTaskLocalArtifacts(t, cfg, nil, false)
			for _, source := range []struct {
				kind types.ResumeSourceKind
				path string
			}{{types.ResumeSourceSandbox, sbx}, {types.ResumeSourceSnapshot, snp}} {
				result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{RootSourceKind: string(source.kind), RootRef: source.path, LaunchMode: string(types.LaunchCold), RelativeDir: dir, MaxRefs: 16, ReadSourceImageConfig: true})
				if err != nil {
					t.Fatal(err)
				}
				if result.Summary.HasBuildCommands != (command != "") {
					t.Fatalf("%s command presence = %t", source.kind, result.Summary.HasBuildCommands)
				}
				body, err := json.Marshal(result.Summary)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(body), "tenant-command-text") {
					t.Fatal("command text crossed task boundary")
				}
				ordinary, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{RootSourceKind: string(source.kind), RootRef: source.path, LaunchMode: string(types.LaunchCold), RelativeDir: dir, MaxRefs: 16})
				if err != nil {
					t.Fatal(err)
				}
				body, err = json.Marshal(ordinary.Summary)
				if err != nil || strings.Contains(string(body), "has_build_commands") {
					t.Fatalf("ordinary Sandbox wire changed: %s, %v", body, err)
				}
			}
		})
	}
}
