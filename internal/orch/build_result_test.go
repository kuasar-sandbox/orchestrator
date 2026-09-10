package orch

import (
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestValidateBuildResultAcceptsExactlyOneTargetArtifact(t *testing.T) {
	ref := "manifest://" + strings.Repeat("a", 64)
	for _, test := range []struct {
		name   string
		build  *types.Build
		result types.BuildResult
	}{
		{
			name:  "auto image",
			build: &types.Build{Profile: types.ProfileE2B},
			result: types.BuildResult{
				Target: types.BuildTarget{Kind: types.BuildTargetImage}, ImageRef: ref,
			},
		},
		{
			name:  "auto memory sandbox",
			build: &types.Build{Profile: types.ProfileE2B, ReadyCmd: "probe"},
			result: types.BuildResult{
				Target:      types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true},
				SnapshotRef: ref, ReadyCmd: "probe",
			},
		},
		{
			name: "explicit top-level Sandbox E",
			build: &types.Build{Profile: types.ProfileE2B, StartCmd: "declared-only", Builder: types.BuildOptions{
				Target: &types.BuildTarget{Kind: types.BuildTargetSandbox},
			}},
			result: types.BuildResult{
				Target: types.BuildTarget{Kind: types.BuildTargetSandbox}, SandboxRef: ref,
				StartCmd: "declared-only",
			},
		},
		{
			name: "explicit empty memory sandbox",
			build: &types.Build{Profile: types.ProfileBare, Builder: types.BuildOptions{
				Target: &types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true},
			}},
			result: types.BuildResult{
				Target: types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true}, SnapshotRef: ref,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateBuildResult(test.build, test.result); err != nil {
				t.Fatalf("valid result rejected: %v", err)
			}
		})
	}
}

func TestValidateBuildResultFailsClosedOnTargetOrArtifactMismatch(t *testing.T) {
	ref := "manifest://" + strings.Repeat("a", 64)
	image := types.BuildTarget{Kind: types.BuildTargetImage}
	topLevelSandbox := types.BuildTarget{Kind: types.BuildTargetSandbox}
	memory := types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true}
	for _, test := range []struct {
		name   string
		build  *types.Build
		result types.BuildResult
	}{
		{name: "missing target", build: &types.Build{}, result: types.BuildResult{ImageRef: ref}},
		{name: "explicit mismatch", build: &types.Build{Builder: types.BuildOptions{Target: &topLevelSandbox}}, result: types.BuildResult{Target: image, ImageRef: ref}},
		{name: "auto mismatch", build: &types.Build{}, result: types.BuildResult{Target: memory, SnapshotRef: ref}},
		{name: "image command", build: &types.Build{}, result: types.BuildResult{Target: image, ImageRef: ref, StartCmd: "serve"}},
		{name: "image with sandbox ref", build: &types.Build{}, result: types.BuildResult{Target: image, ImageRef: ref, SandboxRef: ref}},
		{name: "top-level Sandbox E wrong ref", build: &types.Build{Builder: types.BuildOptions{Target: &topLevelSandbox}}, result: types.BuildResult{Target: topLevelSandbox, SnapshotRef: ref}},
		{name: "memory wrong ref", build: &types.Build{Builder: types.BuildOptions{Target: &memory}}, result: types.BuildResult{Target: memory, SandboxRef: ref}},
		{name: "malformed portable ref", build: &types.Build{}, result: types.BuildResult{Target: image, ImageRef: "manifest://short"}},
		{name: "successful failure stage", build: &types.Build{}, result: types.BuildResult{Target: image, ImageRef: ref, FailureStage: "runtime"}},
		{name: "failed result with artifact", build: &types.Build{}, result: types.BuildResult{Error: "failed", Target: image, ImageRef: ref}},
		{name: "changes explicit start", build: &types.Build{StartCmd: "serve"}, result: types.BuildResult{Target: memory, SnapshotRef: ref, StartCmd: "other"}},
		{name: "changes explicit ready", build: &types.Build{ReadyCmd: "probe"}, result: types.BuildResult{Target: memory, SnapshotRef: ref, ReadyCmd: "other"}},
		{name: "introduces command without source", build: &types.Build{Builder: types.BuildOptions{Target: &topLevelSandbox}}, result: types.BuildResult{Target: topLevelSandbox, SandboxRef: ref, StartCmd: "injected"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateBuildResult(test.build, test.result); err == nil {
				t.Fatalf("invalid result accepted: %+v", test.result)
			}
		})
	}
	if err := validateBuildResult(&types.Build{}, types.BuildResult{Error: "failed", FailureStage: "artifact_prepare"}); err != nil {
		t.Fatalf("plain failure result rejected: %v", err)
	}
}
