package orch

import (
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// validateBuildResult is the fail-closed worker boundary. Requested target is
// immutable registration input; the worker may resolve auto only by returning
// the effective commands that make the resolution independently checkable.
func validateBuildResult(build *types.Build, result types.BuildResult) error {
	if build == nil {
		return fmt.Errorf("build result has no build definition")
	}
	if result.Error != "" {
		if result.Target.Kind != "" || result.Target.Memory || result.ImageRef != "" ||
			result.SandboxRef != "" || result.SnapshotRef != "" ||
			result.StartCmd != "" || result.ReadyCmd != "" {
			return fmt.Errorf("failed build result must not contain a target, commands, or artifact")
		}
		return nil
	}
	if result.FailureStage != "" {
		return fmt.Errorf("successful build result contains failure_stage")
	}
	if err := result.Target.Validate(); err != nil {
		return fmt.Errorf("build result target: %w", err)
	}
	if requested := build.Builder.Target; requested != nil {
		if result.Target != *requested {
			return fmt.Errorf("build result target %+v differs from requested target %+v", result.Target, *requested)
		}
	} else {
		expected := types.ResolveBuildTarget(nil, result.StartCmd, result.ReadyCmd)
		if result.Target != expected {
			return fmt.Errorf("build result target %+v differs from auto target %+v", result.Target, expected)
		}
	}
	if build.Profile == types.ProfileBare && (result.StartCmd != "" || result.ReadyCmd != "") {
		return fmt.Errorf("bare build result contains start or ready commands")
	}
	mayInheritCommands := buildSourceMayInheritCommands(build)
	if build.StartCmd != "" && result.StartCmd != build.StartCmd {
		return fmt.Errorf("build result start command differs from explicit trigger command")
	}
	if build.ReadyCmd != "" && result.ReadyCmd != build.ReadyCmd {
		return fmt.Errorf("build result ready command differs from explicit trigger command")
	}
	if !mayInheritCommands && (result.StartCmd != build.StartCmd || result.ReadyCmd != build.ReadyCmd) {
		return fmt.Errorf("build result introduced commands without a Sandbox source")
	}
	// Registered options are not result-integrity constraints. A source-dependent
	// auto target may legitimately omit options it cannot represent.

	var ref string
	switch {
	case result.Target.Kind == types.BuildTargetImage:
		if result.StartCmd != "" || result.ReadyCmd != "" {
			return fmt.Errorf("image build result contains start or ready commands")
		}
		if result.ImageRef == "" || result.SandboxRef != "" || result.SnapshotRef != "" {
			return fmt.Errorf("image target requires only image_ref")
		}
		ref = result.ImageRef
	case result.Target.Kind == types.BuildTargetSandbox && !result.Target.Memory:
		if result.ImageRef != "" || result.SandboxRef == "" || result.SnapshotRef != "" {
			return fmt.Errorf("sandbox target with memory=false requires only sandbox_ref")
		}
		ref = result.SandboxRef
	case result.Target.Kind == types.BuildTargetSandbox && result.Target.Memory:
		if result.ImageRef != "" || result.SandboxRef != "" || result.SnapshotRef == "" {
			return fmt.Errorf("sandbox target with memory=true requires only snapshot_ref")
		}
		ref = result.SnapshotRef
	default:
		return fmt.Errorf("unsupported build result target %+v", result.Target)
	}
	if _, err := types.ParsePortableRef(ref); err != nil {
		return fmt.Errorf("build result artifact: %w", err)
	}
	return nil
}

func buildSourceMayInheritCommands(build *types.Build) bool {
	if build == nil || build.FromTemplate == "" {
		return false
	}
	template, err := types.ParseTemplateID(build.FromTemplate)
	if err != nil {
		// Source syntax was sealed before execution. A durable accepted result
		// must not become dependent on reparsing unrelated definition fields
		// during restart recovery; malformed state is handled by preparation.
		return true
	}
	return template.Kind == types.KindSbx || template.Kind == types.KindSnp
}
