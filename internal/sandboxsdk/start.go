package sandboxsdk

import (
	"context"

	runtimesdk "github.com/kuasar-sandbox/sandboxer/pkg/runtime"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

// startSpec keeps the complete SDK LaunchSpec returned by source.Apply. Rebuilding
// it through RunOptions would lose source-scoped access and SDK-owned provenance.
// This adapter contributes only the task's host settings and output routing.
func startSpec(ctx context.Context, opts sandbox.RunOptions, launch sandbox.LaunchSpec) (runtimesdk.SandboxSpec, error) {
	if launch.SourceBinding == nil {
		if err := opts.Cfg.ValidateCold(); err != nil {
			return runtimesdk.SandboxSpec{}, err
		}
	}
	launch.Stdio = opts.StdioMode
	launch.Forwards = opts.Forwards
	shape, err := sandbox.DeriveRuntimeSpecWithOptions(ctx, opts.Cfg, sandbox.DeriveRuntimeOptions{
		Launch: launch, EnableCheckpoint: opts.EnableCheckpoint,
		BaseRoot: opts.BaseRoot, SandboxID: opts.SandboxID, PathID: opts.PathID,
	})
	if err != nil {
		return runtimesdk.SandboxSpec{}, err
	}
	shape.CHBinary, shape.RuntimeRoot = opts.CHBinary, opts.RuntimeRoot
	shape.Console = opts.StdioMode.Console
	shape.PingFatalThreshold, shape.StatsInterval = opts.PingFatalThreshold, opts.StatsInterval
	shape.NotifyReadiness = opts.NotifyReadiness
	return runtimesdk.SandboxSpec{Runtime: shape, Launch: launch}, nil
}
