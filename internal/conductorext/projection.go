package conductorext

import (
	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func projectSandbox(sandbox *types.Sandbox) conductorextension.SandboxView {
	if sandbox == nil {
		return conductorextension.SandboxView{}
	}
	view := conductorextension.SandboxView{
		ID: sandbox.ID, StableID: sandbox.StableID(), Profile: conductorextension.Profile(sandbox.Profile),
		TemplateID: sandbox.TemplateID, State: conductorextension.SandboxState(sandbox.State), RunID: sandbox.RunID,
		CreatedUnix: sandbox.CreatedUnix, DeadlineUnix: sandbox.DeadlineUnix, Metadata: cloneMap(sandbox.Metadata),
		RunDir: sandbox.RunDir, BaseDir: sandbox.BaseDir, EnvdUDS: sandbox.EnvdUDS, CIUDS: sandbox.CiUDS,
		FloatingIP: sandbox.FloatingIP, InnerIP: sandbox.InnerIP, VSwitchPort: sandbox.VswitchPort,
		PortMAC:          sandbox.PortMAC,
		ResumeSourceKind: conductorextension.ResumeSourceKind(sandbox.ResumeSource.Kind),
		ResumeSourceRef:  sandbox.ResumeSource.Ref,
		ArtifactLocation: artifactLocation(sandbox.ResumeSource.Ref),
		AutoPauseMemory:  sandbox.AutoPauseMemory,
		LaunchMode:       conductorextension.LaunchMode(sandbox.LaunchMode),
	}
	if sandbox.Cluster != nil {
		view.Cluster = &conductorextension.SandboxClusterView{Group: sandbox.Cluster.Group, RouteKey: sandbox.Cluster.RouteKey}
	}
	view.APISecretFingerprint, _ = store.APISecretHash(sandbox.APISecret)
	view.ManifestKeyFingerprint, _ = store.ManifestKeyHash(sandbox.ManifestKey)
	return view
}

// ProjectSandbox returns the same deep-copied public projection used by Get and
// Watch. Lifecycle Hook operations use it so Current never aliases core state.
func ProjectSandbox(sandbox *types.Sandbox) conductorextension.SandboxView {
	return projectSandbox(sandbox)
}

func artifactLocation(ref string) conductorextension.ArtifactLocation {
	switch {
	case ref == "":
		return conductorextension.ArtifactLocationNone
	case types.IsPortableRef(ref):
		return conductorextension.ArtifactLocationRemote
	default:
		return conductorextension.ArtifactLocationLocal
	}
}

func projectBuild(build *types.Build) conductorextension.BuildView {
	if build == nil {
		return conductorextension.BuildView{}
	}
	view := conductorextension.BuildView{
		BuildID: build.BuildID, TemplateID: build.TemplateID, PersistID: build.PersistID,
		Profile: conductorextension.Profile(build.Profile), Kind: conductorextension.BuildKind(build.Kind),
		State: conductorextension.BuildState(build.Status), Reason: build.Reason,
		CreatedUnix: build.CreatedUnix, WaitingUnix: build.WaitingUnix, ExecutionClaimedUnix: build.ExecutionClaimedUnix,
		Names: append([]string(nil), build.Names...), Aliases: append([]string(nil), build.Aliases...),
		FromImage: build.FromImage, FromTemplate: build.FromTemplate,
		Resources: conductorextension.BuildResources{CPU: build.Resources.CPU, Memory: build.Resources.Memory, Storage: build.Resources.Storage},
		Steps:     projectSteps(build.Steps), StartCommand: build.StartCmd, ReadyCommand: build.ReadyCmd,
		Metadata: cloneMap(build.Metadata), Builder: projectBuildOptions(build.Builder),
		PhaseResourcePatch: build.PhaseResourcePatch, RunID: build.RunID, ExecutionClaimed: build.ExecutionClaimed,
		EnforcementStatus: build.EnforcementStatus, Phase: build.Phase, PhaseSandboxID: build.PhaseSandboxID,
		RuntimeVSwitchPort: build.RuntimeVswitchPort, RuntimeFloatingIP: build.RuntimeFloatingIP,
		RuntimePortMAC: build.RuntimePortMAC, ClusterGroup: build.ClusterGroup,
	}
	view.APISecretFingerprint, _ = store.APISecretHash(build.APISecret)
	view.ManifestKeyFingerprint, _ = store.ManifestKeyHash(build.ManifestKey)
	return view
}

// ProjectBuild returns the same deep-copied public projection used by Get and
// Watch. Lifecycle Hook operations use it so Current never aliases core state.
func ProjectBuild(build *types.Build) conductorextension.BuildView {
	return projectBuild(build)
}

func projectSteps(steps []types.TemplateStep) []conductorextension.BuildStep {
	if steps == nil {
		return nil
	}
	out := make([]conductorextension.BuildStep, len(steps))
	for index, step := range steps {
		out[index] = conductorextension.BuildStep{
			Type: step.Type, Args: append([]string(nil), step.Args...), FilesHash: step.FilesHash, Force: step.Force,
		}
	}
	return out
}

func projectBuildOptions(options types.BuildOptions) conductorextension.BuildOptions {
	out := conductorextension.BuildOptions{}
	if options.Resources != nil {
		out.Resources = &conductorextension.BuildResources{
			CPU: options.Resources.CPU, Memory: options.Resources.Memory, Storage: options.Resources.Storage,
		}
	}
	if options.Referer != nil {
		out.Referer = &conductorextension.BuildRefererOptions{
			Enabled: cloneBool(options.Referer.Enabled), Writeback: cloneBool(options.Referer.Writeback),
		}
	}
	if options.Registry != nil {
		out.Registry = &conductorextension.BuildRegistryOptions{}
		if options.Registry.TLS != nil {
			out.Registry.TLS = &conductorextension.BuildRegistryTLSOptions{
				CABundlePEM: options.Registry.TLS.CABundlePEM, InsecureSkipVerify: options.Registry.TLS.InsecureSkipVerify,
			}
		}
	}
	return out
}

func cloneSandboxView(view conductorextension.SandboxView) conductorextension.SandboxView {
	view.Metadata = cloneMap(view.Metadata)
	if view.Cluster != nil {
		cluster := *view.Cluster
		view.Cluster = &cluster
	}
	return view
}

func cloneBuildView(view conductorextension.BuildView) conductorextension.BuildView {
	view.Names = append([]string(nil), view.Names...)
	view.Aliases = append([]string(nil), view.Aliases...)
	view.Metadata = cloneMap(view.Metadata)
	if view.Steps != nil {
		steps := make([]conductorextension.BuildStep, len(view.Steps))
		for index, step := range view.Steps {
			steps[index] = step
			steps[index].Args = append([]string(nil), step.Args...)
		}
		view.Steps = steps
	}
	view.Builder = cloneBuildOptions(view.Builder)
	return view
}

func cloneBuildOptions(options conductorextension.BuildOptions) conductorextension.BuildOptions {
	out := conductorextension.BuildOptions{}
	if options.Resources != nil {
		resources := *options.Resources
		out.Resources = &resources
	}
	if options.Referer != nil {
		out.Referer = &conductorextension.BuildRefererOptions{
			Enabled: cloneBool(options.Referer.Enabled), Writeback: cloneBool(options.Referer.Writeback),
		}
	}
	if options.Registry != nil {
		out.Registry = &conductorextension.BuildRegistryOptions{}
		if options.Registry.TLS != nil {
			tls := *options.Registry.TLS
			out.Registry.TLS = &tls
		}
	}
	return out
}

func cloneMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}
