package orch

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/uuid"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/conductorext"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// SetExtensionHooks freezes the optional capabilities discovered after the one
// Extension.Start call. It is invoked before reconciliation, pools, node-link,
// or listeners can enter lifecycle methods.
func (o *Orchestrator) SetExtensionHooks(sandbox conductorextension.SandboxHook, build conductorextension.BuildHook) {
	o.extensionSandboxHook = sandbox
	o.extensionBuildHook = build
}

func (o *Orchestrator) callSandboxHook(ctx context.Context, operation *conductorextension.SandboxOperation) error {
	if o.extensionSandboxHook == nil {
		return nil
	}
	if err := o.extensionSandboxHook.PrepareSandbox(ctx, operation); err != nil {
		o.log.Warn("conductor extension sandbox hook",
			"operation_id", operation.ID, "kind", operation.Kind,
			"origin", operation.Origin, "sid", operation.SandboxID, "err", err)
		if errors.Is(err, conductorextension.ErrRejected) {
			return conductorextension.ErrRejected
		}
		return api.ErrExtensionUnavailable
	}
	return nil
}

func (o *Orchestrator) callBuildHook(ctx context.Context, operation *conductorextension.BuildOperation) error {
	if o.extensionBuildHook == nil {
		return nil
	}
	if err := o.extensionBuildHook.PrepareBuild(ctx, operation); err != nil {
		o.log.Warn("conductor extension build hook",
			"operation_id", operation.ID, "kind", operation.Kind,
			"origin", operation.Origin, "build_id", operation.BuildID, "err", err)
		if errors.Is(err, conductorextension.ErrRejected) {
			return conductorextension.ErrRejected
		}
		return api.ErrExtensionUnavailable
	}
	return nil
}

// prepareSandboxCreateHook is the single Hook boundary for both direct and
// canonical cluster Create. Callers perform their path-specific final metadata
// normalization and ownership checks after this common envelope validation.
func (o *Orchestrator) prepareSandboxCreateHook(
	ctx context.Context,
	origin conductorextension.SandboxOperationOrigin,
	sandboxID string,
	request *conductorextension.SandboxCreateRequest,
) (*conductorextension.SandboxCreateRequest, error) {
	operation := newSandboxOperation(conductorextension.SandboxOperationCreate, origin, sandboxID, nil)
	operation.Create = cloneSandboxCreateRequest(request)
	operationID := operation.ID
	if err := o.callSandboxHook(ctx, operation); err != nil {
		return nil, err
	}
	if err := validateSandboxOperationEnvelope(operation, operationID, conductorextension.SandboxOperationCreate, origin, sandboxID); err != nil {
		return nil, err
	}
	candidate := cloneSandboxCreateRequest(operation.Create)
	if candidate == nil {
		return nil, fmt.Errorf("%w: extension removed sandbox create candidate", api.ErrBadRequest)
	}
	if request == nil || candidate.Profile != request.Profile {
		return nil, fmt.Errorf("%w: extension changed core-owned sandbox profile", api.ErrBadRequest)
	}
	return candidate, nil
}

func newSandboxOperation(kind conductorextension.SandboxOperationKind, origin conductorextension.SandboxOperationOrigin, sandboxID string, current *types.Sandbox) *conductorextension.SandboxOperation {
	operation := &conductorextension.SandboxOperation{
		ID: uuid.NewString(), Kind: kind, Origin: origin, SandboxID: sandboxID,
	}
	if current != nil {
		view := conductorext.ProjectSandbox(current)
		operation.Current = &view
	}
	return operation
}

func newBuildOperation(kind conductorextension.BuildOperationKind, origin conductorextension.BuildOperationOrigin, buildID string, current *types.Build) *conductorextension.BuildOperation {
	operation := &conductorextension.BuildOperation{
		ID: uuid.NewString(), Kind: kind, Origin: origin, BuildID: buildID,
	}
	if current != nil {
		view := conductorext.ProjectBuild(current)
		operation.Current = &view
	}
	return operation
}

func validateSandboxOperationEnvelope(operation *conductorextension.SandboxOperation, id string, kind conductorextension.SandboxOperationKind, origin conductorextension.SandboxOperationOrigin, sandboxID string) error {
	if operation == nil || operation.ID != id || operation.Kind != kind || operation.Origin != origin || operation.SandboxID != sandboxID {
		return fmt.Errorf("%w: extension changed core-owned sandbox operation identity", api.ErrBadRequest)
	}
	present := 0
	for _, candidate := range []bool{operation.Create != nil, operation.Pause != nil, operation.Resume != nil, operation.Delete != nil} {
		if candidate {
			present++
		}
	}
	if present != 1 {
		return fmt.Errorf("%w: sandbox operation must retain exactly one request candidate", api.ErrBadRequest)
	}
	return nil
}

func validateBuildOperationEnvelope(operation *conductorextension.BuildOperation, id string, kind conductorextension.BuildOperationKind, origin conductorextension.BuildOperationOrigin, buildID string) error {
	if operation == nil || operation.ID != id || operation.Kind != kind || operation.Origin != origin || operation.BuildID != buildID {
		return fmt.Errorf("%w: extension changed core-owned build operation identity", api.ErrBadRequest)
	}
	present := 0
	for _, candidate := range []bool{operation.Register != nil, operation.Trigger != nil} {
		if candidate {
			present++
		}
	}
	if present != 1 {
		return fmt.Errorf("%w: build operation must retain exactly one request candidate", api.ErrBadRequest)
	}
	return nil
}

func sandboxPrecondition(sandbox *types.Sandbox) *types.Sandbox {
	return cloneSandbox(sandbox)
}

func sandboxPreconditionMatches(expected, current *types.Sandbox) bool {
	return reflect.DeepEqual(expected, current)
}

func buildPrecondition(build *types.Build) *types.Build {
	return cloneBuildForObservation(build)
}

func buildPreconditionMatches(expected, current *types.Build) bool {
	return reflect.DeepEqual(expected, current)
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneSandboxCreateRequest(request *conductorextension.SandboxCreateRequest) *conductorextension.SandboxCreateRequest {
	if request == nil {
		return nil
	}
	out := *request
	out.Metadata = cloneStringMap(request.Metadata)
	out.Env = cloneStringMap(request.Env)
	out.MMDS = cloneString(request.MMDS)
	out.AutoPauseMemory = cloneBool(request.AutoPauseMemory)
	return &out
}

func cloneSandboxPauseRequest(request *conductorextension.SandboxPauseRequest) *conductorextension.SandboxPauseRequest {
	if request == nil {
		return nil
	}
	return &conductorextension.SandboxPauseRequest{
		CaptureKind:          request.CaptureKind,
		CheckpointMergeRef:   cloneBool(request.CheckpointMergeRef),
		CheckpointDropCaches: cloneBool(request.CheckpointDropCaches),
	}
}

func cloneSandboxResumeRequest(request *conductorextension.SandboxResumeRequest) *conductorextension.SandboxResumeRequest {
	if request == nil {
		return nil
	}
	return &conductorextension.SandboxResumeRequest{
		RequestedDeadlineUnix: cloneInt64(request.RequestedDeadlineUnix),
		Mode:                  request.Mode, Trigger: request.Trigger,
	}
}

func cloneSandboxDeleteRequest(request *conductorextension.SandboxDeleteRequest) *conductorextension.SandboxDeleteRequest {
	if request == nil {
		return nil
	}
	out := *request
	return &out
}

func cloneBuildResources(resources types.BuildResources) conductorextension.BuildResources {
	return conductorextension.BuildResources{CPU: resources.CPU, Memory: resources.Memory, Storage: resources.Storage}
}

func internalBuildResources(resources conductorextension.BuildResources) types.BuildResources {
	return types.BuildResources{CPU: resources.CPU, Memory: resources.Memory, Storage: resources.Storage}
}

func clonePublicBuildResources(resources *conductorextension.BuildResources) *conductorextension.BuildResources {
	if resources == nil {
		return nil
	}
	copy := *resources
	return &copy
}

func publicBuildOptions(options types.BuildOptions) conductorextension.BuildOptions {
	out := conductorextension.BuildOptions{Resources: clonePublicBuildResources(nil)}
	if options.Target != nil {
		out.Target = &conductorextension.BuildTarget{
			Kind: conductorextension.BuildTargetKind(options.Target.Kind), Memory: options.Target.Memory,
		}
	}
	if options.Resources != nil {
		resources := cloneBuildResources(*options.Resources)
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
			out.Registry.TLS = &conductorextension.BuildRegistryTLSOptions{
				CABundlePEM:        options.Registry.TLS.CABundlePEM,
				InsecureSkipVerify: options.Registry.TLS.InsecureSkipVerify,
			}
		}
	}
	return out
}

func internalBuildOptions(options conductorextension.BuildOptions) types.BuildOptions {
	out := types.BuildOptions{}
	if options.Target != nil {
		out.Target = &types.BuildTarget{
			Kind: types.BuildTargetKind(options.Target.Kind), Memory: options.Target.Memory,
		}
	}
	if options.Resources != nil {
		resources := internalBuildResources(*options.Resources)
		out.Resources = &resources
	}
	if options.Referer != nil {
		out.Referer = &types.BuildRefererOptions{
			Enabled: cloneBool(options.Referer.Enabled), Writeback: cloneBool(options.Referer.Writeback),
		}
	}
	if options.Registry != nil {
		out.Registry = &types.BuildRegistryOptions{}
		if options.Registry.TLS != nil {
			out.Registry.TLS = &types.BuildRegistryTLSOptions{
				CABundlePEM:        options.Registry.TLS.CABundlePEM,
				InsecureSkipVerify: options.Registry.TLS.InsecureSkipVerify,
			}
		}
	}
	return out
}

func clonePublicBuildOptions(options conductorextension.BuildOptions) conductorextension.BuildOptions {
	return publicBuildOptions(internalBuildOptions(options))
}

func publicBuildSteps(steps []types.TemplateStep) []conductorextension.BuildStep {
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

func internalBuildSteps(steps []conductorextension.BuildStep) []types.TemplateStep {
	if steps == nil {
		return nil
	}
	out := make([]types.TemplateStep, len(steps))
	for index, step := range steps {
		out[index] = types.TemplateStep{
			Type: step.Type, Args: append([]string(nil), step.Args...), FilesHash: step.FilesHash, Force: step.Force,
		}
	}
	return out
}

func cloneBuildRegisterRequest(request *conductorextension.BuildRegisterRequest) *conductorextension.BuildRegisterRequest {
	if request == nil {
		return nil
	}
	out := *request
	out.Names = append([]string(nil), request.Names...)
	out.Aliases = append([]string(nil), request.Aliases...)
	out.Metadata = cloneStringMap(request.Metadata)
	out.Env = cloneStringMap(request.Env)
	out.Builder = clonePublicBuildOptions(request.Builder)
	return &out
}

func cloneBuildResourcePatch(patch conductorextension.BuildResourcePatch) conductorextension.BuildResourcePatch {
	return conductorextension.BuildResourcePatch{
		CPU: cloneInt64(patch.CPU), Memory: cloneInt64(patch.Memory), Storage: cloneInt64(patch.Storage),
	}
}

func cloneBuildTriggerRequest(request *conductorextension.BuildTriggerRequest) *conductorextension.BuildTriggerRequest {
	if request == nil {
		return nil
	}
	out := *request
	out.Steps = make([]conductorextension.BuildStep, len(request.Steps))
	copy(out.Steps, request.Steps)
	for index := range out.Steps {
		out.Steps[index].Args = append([]string(nil), request.Steps[index].Args...)
	}
	out.ResourceAssertion = cloneBuildResourcePatch(request.ResourceAssertion)
	return &out
}

func publicBuildResourcePatch(patch buildcfg.ResourcePatch) conductorextension.BuildResourcePatch {
	return conductorextension.BuildResourcePatch{
		CPU: cloneInt64(patch.CPU), Memory: cloneInt64(patch.Memory), Storage: cloneInt64(patch.Storage),
	}
}

func internalBuildResourcePatch(patch conductorextension.BuildResourcePatch) buildcfg.ResourcePatch {
	return buildcfg.ResourcePatch{
		CPU: cloneInt64(patch.CPU), Memory: cloneInt64(patch.Memory), Storage: cloneInt64(patch.Storage),
	}
}

func buildTriggerRequest(spec api.TriggerSpec) *conductorextension.BuildTriggerRequest {
	return &conductorextension.BuildTriggerRequest{
		FromImage: spec.FromImage, FromTemplate: spec.FromTemplate,
		Steps: publicBuildSteps(spec.Steps), StartCommand: spec.StartCmd, ReadyCommand: spec.ReadyCmd,
		ResourceAssertion: publicBuildResourcePatch(spec.ResourceAssertion),
	}
}

func internalBuildTriggerRequest(request *conductorextension.BuildTriggerRequest) api.TriggerSpec {
	return api.TriggerSpec{
		FromImage: request.FromImage, FromTemplate: request.FromTemplate,
		Steps: internalBuildSteps(request.Steps), StartCmd: request.StartCommand, ReadyCmd: request.ReadyCommand,
		ResourceAssertion: internalBuildResourcePatch(request.ResourceAssertion),
	}
}
