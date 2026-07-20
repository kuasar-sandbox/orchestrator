package orch

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// persistBuildState commits a standalone Build directly or atomically advances
// a cluster Build together with its node-local Admission/resource record.
func (o *Orchestrator) persistBuildState(ctx context.Context, build *types.Build) error {
	workflow, err := o.st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindBuild, build.BuildID)
	if err != nil {
		return err
	}
	if workflow == nil {
		return o.st.PutBuild(ctx, build)
	}
	update := nodeexec.EventUpdate{State: string(build.Status), Reason: build.Reason}
	switch build.Status {
	case types.BuildReady:
		update.ArtifactRef = build.PersistID
	case types.BuildError:
	case types.BuildRegistered, types.BuildWaiting, types.BuildBuilding:
	default:
		return fmt.Errorf("invalid Build state %q", build.Status)
	}
	_, err = o.st.CommitClusterBuildState(ctx, build, update)
	return err
}

func (o *Orchestrator) perBuildResources() routesync.BuildResources {
	storage := int64(0)
	if info, err := os.Stat(o.cfg.Builder.DiffTemplate); err == nil {
		storage = info.Size()
	}
	return routesync.BuildResources{
		CPU: o.cfg.Builder.VCPU * 1000, Mem: int64(o.cfg.Builder.MemoryMiB()) << 20,
		Storage: storage,
	}
}

func (o *Orchestrator) ClusterNodeInfo() (capacity int, buildCapacity *routesync.BuildResources, runtimeDigest string) {
	perBuild := o.perBuildResources()
	maximum := max(o.cfg.Builder.MaxConcurrent, 1)
	buildCapacity = &routesync.BuildResources{
		Slots: maximum, CPU: perBuild.CPU * maximum,
		Mem: perBuild.Mem * int64(maximum), Storage: perBuild.Storage * int64(maximum),
	}
	if digest, err := sha256File(o.runtimeFileFor(types.ProfileE2B)); err == nil {
		runtimeDigest = digest
	}
	return o.cfg.Sandbox.Capacity, buildCapacity, runtimeDigest
}

func (o *Orchestrator) SetClusterContext(ctx context.Context) { o.clusterCtx = ctx }

func (o *Orchestrator) asyncCtx() context.Context {
	if o.clusterCtx != nil {
		return o.clusterCtx
	}
	return context.Background()
}

func (o *Orchestrator) validateClusterCommandBinding(
	ctx context.Context,
	command *routesync.Command,
	kind clusterstate.ExecutionKind,
	objectID string,
) error {
	identity, err := o.st.GetClusterIdentity(ctx)
	if err != nil {
		return fmt.Errorf("cluster command requires an enrolled local identity: %w", err)
	}
	return validateClusterCommandBinding(command, kind, objectID, identity.NodeID)
}

func validateClusterCommandBinding(
	command *routesync.Command,
	kind clusterstate.ExecutionKind,
	objectID string,
	expectedNodeID string,
) error {
	if command == nil || command.Binding == "" || expectedNodeID == "" || command.NodeEpoch == 0 ||
		command.SessionSeq == 0 || command.RegistryGeneration == "" || command.BindingDigest == "" ||
		command.DemandDigest == "" || command.DispatchSpecDigest == "" {
		return errors.New("cluster command has an incomplete execution Binding fence")
	}
	binding, err := clusterstate.DecodeExecutionBinding(command.Binding)
	if err != nil {
		return err
	}
	if binding.Kind != kind || binding.ObjectID != objectID || binding.NodeID != expectedNodeID ||
		binding.NodeEpoch != command.NodeEpoch || binding.RegistryGeneration != command.RegistryGeneration {
		return errors.New("cluster command execution Binding identity mismatch")
	}
	digest, err := clusterstate.ExecutionBindingDigest(command.Binding)
	if err != nil {
		return err
	}
	if digest != command.BindingDigest || hex.EncodeToString(binding.DemandDigest[:]) != command.DemandDigest ||
		hex.EncodeToString(binding.DispatchSpecDigest[:]) != command.DispatchSpecDigest {
		return errors.New("cluster command execution Binding digest mismatch")
	}
	return nil
}

var errWrongExecutionBinding = errors.New("cluster: wrong execution Binding")

func (o *Orchestrator) verifySandboxCommandBinding(ctx context.Context, command *routesync.Command) error {
	if command == nil || command.SID == "" || command.NodeEpoch == 0 ||
		command.RegistryGeneration == "" || command.BindingDigest == "" {
		return fmt.Errorf("%w: incomplete Sandbox command fence", errWrongExecutionBinding)
	}
	sandbox, err := o.st.Get(ctx, command.SID)
	if err != nil {
		return err
	}
	if sandbox == nil {
		return fmt.Errorf("%w: Sandbox %q is absent", errWrongExecutionBinding, command.SID)
	}
	binding, opaque, err := clusterstate.ExecutionBindingFromMetadata(sandbox.Metadata)
	if err != nil {
		return fmt.Errorf("%w: %v", errWrongExecutionBinding, err)
	}
	digest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		return fmt.Errorf("%w: %v", errWrongExecutionBinding, err)
	}
	if binding.Kind != clusterstate.ExecutionKindSandbox || binding.ObjectID != command.SID ||
		binding.NodeEpoch != command.NodeEpoch || binding.RegistryGeneration != command.RegistryGeneration ||
		digest != command.BindingDigest {
		return errWrongExecutionBinding
	}
	return nil
}

func (o *Orchestrator) rebindClusterExecution(ctx context.Context, command *routesync.Command) error {
	if command == nil || command.OldBindingDigest == "" {
		return errors.New("cluster rebind requires the old Binding digest")
	}
	var kind clusterstate.ExecutionKind
	var objectID string
	switch {
	case command.SID != "" && command.BuildID == "":
		kind, objectID = clusterstate.ExecutionKindSandbox, command.SID
	case command.BuildID != "" && command.SID == "":
		kind, objectID = clusterstate.ExecutionKindBuild, command.BuildID
	default:
		return errors.New("cluster rebind requires exactly one Sandbox or Build ID")
	}
	if err := o.validateClusterCommandBinding(ctx, command, kind, objectID); err != nil {
		return fmt.Errorf("%w: %v", errWrongExecutionBinding, err)
	}
	changed, err := o.st.CASExecutionBinding(ctx, kind, objectID, command.OldBindingDigest, command.Binding)
	if err != nil {
		return err
	}
	if !changed {
		return errWrongExecutionBinding
	}
	if kind == clusterstate.ExecutionKindSandbox {
		sandbox, err := o.st.Get(ctx, objectID)
		if err != nil {
			return err
		}
		if sandbox == nil {
			return errWrongExecutionBinding
		}
		o.cache(sandbox)
		o.publishUpsert(sandbox)
	}
	return nil
}
