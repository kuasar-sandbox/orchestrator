package orch

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// NodeKeyMaterialResolver resolves provider references on the node side of the
// authenticated node-link. Inline delivery does not use this interface.
type NodeKeyMaterialResolver interface {
	ResolveKey(ctx context.Context, ref string) (string, error)
	ResolveRegistryAuth(ctx context.Context, ref string) (string, error)
}

func (o *Orchestrator) SetNodeKeyMaterialResolver(resolver NodeKeyMaterialResolver) {
	o.keyResolver = resolver
}

// This file makes the orchestrator the node side of the cluster node-link
// (nodelink.Node): it executes the registry's lifecycle commands and reports the
// terminal state by sandbox/build ID. Cluster routing metadata is stored opaquely
// on the object; the nodelink owner resolves it from its per-node ownership table.

// HandleCommand executes a registry node-link command and returns a receipt ack
// (cluster.md): accepted once the synchronous preconditions hold (key
// installed, template valid), rejected otherwise. Slow work (a boot/resume/
// teardown) runs asynchronously so the ack is prompt and the node-link reader
// isn't blocked; the terminal sandbox state is reported on the route stream,
// which a Reserve waits on.
func (o *Orchestrator) HandleCommand(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	switch cmd.Kind {
	case routesync.CmdCreate:
		manifestKey, tmpl, err := o.precheckCluster(ctx, cmd)
		if err != nil {
			o.log.Warn("cluster create rejected", "sid", cmd.SID, "err", err)
			return reject(cmd, err)
		}
		if err := o.claimClusterCreate(ctx, cmd.SID); err != nil {
			o.log.Warn("cluster create rejected", "sid", cmd.SID, "err", err)
			return reject(cmd, err)
		}
		go func() {
			defer o.releaseClusterCreate(cmd.SID)
			if _, err := o.bootCluster(o.asyncCtx(), cmd, manifestKey, tmpl); err != nil {
				o.log.Error("cluster create", "sid", cmd.SID, "err", err)
			}
		}()
		return accept(cmd)
	case routesync.CmdConnect:
		go func() {
			if err := o.connectCluster(o.asyncCtx(), cmd.SID); err != nil {
				o.log.Error("cluster connect", "sid", cmd.SID, "err", err)
			}
		}()
		return accept(cmd)
	case routesync.CmdDelete:
		go func() {
			if err := o.deleteCluster(o.asyncCtx(), cmd.SID); err != nil {
				o.log.Error("cluster delete", "sid", cmd.SID, "err", err)
			}
		}()
		return accept(cmd)
	case routesync.CmdSandboxAdmitDispatch, routesync.CmdBuildAdmitDispatch,
		routesync.CmdSandboxResume, routesync.CmdSandboxDelete,
		routesync.CmdRebindExecution, routesync.CmdFinalizeWorkflow:
		return reject(cmd, errors.New("final cluster workflow executor is dormant until atomic cutover"))
	case routesync.CmdKeyPut:
		if cmd.KeyLease != nil {
			ref, err := o.putClusterKeyLease(ctx, *cmd.KeyLease)
			if err != nil {
				return reject(cmd, err)
			}
			ack := accept(cmd)
			ack.KeyLeaseRef = &ref
			return ack
		}
		// Pre-cutover manifest-only transport. Phase 5 deletes this branch.
		if cmd.ManifestKeyType == "ref" || cmd.ManifestKeyRef != "" {
			return reject(cmd, fmt.Errorf("manifest_key ref delivery is not configured"))
		}
		if cmd.ManifestKey != "" {
			var ttl int64
			if cmd.ExpiresUnix > 0 {
				if ttl = cmd.ExpiresUnix - time.Now().Unix(); ttl <= 0 {
					ttl = 1
				}
			}
			if _, err := o.st.AddManifestKey(ctx, cmd.ManifestKey, "cluster", ttl, ""); err != nil {
				return reject(cmd, err)
			}
		}
		return accept(cmd)
	case routesync.CmdKeyDrop:
		if cmd.KeyLeaseRef != nil {
			if err := cmd.KeyLeaseRef.Validate(); err != nil {
				return reject(cmd, err)
			}
			if _, err := o.st.DropKeyLeaseRef(
				ctx,
				cmd.KeyLeaseRef.Group,
				cmd.KeyLeaseRef.AuthKeyFingerprint,
				cmd.KeyLeaseRef.ManifestKeyFingerprint,
				cmd.KeyLeaseRef.KeyRevision,
				cmd.KeyLeaseRef.RegistryAuthDigest,
			); err != nil {
				return reject(cmd, err)
			}
			ack := accept(cmd)
			ref := *cmd.KeyLeaseRef
			ack.KeyLeaseRef = &ref
			return ack
		}
		// Pre-cutover manifest-only transport. Phase 5 deletes this branch.
		if err := o.dropClusterKey(ctx, cmd.KeyFingerprint); err != nil {
			return reject(cmd, err)
		}
		return accept(cmd)
	case routesync.CmdBuildRegister:
		// Pre-provision a registry-assigned build (cluster.md): create the build
		// record with the registry's ids + resolved key, stash the image-pull creds
		// for this build, and report `registered` up. The e2b trigger (router-
		// forwarded) then runs it; state flows back as build events.
		if err := o.registerClusterBuild(ctx, cmd); err != nil {
			return reject(cmd, err)
		}
		return accept(cmd)
	default:
		return reject(cmd, fmt.Errorf("unhandled command kind %q", cmd.Kind))
	}
}

func (o *Orchestrator) claimClusterCreate(ctx context.Context, sid string) error {
	if sid == "" {
		return fmt.Errorf("cluster create: sandbox id is required")
	}
	o.mu.Lock()
	if o.reg[sid] != nil {
		o.mu.Unlock()
		return fmt.Errorf("cluster create: sandbox %q already exists", sid)
	}
	if _, claimed := o.clusterCreates[sid]; claimed {
		o.mu.Unlock()
		return fmt.Errorf("cluster create: sandbox %q is already being created", sid)
	}
	o.clusterCreates[sid] = struct{}{}
	o.mu.Unlock()

	existing, err := o.st.Get(ctx, sid)
	if err != nil {
		o.releaseClusterCreate(sid)
		return err
	}
	if existing != nil {
		o.releaseClusterCreate(sid)
		return fmt.Errorf("cluster create: sandbox %q already exists", sid)
	}
	return nil
}

func (o *Orchestrator) releaseClusterCreate(sid string) {
	o.mu.Lock()
	delete(o.clusterCreates, sid)
	o.mu.Unlock()
}

// ResourceProbe surfaces the node's water level for the cluster heartbeat. serve
// sets it (an adapter over the resource controller) when resource_listen is on;
// nil = no controller (static cgroup), then zone/water are reported as their
// zero-load defaults and only the sandbox count + build alloc carry signal.
type ResourceProbe interface {
	Zone() string          // green | yellow | red | critical
	AllocatedBytes() int64 // memory currently reserved
	PoolBytes() int64      // allocatable memory pool
	Draining() bool        // node-side drain set (node-resource.md §2.5)
}

// SetResourceProbe wires the node water-level source for the cluster heartbeat.
func (o *Orchestrator) SetResourceProbe(p ResourceProbe) { o.probe = p }

// registerClusterBuild pre-provisions a build the registry assigned + placed here
// (cluster.md): resolve the tenant key by the predistributed fingerprint,
// create the build record under the registry's ids, stash the transient image-pull
// creds, and report `registered` up the node-link.
func (o *Orchestrator) registerClusterBuild(ctx context.Context, cmd *routesync.Command) error {
	if cmd.BuildID == "" || cmd.TemplateRef == "" {
		return fmt.Errorf("build_register: missing build_id / template_id")
	}
	profile, err := types.ParseProfile(cmd.Profile)
	if err != nil {
		return fmt.Errorf("build_register: %w", err)
	}
	manifestKey, err := o.resolveByFingerprint(ctx, cmd.KeyFingerprint)
	if err != nil {
		return err
	}
	commandMeta, err := o.clusterCommandMetadata(ctx, cmd, clusterstate.ExecutionKindBuild, cmd.BuildID)
	if err != nil {
		return err
	}
	meta, builderOpts, err := buildcfg.Extract(commandMeta)
	if err != nil {
		return err
	}
	if err := o.validateBuildOptions(builderOpts, false); err != nil {
		return err
	}
	existing, err := o.st.GetBuild(ctx, cmd.BuildID)
	if err != nil {
		return err
	}
	if existing != nil {
		if existing.TemplateID != cmd.TemplateRef || existing.Profile != profile || existing.ManifestKey != manifestKey {
			return fmt.Errorf("build_register: build %s conflicts with existing identity", cmd.BuildID)
		}
		if cmd.Binding != "" && existing.Metadata[clusterstate.ObjectMetadataKey] != cmd.Binding {
			return fmt.Errorf("build_register: build %s conflicts with existing execution binding", cmd.BuildID)
		}
		if existing.Status != types.BuildReady && existing.Status != types.BuildError {
			o.clusterBuildMu.Lock()
			o.clusterBuilds[cmd.BuildID] = &clusterBuild{imageRepo: cmd.ImageRepo, registryAuth: cmd.RegistryAuth}
			o.clusterBuildMu.Unlock()
		}
		return nil
	}
	b := &types.Build{
		BuildID:     cmd.BuildID,
		TemplateID:  cmd.TemplateRef,
		AuthKey:     manifestKey, // pre-cutover path used one root for both domains
		ManifestKey: manifestKey,
		CPUCount:    max(1, o.cfg.Builder.VCPU),
		MemoryMB:    max(1, o.cfg.Builder.MemoryMiB()),
		Profile:     profile,
		Kind:        types.KindImg,
		Status:      types.BuildRegistered,
		FromImage:   o.imageURIFromMask(cmd.TemplateRef, cmd.BuildID),
		Metadata:    meta,
		Builder:     builderOpts,
		CreatedUnix: time.Now().Unix(),
	}
	if err := o.st.PutBuild(ctx, b); err != nil {
		return err
	}
	o.clusterBuildMu.Lock()
	o.clusterBuilds[cmd.BuildID] = &clusterBuild{imageRepo: cmd.ImageRepo, registryAuth: cmd.RegistryAuth}
	o.clusterBuildMu.Unlock()
	o.publishBuildState(cmd.BuildID, "registered", "", "")
	return nil
}

// BuildEvents is the node-link client's source of build state transitions
// (nodelink.Node); the client streams them to the registry (§5.1).
func (o *Orchestrator) BuildEvents() <-chan *routesync.BuildEvent { return o.buildEvents }

// publishBuildState emits a build event for a cluster build (no-op for a non-
// cluster, e.g. single-node, build). Non-blocking: a full buffer drops the event
// (the registry reconverges from the next transition / the router's status).
func (o *Orchestrator) publishBuildState(buildID, state, templateID, reason string) {
	o.clusterBuildMu.Lock()
	cb := o.clusterBuilds[buildID]
	o.clusterBuildMu.Unlock()
	if cb == nil {
		return // not a cluster-driven build
	}
	ev := &routesync.BuildEvent{BuildID: buildID, State: state, TemplateID: templateID, Reason: reason}
	select {
	case o.buildEvents <- ev:
	default:
	}
	if state == "ready" || state == "error" {
		o.clusterBuildMu.Lock()
		delete(o.clusterBuilds, buildID) // terminal: drop the transient creds
		o.clusterBuildMu.Unlock()
	}
}

// clusterBuildCreds returns a cluster build's transient image-pull creds (registry
// auth) if it is registry-driven, so resolveBuildCreds uses them instead of the
// node's stored registry_auth_enc (cluster.md: creds are not persisted here).
func (o *Orchestrator) clusterBuildCreds(buildID string) (string, bool) {
	o.clusterBuildMu.Lock()
	defer o.clusterBuildMu.Unlock()
	cb := o.clusterBuilds[buildID]
	if cb == nil {
		return "", false
	}
	return cb.registryAuth, true
}

// Heartbeat reports the node's water level for the registry (nodelink.Node):
// sandbox count + (when resource_listen is on) zone/allocated/pool/draining, and
// the in-flight build resource alloc. The cluster placer (cluster-placer.md)
// excludes draining nodes, filters by zone, and ranks by the water level / count.
func (o *Orchestrator) Heartbeat() *routesync.Heartbeat {
	o.mu.Lock()
	count := len(o.reg)
	o.mu.Unlock()
	hb := &routesync.Heartbeat{Counts: count, Zone: string(nodectlZoneGreen), BuildAlloc: o.buildAlloc()}
	if p := o.probe; p != nil {
		hb.Zone = p.Zone()
		hb.Allocated = p.AllocatedBytes()
		hb.Pool = p.PoolBytes()
		hb.Draining = p.Draining()
	}
	return hb
}

// nodectlZoneGreen is the default zone reported when no resource controller is
// present (a static-cgroup node is never "hot" from the cluster's view).
const nodectlZoneGreen = "green"

// buildAlloc is the in-flight build resource usage: live builds × the per-build
// pool (builder vcpu/memory + diff_template scratch). Feeds resource-aware build
// placement (cluster-placer.md); nil when no builds are running.
func (o *Orchestrator) buildAlloc() *routesync.BuildResources {
	o.pendMu.Lock()
	n := int64(len(o.pend))
	o.pendMu.Unlock()
	if n == 0 {
		return nil
	}
	per := o.perBuildResources()
	return &routesync.BuildResources{CPU: per.CPU * int(n), Mem: per.Mem * n, Storage: per.Storage * n}
}

// perBuildResources is one build sandbox's resource footprint from builder config.
func (o *Orchestrator) perBuildResources() routesync.BuildResources {
	storage := int64(0)
	if fi, err := os.Stat(o.cfg.Builder.DiffTemplate); err == nil {
		storage = fi.Size()
	}
	return routesync.BuildResources{
		CPU:     o.cfg.Builder.VCPU * 1000, // milli-cores
		Mem:     int64(o.cfg.Builder.MemoryMiB()) << 20,
		Storage: storage,
	}
}

// ClusterNodeInfo builds the static node-register fields for node-link (cluster.md
// §5.1): max sandbox capacity (0 = unbounded), the build resource pool, and this
// node's guest runtime digest for placement runtime matching.
func (o *Orchestrator) ClusterNodeInfo() (capacity int, buildCap *routesync.BuildResources, runtimeDigest string) {
	per := o.perBuildResources()
	mc := o.cfg.Builder.MaxConcurrent
	if mc <= 0 {
		mc = 1
	}
	buildCap = &routesync.BuildResources{CPU: per.CPU * mc, Mem: per.Mem * int64(mc), Storage: per.Storage * int64(mc)}
	if dig, err := sha256File(o.runtimeFileFor(types.ProfileE2B)); err == nil {
		runtimeDigest = dig
	}
	return o.cfg.Sandbox.Capacity, buildCap, runtimeDigest
}

// SetClusterContext sets the lifetime for node-link async work (boots / resumes /
// teardowns): serve passes its shutdown context so in-flight work is cancelled on
// drain instead of leaking past it. Call before starting the node-link client.
func (o *Orchestrator) SetClusterContext(ctx context.Context) { o.clusterCtx = ctx }

func (o *Orchestrator) asyncCtx() context.Context {
	if o.clusterCtx != nil {
		return o.clusterCtx
	}
	return context.Background()
}

func accept(cmd *routesync.Command) *routesync.CmdAck {
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}
}

func (o *Orchestrator) putClusterKeyLease(ctx context.Context, wire routesync.NodeKeyLeaseV1) (routesync.NodeKeyLeaseRefV1, error) {
	ref, err := wire.Ref()
	if err != nil {
		return routesync.NodeKeyLeaseRefV1{}, err
	}
	if wire.ExpiresUnix <= time.Now().Unix() {
		return routesync.NodeKeyLeaseRefV1{}, errors.New("cluster: key lease is already expired")
	}
	authKey, err := o.resolveNodeKeyMaterial(ctx, wire.AuthKey)
	if err != nil {
		return routesync.NodeKeyLeaseRefV1{}, fmt.Errorf("cluster: resolve AuthKey: %w", err)
	}
	manifestKey, err := o.resolveNodeKeyMaterial(ctx, wire.ManifestKey)
	if err != nil {
		return routesync.NodeKeyLeaseRefV1{}, fmt.Errorf("cluster: resolve ManifestKey: %w", err)
	}
	registryAuth, err := o.resolveNodeRegistryAuth(ctx, wire.RegistryAuth)
	if err != nil {
		return routesync.NodeKeyLeaseRefV1{}, fmt.Errorf("cluster: resolve registry auth: %w", err)
	}
	if _, err := o.st.PutKeyLease(ctx, store.KeyLease{
		Group: wire.Group, AuthKey: authKey, ManifestKey: manifestKey,
		KeyRevision: wire.KeyRevision, RegistryAuth: registryAuth,
		RegistryAuthDigest: ref.RegistryAuthDigest,
		Label:              "cluster", ExpiresUnix: wire.ExpiresUnix,
	}); err != nil {
		return routesync.NodeKeyLeaseRefV1{}, err
	}
	return ref, nil
}

func (o *Orchestrator) resolveNodeKeyMaterial(ctx context.Context, material routesync.NodeKeyMaterialV1) (string, error) {
	switch material.Type {
	case routesync.KeyMaterialInline:
		return material.Value, nil
	case routesync.KeyMaterialRef:
		if o.keyResolver == nil {
			return "", errors.New("provider key reference resolver is not configured")
		}
		value, err := o.keyResolver.ResolveKey(ctx, material.Ref)
		if err != nil {
			return "", err
		}
		fingerprint, err := keyFingerprint("resolved key", value)
		if err != nil {
			return "", err
		}
		if fingerprint != material.Fingerprint {
			return "", errors.New("resolved key fingerprint mismatch")
		}
		return value, nil
	default:
		return "", errors.New("unsupported key material type")
	}
}

func (o *Orchestrator) resolveNodeRegistryAuth(ctx context.Context, auth routesync.NodeRegistryAuthV1) (string, error) {
	var value string
	switch auth.Type {
	case "":
		return "", nil
	case routesync.KeyMaterialInline:
		value = auth.Value
	case routesync.KeyMaterialRef:
		if o.keyResolver == nil {
			return "", errors.New("provider registry auth resolver is not configured")
		}
		resolved, err := o.keyResolver.ResolveRegistryAuth(ctx, auth.Ref)
		if err != nil {
			return "", err
		}
		value = resolved
	default:
		return "", errors.New("unsupported registry auth material type")
	}
	if err := regcreds.ValidateDockerAuth(value); err != nil {
		return "", fmt.Errorf("invalid registry auth: %w", err)
	}
	return value, nil
}

func reject(cmd *routesync.Command, err error) *routesync.CmdAck {
	return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, Reason: err.Error()}
}

func rejectBinding(cmd *routesync.Command, err error) *routesync.CmdAck {
	return &routesync.CmdAck{
		CmdID: cmd.CmdID, Status: routesync.AckRejected,
		Outcome: routesync.DispatchWrongBinding, Reason: err.Error(),
	}
}

// CreateCluster is the synchronous precheck + boot of a node-link create. The
// async path (HandleCommand) splits it so the ack is prompt; callers/tests that
// want the result synchronously use this.
func (o *Orchestrator) CreateCluster(ctx context.Context, cmd *routesync.Command) (*types.Sandbox, error) {
	manifestKey, tmpl, err := o.precheckCluster(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return o.bootCluster(ctx, cmd, manifestKey, tmpl)
}

// precheckCluster resolves the manifest key (by the fingerprint the registry
// predistributed) and the snapshot template — the fast, synchronous preconditions
// whose failure is a rejected ack (rather than a slow create that fails only by
// Reserve timeout).
func (o *Orchestrator) precheckCluster(ctx context.Context, cmd *routesync.Command) (string, types.TemplateID, error) {
	if _, err := o.clusterCommandMetadata(ctx, cmd, clusterstate.ExecutionKindSandbox, cmd.SID); err != nil {
		return "", types.TemplateID{}, err
	}
	manifestKey, err := o.resolveByFingerprint(ctx, cmd.KeyFingerprint)
	if err != nil {
		return "", types.TemplateID{}, err
	}
	tmpl, err := types.ParseTemplateID(cmd.TemplateRef)
	if err != nil {
		return "", types.TemplateID{}, fmt.Errorf("cluster create: template %q: %w", cmd.TemplateRef, err)
	}
	return manifestKey, tmpl, nil
}

// bootCluster builds + launches the sandbox from the registry-supplied metadata
// and publishes its route, which satisfies the registry's Reserve.
func (o *Orchestrator) bootCluster(ctx context.Context, cmd *routesync.Command, manifestKey string, tmpl types.TemplateID) (*types.Sandbox, error) {
	envdTok := cmd.AccessToken
	if envdTok == "" {
		return nil, fmt.Errorf("cluster create: access_token required")
	}
	trafTok, _ := keys.MintToken()

	meta, err := o.clusterCommandMetadata(ctx, cmd, clusterstate.ExecutionKindSandbox, cmd.SID)
	if err != nil {
		return nil, err
	}

	sb := &types.Sandbox{
		ID:                 cmd.SID,
		TemplateID:         tmpl.String(),
		State:              types.StateRunning,
		RunDir:             o.cfg.Paths.RunRoot + "/" + cmd.SID,
		BaseDir:            o.cfg.Paths.BaseRoot + "/" + cmd.SID,
		AuthKey:            manifestKey, // pre-cutover path used one root for both domains
		ManifestKey:        manifestKey,
		EnvdAccessToken:    envdTok,
		TrafficAccessToken: trafTok,
		Metadata:           meta,
		CreatedUnix:        time.Now().Unix(),
		DeadlineUnix:       time.Now().Add(time.Duration(o.cfg.Sandbox.TimeoutSec) * time.Second).Unix(),
	}
	if tmpl.Profile == types.ProfileE2B {
		sb.EnvdUDS = sb.RunDir + "/envd.sock"
		sb.CiUDS = sb.RunDir + "/ci.sock"
	}
	if err := o.launch(ctx, sb, tmpl); err != nil {
		o.teardown(context.Background(), sb)
		return nil, err
	}
	o.publishUpsert(sb)
	return sb, nil
}

func (o *Orchestrator) clusterCommandMetadata(
	ctx context.Context,
	cmd *routesync.Command,
	kind clusterstate.ExecutionKind,
	objectID string,
) (map[string]string, error) {
	expectedNodeID := ""
	if cmd != nil && cmd.Binding != "" {
		identity, err := o.st.GetClusterIdentity(ctx)
		if err != nil {
			return nil, fmt.Errorf("cluster command requires an enrolled local identity: %w", err)
		}
		expectedNodeID = identity.NodeID
	}
	return clusterCommandMetadata(cmd, kind, objectID, expectedNodeID)
}

func clusterCommandMetadata(
	cmd *routesync.Command,
	kind clusterstate.ExecutionKind,
	objectID string,
	expectedNodeID string,
) (map[string]string, error) {
	if cmd == nil {
		return nil, fmt.Errorf("cluster command is required")
	}
	if cmd.Binding == "" {
		if cmd.BindingDigest != "" || cmd.RegistryGeneration != "" || cmd.DemandDigest != "" || cmd.DispatchSpecDigest != "" {
			return nil, fmt.Errorf("cluster command has incomplete execution binding")
		}
		meta := make(map[string]string, len(cmd.Config))
		for key, value := range cmd.Config {
			meta[key] = value
		}
		return meta, nil
	}
	if cmd.NodeEpoch == 0 || cmd.SessionSeq == 0 || cmd.RegistryGeneration == "" || cmd.BindingDigest == "" ||
		cmd.DemandDigest == "" || cmd.DispatchSpecDigest == "" {
		return nil, fmt.Errorf("cluster command has incomplete execution binding fence")
	}
	binding, err := clusterstate.DecodeExecutionBinding(cmd.Binding)
	if err != nil {
		return nil, err
	}
	if binding.Kind != kind || binding.ObjectID != objectID {
		return nil, fmt.Errorf("cluster command execution binding identifies a different object")
	}
	if expectedNodeID == "" || binding.NodeID != expectedNodeID {
		return nil, fmt.Errorf("cluster command execution binding identifies a different node")
	}
	if binding.NodeEpoch != cmd.NodeEpoch || binding.RegistryGeneration != cmd.RegistryGeneration {
		return nil, fmt.Errorf("cluster command execution binding Registry History Generation mismatch")
	}
	digest, err := clusterstate.ExecutionBindingDigest(cmd.Binding)
	if err != nil {
		return nil, err
	}
	if digest != cmd.BindingDigest {
		return nil, fmt.Errorf("cluster command execution binding digest mismatch")
	}
	if hex.EncodeToString(binding.DemandDigest[:]) != cmd.DemandDigest ||
		hex.EncodeToString(binding.DispatchSpecDigest[:]) != cmd.DispatchSpecDigest {
		return nil, fmt.Errorf("cluster command demand or dispatch digest mismatch")
	}
	return clusterstate.WithExecutionBinding(clusterstate.WithoutSystemMetadata(cmd.Config), cmd.Binding)
}

var errWrongExecutionBinding = errors.New("cluster: wrong execution Binding")

func (o *Orchestrator) verifySandboxCommandBinding(ctx context.Context, cmd *routesync.Command) error {
	if cmd == nil || cmd.SID == "" || cmd.NodeEpoch == 0 || cmd.RegistryGeneration == "" || cmd.BindingDigest == "" {
		return fmt.Errorf("%w: incomplete sandbox command fence", errWrongExecutionBinding)
	}
	sb, err := o.st.Get(ctx, cmd.SID)
	if err != nil {
		return err
	}
	if sb == nil {
		return fmt.Errorf("%w: sandbox %q is absent", errWrongExecutionBinding, cmd.SID)
	}
	binding, opaque, err := clusterstate.ExecutionBindingFromMetadata(sb.Metadata)
	if err != nil {
		return fmt.Errorf("%w: %v", errWrongExecutionBinding, err)
	}
	digest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		return fmt.Errorf("%w: %v", errWrongExecutionBinding, err)
	}
	if binding.Kind != clusterstate.ExecutionKindSandbox || binding.ObjectID != cmd.SID ||
		binding.NodeEpoch != cmd.NodeEpoch || binding.RegistryGeneration != cmd.RegistryGeneration || digest != cmd.BindingDigest {
		return errWrongExecutionBinding
	}
	return nil
}

func (o *Orchestrator) rebindClusterExecution(ctx context.Context, cmd *routesync.Command) error {
	if cmd == nil || cmd.OldBindingDigest == "" {
		return fmt.Errorf("cluster rebind: old Binding digest is required")
	}
	var kind clusterstate.ExecutionKind
	var objectID string
	switch {
	case cmd.SID != "" && cmd.BuildID == "":
		kind, objectID = clusterstate.ExecutionKindSandbox, cmd.SID
	case cmd.BuildID != "" && cmd.SID == "":
		kind, objectID = clusterstate.ExecutionKindBuild, cmd.BuildID
	default:
		return fmt.Errorf("cluster rebind: exactly one sandbox_id or build_id is required")
	}
	apply := func() error {
		if _, err := o.clusterCommandMetadata(ctx, cmd, kind, objectID); err != nil {
			return err
		}
		changed, err := o.st.CASExecutionBinding(ctx, kind, objectID, cmd.OldBindingDigest, cmd.Binding)
		if err != nil {
			return err
		}
		if !changed {
			return errWrongExecutionBinding
		}
		if kind == clusterstate.ExecutionKindSandbox {
			sb, err := o.st.Get(ctx, objectID)
			if err != nil {
				return err
			}
			if sb == nil {
				return errWrongExecutionBinding
			}
			o.cache(sb)
			o.publishUpsert(sb)
		}
		return nil
	}
	if kind == clusterstate.ExecutionKindSandbox {
		return o.lifecycle.Do(objectID, apply)
	}
	return apply()
}

func (o *Orchestrator) resolveByFingerprint(ctx context.Context, fp string) (string, error) {
	if fp == "" {
		return "", fmt.Errorf("cluster create: empty key fingerprint")
	}
	candidates, err := o.st.AllowedManifestKeysByHash(ctx, fp)
	if err != nil {
		return "", err
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("cluster: no allowlisted manifest key for fingerprint %s (key not distributed?)", fp)
	}
	return candidates[0], nil
}

func (o *Orchestrator) resolveByFingerprints(ctx context.Context, group, authFP, manifestFP string) (store.KeyLease, error) {
	if group == "" || authFP == "" || manifestFP == "" {
		return store.KeyLease{}, errors.New("cluster: group and both key fingerprints are required")
	}
	lease, found, err := o.st.KeyLeaseByFingerprints(ctx, group, authFP, manifestFP)
	if err != nil {
		return store.KeyLease{}, err
	}
	if !found {
		return store.KeyLease{}, fmt.Errorf("cluster: exact node key lease is absent or expired for group %q", group)
	}
	return lease, nil
}

// connectCluster resumes a node-local PAUSED sandbox by sid, reusing the
// API-key-gated Connect with a key derived from the sandbox's own AuthKey.
func (o *Orchestrator) connectCluster(ctx context.Context, sid string) error {
	apiKey, err := o.deriveSandboxAPIKey(ctx, sid)
	if err != nil {
		return err
	}
	_, err = o.Connect(ctx, sid, apiKey, "", 0)
	return err
}

func (o *Orchestrator) deleteCluster(ctx context.Context, sid string) error {
	apiKey, err := o.deriveSandboxAPIKey(ctx, sid)
	if err != nil {
		return err
	}
	ok, err := o.Kill(ctx, sid, apiKey)
	o.log.Info("cluster delete", "sid", sid, "killed", ok, "err", err)
	return err
}

// deriveSandboxAPIKey mints the API key for a sandbox's own AuthKey so the
// cluster command can reuse the api-key-gated Connect/Kill (the node already
// trusts the registry's command; this just satisfies the local auth path).
func (o *Orchestrator) deriveSandboxAPIKey(ctx context.Context, sid string) (string, error) {
	sb, err := o.st.Get(ctx, sid)
	if err != nil {
		return "", err
	}
	if sb == nil {
		return "", fmt.Errorf("cluster: sandbox %s not found", sid)
	}
	raw, err := hex.DecodeString(sb.AuthKey)
	if err != nil {
		return "", err
	}
	return apikey.Mint(raw)
}

// dropClusterKey removes a manifest key from the node's allowlist by fingerprint.
// It is best-effort; normal withdrawal relies on TTL expiry when heartbeat
// refresh stops (cluster.md).
func (o *Orchestrator) dropClusterKey(ctx context.Context, fingerprint string) error {
	keys, err := o.st.AllowedManifestKeysByHash(ctx, fingerprint)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if _, err := o.st.RemoveManifestKey(ctx, k); err != nil {
			return err
		}
	}
	o.log.Debug("cluster key_drop", "fp", fingerprint, "removed", len(keys))
	return nil
}
