package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
)

// BuildReserveResult is the registry-assigned identity + placement for a build
// (the router returns these to the e2b client + routes its follow-ups by build_id).
type BuildReserveResult struct {
	BuildID     string             `json:"build_id"`
	TemplateID  string             `json:"template_id"`
	NodeID      string             `json:"node_id"`
	APIEndpoint string             `json:"api_endpoint"`
	Profile     types.Profile      `json:"profile"`
	Target      *types.BuildTarget `json:"target"`
}

// BuildReserveReq is the router's build-register ask: the group + profile, the
// canonical required Build resources, and portable template configuration.
type BuildReserveReq struct {
	Workspaces []workspace.Entry         `json:"workspaces,omitempty"`
	Group      string                    `json:"group"`
	BuildID    string                    `json:"build_id,omitempty"`
	TemplateID string                    `json:"template_id,omitempty"`
	Profile    types.Profile             `json:"profile"`
	Resources  *routesync.BuildResources `json:"resources,omitempty"`
	Metadata   map[string]string         `json:"metadata,omitempty"`
	Env        map[string]string         `json:"env,omitempty"`
	Secure     bool                      `json:"secure,omitempty"`
	// Credentials is a request-scoped transport envelope. Only its digest is
	// replicated; the selected node stores the values encrypted.
	Credentials *sandboxcfg.Credentials `json:"credentials,omitempty"`
	// MMDSSecrets is a request-scoped transport envelope. ReserveBuild strips
	// all secret values from Metadata before persisting its registration intent.
	MMDSSecrets map[string]string `json:"mmds_secrets,omitempty"`
}

const buildRegisterAckTimeout = 5 * time.Second

const (
	terminalBuildStoreAttempts   = 5
	terminalBuildStoreRetryDelay = 20 * time.Millisecond
)

var errNodeBuildIDConflict = metadata.ErrOwnershipConflict

// ReserveBuild places a build on a resource-eligible node and pre-provisions it
// (cluster.md): the registry assigns the build/template ids, PlaceBuild filters
// by low-frequency configured capacity, Registry checks the Holder's current
// registration usage, and a build_register command asks that node to make the
// authoritative durable admission decision. The registry record pins ambiguous
// delivery to the selected node; it is routing state, not an admission claim.
func (r *Registry) ReserveBuild(ctx context.Context, req BuildReserveReq) (*BuildReserveResult, error) {
	if req.Group == "" {
		return nil, fmt.Errorf("registry: group is required")
	}
	if !req.Profile.Valid() {
		return nil, fmt.Errorf("registry: unknown build profile %q", req.Profile)
	}
	if err := workspace.CheckEnvelope(req.Metadata, req.Workspaces); err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
	}
	workspaceDigest, err := workspace.InputDigest(req.Workspaces)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid workspace declaration", errInvalidSandboxConfig)
	}
	buildMetadata, mmdsSecrets, mmdsDigest, err := normalizeBuildRegistrationMMDS(req.Metadata, req.MMDSSecrets)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
	}
	buildMetadata, credentials, credentialsDigest, err := normalizeBuildRegistrationCredentials(buildMetadata, req.Credentials, req.Profile)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
	}
	buildMetadata, err = sandboxcfg.NormalizeResourceMetadata(buildMetadata)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
	}
	buildMetadata, err = sandboxcfg.NormalizeTrafficMetadata(buildMetadata)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
	}
	if raw, present := buildMetadata[sandboxcfg.NsTraffic]; present {
		patch, parseErr := sandboxcfg.ParseTrafficPatch(raw)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, parseErr)
		}
		if validateErr := sandboxcfg.ValidateTrafficForProfile(req.Profile, patch); validateErr != nil {
			return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, validateErr)
		}
	}
	buildMetadata, err = clusterstate.WithObjectLocation(buildMetadata, clusterstate.ObjectLocation{Group: req.Group})
	if err != nil {
		return nil, err
	}
	req.Metadata = buildMetadata
	attempt := buildRegistrationAttempt{MMDSSecrets: cloneStringMap(mmdsSecrets), Credentials: cloneBuildCredentials(credentials), Workspaces: req.Workspaces}
	resources := req.Resources
	if resources == nil {
		return nil, errors.New("registry: build resources are required")
	}
	if err := resources.Types().ValidateRequired(); err != nil {
		return nil, fmt.Errorf("registry: build resources: %w", err)
	}
	buildID := req.BuildID
	if buildID == "" {
		buildID = "bld-" + newID()
	}
	if err := types.ValidateBuildID(buildID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrReserveBadRequest, err)
	}
	ref, err := clusterstate.NodeBuildRefFromMetadata(buildID, buildMetadata)
	if err != nil {
		return nil, err
	}
	templateID := req.TemplateID
	if templateID == "" {
		templateID = "transient-" + newID()
	}
	ref.TemplateID = templateID
	excluded := placementExclusions{}
	var lastFailure error
	if req.BuildID != "" {
		if rec, found, err := r.stores.GetBuildInGroup(ctx, req.Group, req.BuildID); err != nil {
			return nil, err
		} else if found {
			if rec.RegistrationWorkspacesDigest != workspaceDigest || !sameBuildRegistrationDefinition(rec, req.Profile, resources, req.Metadata, req.Env, req.Secure, credentialsDigest, mmdsDigest, req.TemplateID) {
				return nil, fmt.Errorf("registry: build %s immutable definition conflicts with existing registration", req.BuildID)
			}
			if rec.State != BuildStarting && rec.RegistrationTargetSet {
				return r.buildReserveResult(ctx, rec), nil
			}
			// A prior dispatch had an ambiguous result. Re-establish the node
			// ownership index and replay the exact stored intent to the same node;
			// neither current placement nor Provider output may change it.
			ref.TemplateID = rec.TemplateID
			if err := r.refreshBuildRegistrationRef(ctx, rec); err != nil {
				return nil, err
			}
			ack, err := r.dispatchBuildRegistration(ctx, rec, attempt)
			if err != nil {
				return nil, fmt.Errorf("registry: build_register remains ambiguous on node %s: %w", rec.NodeID, err)
			}
			if ack != nil && ack.Status == routesync.AckAccepted {
				target, err := acceptedBuildRegistrationTarget(ack)
				if err != nil {
					return nil, fmt.Errorf("registry: build_register returned an invalid acceptance on node %s: %w", rec.NodeID, err)
				}
				registered, err := r.markBuildRegistrationAccepted(ctx, rec.Group, rec.BuildID, rec.NodeID, rec.TemplateID, target)
				if err != nil {
					return nil, err
				}
				return r.buildReserveResult(ctx, registered), nil
			}
			if !definitiveBuildRegistrationRejection(ack) {
				return nil, ambiguousBuildRegistrationError(rec.NodeID, ack)
			}
			if rec.State != BuildStarting {
				return nil, fmt.Errorf("registry: build_register target recovery was rejected by node %s: %w", rec.NodeID, buildRegistrationRejectionError(ack))
			}
			if err := r.removeBuildRegistrationIntent(ctx, rec); err != nil {
				return nil, err
			}
			lastFailure = buildRegistrationRejectionError(ack)
			excluded.add(rec.NodeID)
		}
	}

	// Place + dispatch. node_list supplies candidates; the node owner validates the
	// live connection and atomically applies authoritative registration admission.
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		placement, err := r.placer.Place(ctx, PlaceRequest{
			Group: req.Group, RouteKey: "build", Build: true, Config: req.Metadata,
			BuildResources: resources,
			ExcludeNodeIDs: excluded.values(),
		})
		if err != nil {
			if errors.Is(err, ErrNoNode) && lastFailure != nil {
				return nil, lastFailure
			}
			return nil, err
		}
		if placement == nil || placement.NodeID == "" {
			return nil, ErrNoNode
		}
		if !validFullFingerprint(placement.APISecretFingerprint) {
			return nil, errors.New("registry: build placement is missing a valid API secret fingerprint")
		}
		id := placement.NodeID
		if excluded.has(id) {
			if lastFailure != nil {
				return nil, lastFailure
			}
			return nil, ErrNoNode
		}
		if err := r.nodeRuntimeLive(ctx, id); err != nil {
			if !errors.Is(err, ErrNodeGone) {
				return nil, err
			}
			lastFailure = err
			excluded.add(id)
			continue
		}
		runtime, found, err := r.nodeOwner.Runtime(ctx, id)
		if err != nil {
			// A profile read failure is not proof that another node is safe to
			// select; keep the operation side-effect free and surface ambiguity.
			return nil, err
		}
		if found && runtime != nil && runtime.BuildRegistrationUsage != nil &&
			!hasRegistrationHeadroom(runtime, resources) {
			lastFailure = fmt.Errorf("registry: node %s has insufficient build registration headroom", id)
			excluded.add(id)
			continue
		}
		// The registry records an intent before dispatch so an ambiguous result is
		// pinned to this node. The node's durable registration transaction is the
		// only authoritative admission decision.
		rec := &BuildRecord{
			Group: req.Group, BuildID: buildID, NodeID: id, Profile: req.Profile,
			APISecretFingerprint:          placement.APISecretFingerprint,
			Resources:                     cloneBuildResources(resources),
			RegistrationConfig:            cloneStringMap(req.Metadata),
			RegistrationEnv:               cloneStringMap(req.Env),
			RegistrationSecure:            req.Secure,
			RegistrationCredentialsDigest: credentialsDigest,
			RegistrationMMDSValuesDigest:  mmdsDigest,
			RegistrationImageRepo:         placement.ImageRepo,
			RegistrationRegistryAuth:      placement.RegistryAuth,
			State:                         BuildStarting, TemplateID: templateID,
			RegistrationWorkspacesDigest: workspaceDigest,
		}
		if _, err := r.stores.builds.CommitRegistrationIntent(ctx, metadata.BuildRecord(*rec), metadata.NodeBuildRef(ref)); err != nil {
			if errors.Is(err, metadata.ErrOwnershipConflict) {
				lastFailure = err
				excluded.add(id)
				continue
			}
			return nil, err
		}
		ack, err := r.dispatchBuildRegistration(ctx, rec, attempt)
		if err != nil {
			// Once the intent is durable, timeout, reset, disconnect, and ACK loss
			// are all ambiguous. Keep the binding so only this node/BuildID can be
			// queried or retried.
			return nil, fmt.Errorf("registry: build_register ambiguous on node %s: %w", id, err)
		}
		if ack == nil || ack.Status != routesync.AckAccepted {
			if !definitiveBuildRegistrationRejection(ack) {
				return nil, ambiguousBuildRegistrationError(id, ack)
			}
			if err := r.removeBuildRegistrationIntent(ctx, rec); err != nil {
				return nil, err
			}
			lastFailure = buildRegistrationRejectionError(ack)
			excluded.add(id)
			continue
		}
		target, err := acceptedBuildRegistrationTarget(ack)
		if err != nil {
			return nil, fmt.Errorf("registry: build_register returned an invalid acceptance on node %s: %w", id, err)
		}
		registered, err := r.markBuildRegistrationAccepted(ctx, rec.Group, rec.BuildID, rec.NodeID, rec.TemplateID, target)
		if err != nil {
			return nil, err
		}
		return r.buildReserveResult(ctx, registered), nil
	}
}

func hasRegistrationHeadroom(node *NodeRecord, requested *routesync.BuildResources) bool {
	if node == nil || node.BuildRegistrationCapacity == nil || requested == nil {
		return true
	}
	capacity := types.BuildAdmissionLimit{MaxBuilds: node.BuildRegistrationCapacity.MaxBuilds}
	if node.BuildRegistrationCapacity.Resources != nil {
		capacity.Resources = node.BuildRegistrationCapacity.Resources.Types()
	}
	var usedBuilds int64
	var used types.BuildResources
	if node.BuildRegistrationUsage != nil {
		usedBuilds = node.BuildRegistrationUsage.Builds
		if node.BuildRegistrationUsage.Resources != nil {
			used = node.BuildRegistrationUsage.Resources.Types()
		}
	}
	return capacity.AllowsAdd(usedBuilds, used, requested.Types())
}

func sameBuildRegistrationDefinition(rec *BuildRecord, profile types.Profile, resources *routesync.BuildResources, config, env map[string]string, secure bool, credentialsDigest, mmdsDigest, requestedTemplateID string) bool {
	return rec != nil && rec.Profile == profile && rec.Resources != nil && resources != nil &&
		*rec.Resources == *resources && maps.Equal(rec.RegistrationConfig, config) && maps.Equal(rec.RegistrationEnv, env) &&
		rec.RegistrationSecure == secure && rec.RegistrationCredentialsDigest == credentialsDigest &&
		rec.RegistrationMMDSValuesDigest == mmdsDigest &&
		(requestedTemplateID == "" || rec.TemplateID == requestedTemplateID)
}

// buildRegistrationAttempt contains only this caller's replay material. Durable
// BuildRecord stores the corresponding digests and never serializes this envelope.
type buildRegistrationAttempt struct {
	MMDSSecrets map[string]string
	Credentials *sandboxcfg.Credentials
	Workspaces  []workspace.Entry
}

func (r *Registry) dispatchBuildRegistration(ctx context.Context, rec *BuildRecord, attempt buildRegistrationAttempt) (*routesync.CmdAck, error) {
	if r.nodeOwner == nil {
		return nil, ErrNodeGone
	}
	digest, err := buildRegistrationMMDSDigest(attempt.MMDSSecrets)
	if err != nil {
		return nil, err
	}
	if digest != rec.RegistrationMMDSValuesDigest {
		return nil, fmt.Errorf("registry: build %s MMDS replay values are unavailable or changed", rec.BuildID)
	}
	credentialsDigest, err := buildRegistrationCredentialsDigest(attempt.Credentials)
	if err != nil {
		return nil, err
	}
	if credentialsDigest != rec.RegistrationCredentialsDigest {
		return nil, fmt.Errorf("registry: build %s credential replay values are unavailable or changed", rec.BuildID)
	}
	wsDigest, err := workspace.InputDigest(attempt.Workspaces)
	if err != nil || wsDigest != rec.RegistrationWorkspacesDigest {
		return nil, errors.New("workspace registration replay values are unavailable or changed")
	}
	cmd := &routesync.Command{
		CmdID: newID(), Kind: routesync.CmdBuildRegister,
		BuildID: rec.BuildID, TemplateRef: rec.TemplateID, Profile: string(rec.Profile),
		BuildResources: cloneBuildResources(rec.Resources), Config: cloneStringMap(rec.RegistrationConfig),
		Workspaces: attempt.Workspaces, BuildEnv: cloneStringMap(rec.RegistrationEnv), BuildSecure: rec.RegistrationSecure,
		BuildCredentials:     cloneBuildCredentials(attempt.Credentials),
		APISecretFingerprint: rec.APISecretFingerprint, ImageRepo: rec.RegistrationImageRepo,
		RegistryAuth: rec.RegistrationRegistryAuth, BuildMMDSSecrets: cloneStringMap(attempt.MMDSSecrets),
	}
	if err := routesync.ValidateMessage(&routesync.Msg{Type: routesync.TypeCommand, Cmd: cmd}); err != nil {
		return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, HTTPStatus: http.StatusBadRequest, Reason: "registration exceeds node-link message limits"}, nil
	}
	return r.nodeOwner.SendCommandAndWait(ctx, rec.NodeID, cmd, buildRegisterAckTimeout)
}

func normalizeBuildRegistrationMMDS(metadata map[string]string, separate map[string]string) (map[string]string, map[string]string, string, error) {
	bodyDoc, _, err := sandboxcfg.ExtractMMDSReplay(metadata, nil)
	if err != nil {
		return nil, nil, "", fmt.Errorf("build MMDS config: %w", err)
	}
	if separate != nil && bodyDoc.SecretsPresent {
		bodySecrets := make(map[string]string, len(bodyDoc.SecretValues))
		for name, value := range bodyDoc.SecretValues {
			bodySecrets[name] = string(value)
		}
		if !maps.Equal(bodySecrets, separate) {
			return nil, nil, "", errors.New("build MMDS initial values conflict between metadata and request envelope")
		}
	}
	var header *string
	if separate != nil {
		raw, err := json.Marshal(struct {
			Secrets map[string]string `json:"secrets"`
		}{Secrets: separate})
		if err != nil {
			return nil, nil, "", fmt.Errorf("encode build MMDS initial values: %w", err)
		}
		value := string(raw)
		header = &value
	}
	doc, cleaned, err := sandboxcfg.ExtractMMDSReplay(metadata, header)
	if err != nil {
		return nil, nil, "", fmt.Errorf("build MMDS config: %w", err)
	}
	values := make(map[string]string, len(doc.SecretValues))
	for name, value := range doc.SecretValues {
		values[name] = string(value)
	}
	digest, err := buildRegistrationMMDSDigest(values)
	if err != nil {
		return nil, nil, "", err
	}
	return cleaned, values, digest, nil
}

func normalizeBuildRegistrationCredentials(metadata map[string]string, separate *sandboxcfg.Credentials, profile types.Profile) (map[string]string, *sandboxcfg.Credentials, string, error) {
	_, bodyPresent := metadata[sandboxcfg.NsCredentials]
	body, cleaned, err := sandboxcfg.ExtractCredentials(metadata)
	if err != nil {
		return nil, nil, "", fmt.Errorf("build credentials: %w", err)
	}
	selected := body
	if separate != nil {
		if bodyPresent && body != *separate {
			return nil, nil, "", errors.New("build credentials conflict between metadata and request envelope")
		}
		selected = *separate
	}
	if err := sandboxcfg.ValidateCredentialsForProfile(profile, selected); err != nil {
		return nil, nil, "", fmt.Errorf("build credentials: %w", err)
	}
	var credentials *sandboxcfg.Credentials
	if selected != (sandboxcfg.Credentials{}) {
		credentials = cloneBuildCredentials(&selected)
	}
	digest, err := buildRegistrationCredentialsDigest(credentials)
	if err != nil {
		return nil, nil, "", err
	}
	return cleaned, credentials, digest, nil
}

func buildRegistrationCredentialsDigest(credentials *sandboxcfg.Credentials) (string, error) {
	value := sandboxcfg.Credentials{}
	if credentials != nil {
		value = *credentials
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode build credential identity: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func cloneBuildCredentials(credentials *sandboxcfg.Credentials) *sandboxcfg.Credentials {
	if credentials == nil {
		return nil
	}
	clone := *credentials
	return &clone
}

func buildRegistrationMMDSDigest(values map[string]string) (string, error) {
	if len(values) == 0 {
		values = map[string]string{}
	}
	canonical, err := json.Marshal(values)
	if err != nil {
		return "", fmt.Errorf("encode build MMDS identity: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func definitiveBuildRegistrationRejection(ack *routesync.CmdAck) bool {
	return ack != nil && ack.Status == routesync.AckRejected &&
		(ack.HTTPStatus == http.StatusBadRequest || ack.HTTPStatus == http.StatusConflict ||
			ack.HTTPStatus == http.StatusTooManyRequests)
}

func buildRegistrationRejectionError(ack *routesync.CmdAck) error {
	reason := ""
	status := 0
	if ack != nil {
		reason = ack.Reason
		status = ack.HTTPStatus
	}
	if reason == "" {
		reason = http.StatusText(status)
	}
	return &nodeCommandRejection{status: status, reason: reason}
}

func ambiguousBuildRegistrationError(nodeID string, ack *routesync.CmdAck) error {
	if ack == nil {
		return fmt.Errorf("registry: build_register ambiguous on node %s: missing acknowledgement", nodeID)
	}
	return fmt.Errorf("registry: build_register ambiguous on node %s: status=%q http_status=%d reason=%s", nodeID, ack.Status, ack.HTTPStatus, ack.Reason)
}

// Replay refreshes ownership only while the exact stored registration still
// exists. The repository checks the Build identity and live node in one transaction.
func (r *Registry) refreshBuildRegistrationRef(ctx context.Context, expected *BuildRecord) error {
	return r.stores.builds.EnsureRegistrationOwnership(ctx, metadata.BuildKey{Group: expected.Group, BuildID: expected.BuildID}, expected.NodeID, expected.TemplateID)
}

func (r *Registry) removeBuildRegistrationIntent(ctx context.Context, expected *BuildRecord) error {
	return r.stores.builds.RejectRegistrationIntent(ctx, metadata.BuildKey{Group: expected.Group, BuildID: expected.BuildID}, expected.NodeID, expected.TemplateID)
}

func (r *Registry) markBuildRegistrationAccepted(ctx context.Context, group, buildID, nodeID, templateID string, target *types.BuildTarget) (*BuildRecord, error) {
	accepted, err := r.stores.builds.AcceptBuildRegistration(ctx, metadata.BuildKey{Group: group, BuildID: buildID}, nodeID, templateID, target)
	if err != nil {
		return nil, err
	}
	rec := BuildRecord(accepted.Value)
	return &rec, nil
}

func (r *Registry) buildReserveResult(ctx context.Context, rec *BuildRecord) *BuildReserveResult {
	if rec == nil {
		return nil
	}
	return &BuildReserveResult{
		BuildID:     rec.BuildID,
		TemplateID:  rec.TemplateID,
		NodeID:      rec.NodeID,
		APIEndpoint: r.nodeAPIEndpoint(ctx, rec.NodeID),
		Profile:     rec.Profile,
		Target:      cloneBuildTarget(rec.RegistrationTarget),
	}
}

// applyBuildUpsert converges the Registry's rebuildable routing/query projection
// from one retained node Build. Admission usage remains node-owned and comes
// from the node's durable heartbeat.
func (r *Registry) applyBuildUpsert(ctx context.Context, nodeID string, e *routesync.BuildEvent) error {
	if e == nil || e.BuildID == "" {
		return nil
	}
	registration, err := nodeStreamRegistration(ctx, nodeID)
	if err != nil {
		return err
	}
	switch BuildState(e.State) {
	case BuildRegistered, BuildWaiting, BuildBuilding, BuildReady, BuildError:
	default:
		return fmt.Errorf("invalid build %s projection state %q", e.BuildID, e.State)
	}
	ref, found, err := r.lookupNodeBuildRef(ctx, nodeID, e.BuildID)
	if err != nil {
		return err
	}
	if !found || (ref.TemplateID != "" && ref.TemplateID != e.TemplateID) {
		return nil
	}
	// The storage operation guards ownership and merges the report atomically.
	write := func(writeCtx context.Context) error {
		return r.stores.builds.ApplyBuildReport(writeCtx, registration, metadata.NodeBuildRef(ref), *e)
	}
	if BuildState(e.State) == BuildReady || BuildState(e.State) == BuildError {
		return retryTerminalBuildStore(ctx, write)
	}
	return write(ctx)
}

// applyBuildDelete removes only the projection bound to this exact node. The
// node's SQLite deletion is the lifecycle decision; Registry has no terminal
// timer of its own.
func (r *Registry) applyBuildDelete(ctx context.Context, nodeID, buildID, templateID string) error {
	if nodeID == "" || buildID == "" {
		return nil
	}
	registration, err := nodeStreamRegistration(ctx, nodeID)
	if err != nil {
		return err
	}
	ref, found, err := r.lookupNodeBuildRef(ctx, nodeID, buildID)
	if err != nil {
		return err
	}
	if !found || (ref.TemplateID != "" && ref.TemplateID != templateID) {
		return nil
	}
	return r.stores.builds.DeleteBuildProjection(ctx, registration, metadata.NodeBuildRef(ref))
}

// applyNodeBuildFullSnapshot prunes only post-registration projection refs
// present in the reconnect baseline. A Build registration added after
// NodeRegister is therefore never mistaken for a missing snapshot row, while
// applyBuildDelete preserves an ambiguous BuildStarting dispatch intent.
// Re-reading the exact ref also fences replacement.
func (r *Registry) applyNodeBuildFullSnapshot(ctx context.Context, nodeID string, expected []clusterstate.NodeBuildRef, seen map[string]struct{}) error {
	if nodeID == "" {
		return nil
	}
	for _, baseline := range expected {
		if baseline.Group == "" || baseline.BuildID == "" {
			continue
		}
		if _, ok := seen[baseline.BuildID]; ok {
			continue
		}
		current, found, err := r.lookupNodeBuildRef(ctx, nodeID, baseline.BuildID)
		if err != nil {
			return err
		}
		if !found || current != baseline {
			continue
		}
		if err := r.applyBuildDelete(ctx, nodeID, baseline.BuildID, baseline.TemplateID); err != nil {
			return err
		}
	}
	return nil
}

func retryTerminalBuildStore(ctx context.Context, operation func(context.Context) error) error {
	// Briefly outlive a closing stream so a committed terminal event can win a
	// response-loss race. Exhaustion is returned to serveNodeLinkLocal, which
	// closes the session and causes the node to perform a fresh durable replay;
	// the transport reader is never held behind an unbounded store outage.
	retryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lifecycleAckTimeout)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt < terminalBuildStoreAttempts; attempt++ {
		if lastErr = operation(retryCtx); lastErr == nil {
			return nil
		}
		if !errors.Is(lastErr, metadata.ErrConflict) && !errors.Is(lastErr, metadata.ErrUnavailable) {
			return lastErr
		}
		if attempt+1 == terminalBuildStoreAttempts {
			break
		}
		timer := time.NewTimer(time.Duration(attempt+1) * terminalBuildStoreRetryDelay)
		select {
		case <-retryCtx.Done():
			timer.Stop()
			return retryCtx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func (r *Registry) lookupNodeBuildRef(ctx context.Context, nodeID, buildID string) (clusterstate.NodeBuildRef, bool, error) {
	ref, _, found, err := r.lookupNodeBuildRefVersion(ctx, nodeID, buildID)
	return ref, found, err
}

func (r *Registry) lookupNodeBuildRefVersion(ctx context.Context, nodeID, buildID string) (clusterstate.NodeBuildRef, metadata.Version, bool, error) {
	var ref clusterstate.NodeBuildRef
	var revision metadata.Version
	var found bool
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		ref, revision, found, err = r.stores.getNodeBuildRefVersion(ctx, nodeID, buildID)
		if err == nil || !transientRouteRead(err) {
			return ref, revision, found, err
		}
		select {
		case <-ctx.Done():
			return clusterstate.NodeBuildRef{}, metadata.Version{}, false, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		}
	}
	return ref, revision, found, err
}

// ResolveBuild maps a group's build_id to its node using durable metadata, so
// router restart recovery does not depend on a process-local Build map.
func (r *Registry) ResolveBuild(ctx context.Context, group, buildID string) (*BuildReserveResult, bool, error) {
	b, found, err := r.stores.GetBuildInGroup(ctx, group, buildID)
	if err != nil || !found {
		return nil, false, err
	}
	return r.resolveBuildRecord(ctx, b)
}

func (r *Registry) resolveBuildRecord(ctx context.Context, b *BuildRecord) (*BuildReserveResult, bool, error) {
	if b.State == BuildStarting || !b.RegistrationTargetSet {
		return nil, false, fmt.Errorf("build registration binding is not yet complete")
	}
	result := r.buildReserveResult(ctx, b)
	if result.APIEndpoint == "" {
		return nil, false, fmt.Errorf("build node API endpoint is unavailable")
	}
	return result, true, nil
}

// ResolveBuildByTemplate reads only the group's existing retained projection.
// Missing registration IDs indicate an incomplete upgrade/full sync, not 404.
func (r *Registry) ResolveBuildByTemplate(ctx context.Context, group, templateID string) (*BuildReserveResult, bool, error) {
	var match *BuildRecord
	for cursor := metadata.Cursor(""); ; {
		page, err := r.stores.builds.ListBuildsByTemplate(ctx, group, templateID, cursor, 2)
		if err != nil {
			return nil, false, err
		}
		for _, row := range page.Items {
			if match != nil {
				return nil, false, fmt.Errorf("ambiguous transient build identity")
			}
			record := BuildRecord(row.Value)
			match = &record
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	if match == nil {
		return nil, false, nil
	}
	return r.resolveBuildRecord(ctx, match)
}

func acceptedBuildRegistrationTarget(ack *routesync.CmdAck) (*types.BuildTarget, error) {
	if ack == nil || ack.Status != routesync.AckAccepted {
		return nil, errors.New("acceptance ACK is missing")
	}
	if ack.BuildRegister == nil {
		return nil, errors.New("acceptance ACK has no build_register result")
	}
	target := cloneBuildTarget(ack.BuildRegister.Target)
	if target != nil {
		if err := target.Validate(); err != nil {
			return nil, err
		}
	}
	return target, nil
}

func cloneBuildTarget(target *types.BuildTarget) *types.BuildTarget {
	if target == nil {
		return nil
	}
	copy := *target
	return &copy
}

func sameBuildTarget(a, b *types.BuildTarget) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
