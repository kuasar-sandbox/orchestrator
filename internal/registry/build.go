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
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
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

var errNodeBuildIDConflict = errors.New("registry: build id is already owned by another group on this node")

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
	metadata, mmdsSecrets, mmdsDigest, err := normalizeBuildRegistrationMMDS(req.Metadata, req.MMDSSecrets)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
	}
	metadata, credentials, credentialsDigest, err := normalizeBuildRegistrationCredentials(metadata, req.Credentials, req.Profile)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
	}
	metadata, err = sandboxcfg.NormalizeResourceMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
	}
	metadata, err = sandboxcfg.NormalizeTrafficMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
	}
	if raw, present := metadata[sandboxcfg.NsTraffic]; present {
		patch, parseErr := sandboxcfg.ParseTrafficPatch(raw)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, parseErr)
		}
		if validateErr := sandboxcfg.ValidateTrafficForProfile(req.Profile, patch); validateErr != nil {
			return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, validateErr)
		}
	}
	metadata, err = clusterstate.WithObjectLocation(metadata, clusterstate.ObjectLocation{Group: req.Group})
	if err != nil {
		return nil, err
	}
	req.Metadata = metadata
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
	ref, err := clusterstate.NodeBuildRefFromMetadata(buildID, metadata)
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
			if !sameBuildRegistrationDefinition(rec, req.Profile, resources, req.Metadata, req.Env, req.Secure, credentialsDigest, mmdsDigest, req.TemplateID) {
				return nil, fmt.Errorf("registry: build %s immutable definition conflicts with existing registration", req.BuildID)
			}
			if rec.State != BuildStarting && rec.RegistrationTargetSet {
				return r.buildReserveResult(ctx, rec), nil
			}
			rec.registrationMMDSSecrets = cloneStringMap(mmdsSecrets)
			rec.registrationCredentials = cloneBuildCredentials(credentials)
			// A prior dispatch had an ambiguous result. Re-establish the node
			// ownership index and replay the exact stored intent to the same node;
			// neither current placement nor Provider output may change it.
			ref.TemplateID = rec.TemplateID
			if err := r.refreshBuildRegistrationRef(ctx, rec, ref); err != nil {
				return nil, err
			}
			ack, err := r.dispatchBuildRegistration(ctx, rec)
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
			registrationMMDSSecrets: cloneStringMap(mmdsSecrets),
			registrationCredentials: cloneBuildCredentials(credentials),
		}
		if _, inserted, err := r.stores.casRouteBuildShard(ctx, rec, 0); err != nil {
			return nil, err
		} else if !inserted {
			return nil, fmt.Errorf("registry: build %s registration appeared during placement", buildID)
		}
		if err := r.stores.AddNodeBuildRef(ctx, id, ref); err != nil {
			if cleanupErr := r.removeBuildRegistrationRoute(ctx, rec); cleanupErr != nil {
				return nil, errors.Join(err, cleanupErr)
			}
			if errors.Is(err, errNodeBuildIDConflict) {
				lastFailure = err
				excluded.add(id)
				continue
			}
			return nil, err
		}
		ack, err := r.dispatchBuildRegistration(ctx, rec)
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

func (r *Registry) dispatchBuildRegistration(ctx context.Context, rec *BuildRecord) (*routesync.CmdAck, error) {
	if r.nodeOwner == nil {
		return nil, ErrNodeGone
	}
	digest, err := buildRegistrationMMDSDigest(rec.registrationMMDSSecrets)
	if err != nil {
		return nil, err
	}
	if digest != rec.RegistrationMMDSValuesDigest {
		return nil, fmt.Errorf("registry: build %s MMDS replay values are unavailable or changed", rec.BuildID)
	}
	credentialsDigest, err := buildRegistrationCredentialsDigest(rec.registrationCredentials)
	if err != nil {
		return nil, err
	}
	if credentialsDigest != rec.RegistrationCredentialsDigest {
		return nil, fmt.Errorf("registry: build %s credential replay values are unavailable or changed", rec.BuildID)
	}
	cmd := &routesync.Command{
		CmdID: newID(), Kind: routesync.CmdBuildRegister,
		BuildID: rec.BuildID, TemplateRef: rec.TemplateID, Profile: string(rec.Profile),
		BuildResources: cloneBuildResources(rec.Resources), Config: cloneStringMap(rec.RegistrationConfig),
		BuildEnv: cloneStringMap(rec.RegistrationEnv), BuildSecure: rec.RegistrationSecure,
		BuildCredentials:     cloneBuildCredentials(rec.registrationCredentials),
		APISecretFingerprint: rec.APISecretFingerprint, ImageRepo: rec.RegistrationImageRepo,
		RegistryAuth: rec.RegistrationRegistryAuth, BuildMMDSSecrets: cloneStringMap(rec.registrationMMDSSecrets),
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

// A replay may cross deletion while accessing another shard. Check the route
// both before and after publishing the ref, and remove only the stale identity
// if the original registration ceased to be current. Never dispatch it then.
func (r *Registry) refreshBuildRegistrationRef(ctx context.Context, expected *BuildRecord, ref clusterstate.NodeBuildRef) error {
	matches := func() (bool, error) {
		current, found, err := r.stores.GetBuildInGroup(ctx, expected.Group, expected.BuildID)
		return found && current != nil && current.NodeID == expected.NodeID && current.TemplateID == expected.TemplateID, err
	}
	if ok, err := matches(); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("registry: build %s registration changed before replay ref", expected.BuildID)
	}
	if err := r.stores.AddNodeBuildRef(ctx, expected.NodeID, ref); err != nil {
		return err
	}
	if ok, err := matches(); err != nil {
		return err // An unavailable authority is not proof that its ref is stale.
	} else if ok {
		return nil
	}
	current, revision, found, err := r.lookupNodeBuildRefVersion(ctx, expected.NodeID, expected.BuildID)
	if err != nil {
		return err
	}
	if found && current.Group == expected.Group && current.TemplateID == expected.TemplateID {
		if _, err := r.stores.removeNodeBuildRefShardAtRevision(ctx, expected.NodeID, expected.BuildID, revision); err != nil {
			return err
		}
	}
	return fmt.Errorf("registry: build %s registration changed while refreshing replay ref", expected.BuildID)
}

// A failed owner-ref insert owns only its original provisional route. It must
// neither remove another lifecycle's route nor touch that lifecycle's ref.
func (r *Registry) removeBuildRegistrationRoute(ctx context.Context, expected *BuildRecord) error {
	for attempt := 0; attempt < 5; attempt++ {
		current, revision, found, err := r.stores.getRouteBuildShard(ctx, expected.Group, expected.BuildID)
		if err != nil {
			return err
		}
		if !found || current.TemplateID != expected.TemplateID || current.NodeID != expected.NodeID || current.State != BuildStarting {
			return fmt.Errorf("registry: build %s provisional registration changed", expected.BuildID)
		}
		if deleted, err := r.stores.deleteRouteBuildShardIfRevision(ctx, expected.Group, expected.BuildID, revision); err != nil {
			return err
		} else if deleted {
			return nil
		}
	}
	return fmt.Errorf("registry: build %s provisional registration cleanup conflicted", expected.BuildID)
}

func (r *Registry) removeBuildRegistrationIntent(ctx context.Context, expected *BuildRecord) error {
	for attempt := 0; attempt < 5; attempt++ {
		ref, refRevision, refFound, err := r.lookupNodeBuildRefVersion(ctx, expected.NodeID, expected.BuildID)
		if err != nil {
			return err
		}
		current, revision, found, err := r.stores.getRouteBuildShard(ctx, expected.Group, expected.BuildID)
		if err != nil {
			return err
		}
		if !found || current.NodeID != expected.NodeID || current.TemplateID != expected.TemplateID || current.State != BuildStarting ||
			(refFound && (ref.Group != expected.Group || ref.TemplateID != expected.TemplateID)) {
			return fmt.Errorf("registry: build %s registration intent changed before rejection", expected.BuildID)
		}
		deleted, err := r.stores.deleteRouteBuildShardIfRevision(ctx, expected.Group, expected.BuildID, revision)
		if err != nil {
			return err
		}
		if !deleted {
			continue
		}
		if refFound {
			if _, err := r.stores.removeNodeBuildRefShardAtRevision(ctx, expected.NodeID, expected.BuildID, refRevision); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("registry: build %s registration rejection conflicted", expected.BuildID)
}

func (r *Registry) markBuildRegistrationAccepted(ctx context.Context, group, buildID, nodeID, templateID string, target *types.BuildTarget) (*BuildRecord, error) {
	for attempt := 0; attempt < 5; attempt++ {
		current, revision, found, err := r.stores.getRouteBuildShard(ctx, group, buildID)
		if err != nil {
			return nil, err
		}
		if !found || current.NodeID != nodeID || current.TemplateID != templateID {
			return nil, fmt.Errorf("registry: build %s registration intent changed before acceptance", buildID)
		}
		if current.RegistrationTargetSet {
			if !sameBuildTarget(current.RegistrationTarget, target) {
				return nil, fmt.Errorf("registry: build %s accepted target conflicts with durable registration", buildID)
			}
			if current.State != BuildStarting {
				return current, nil
			}
		}
		current.RegistrationTarget = cloneBuildTarget(target)
		current.RegistrationTargetSet = true
		current.RegistrationImageRepo = ""
		current.RegistrationRegistryAuth = ""
		if current.State != BuildStarting {
			if _, ok, err := r.stores.casRouteBuildShard(ctx, current, revision); err != nil {
				return nil, err
			} else if ok {
				return current, nil
			}
			continue
		}
		current.State = BuildRegistered
		if _, ok, err := r.stores.casRouteBuildShard(ctx, current, revision); err != nil {
			return nil, err
		} else if ok {
			return current, nil
		}
	}
	return nil, fmt.Errorf("registry: build %s registration acceptance conflicted", buildID)
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

// updateBuildProjection merges a node event into a fresh record revision so it
// cannot erase registration fields committed concurrently by the command ACK.
func (r *Registry) updateBuildProjection(ctx context.Context, group, buildID, nodeID string, update func(*BuildRecord)) error {
	for attempt := 0; attempt < 5; attempt++ {
		rec, revision, found, err := r.stores.getRouteBuildShard(ctx, group, buildID)
		if err != nil {
			return err
		}
		if !found || (rec.NodeID != "" && nodeID != "" && rec.NodeID != nodeID) {
			return nil
		}
		update(rec)
		if _, ok, err := r.stores.casRouteBuildShard(ctx, rec, revision); err != nil {
			return err
		} else if ok {
			return nil
		}
	}
	return fmt.Errorf("build %s projection update conflicted", buildID)
}

// applyBuildUpsert converges the Registry's rebuildable routing/query projection
// from one retained node Build. Admission usage remains node-owned and comes
// from the node's durable heartbeat.
func (r *Registry) applyBuildUpsert(ctx context.Context, nodeID string, e *routesync.BuildEvent) error {
	if e == nil || e.BuildID == "" {
		return nil
	}
	switch BuildState(e.State) {
	case BuildRegistered, BuildWaiting, BuildBuilding, BuildReady, BuildError:
	default:
		return fmt.Errorf("invalid build %s projection state %q", e.BuildID, e.State)
	}
	ref, found, err := r.lookupNodeBuildRef(ctx, nodeID, e.BuildID)
	if err != nil {
		return fmt.Errorf("lookup build owner: %w", err)
	}
	if !found || (ref.TemplateID != "" && ref.TemplateID != e.TemplateID) {
		return nil
	}
	state := BuildState(e.State)
	update := func(writeCtx context.Context) error {
		return r.updateBuildProjection(writeCtx, ref.Group, e.BuildID, nodeID, func(rec *BuildRecord) {
			if e.TemplateID != "" && types.IsTransientID(rec.TemplateID) && rec.TemplateID != e.TemplateID {
				return
			}
			rec.State = state
			// A state event can outrun or survive loss of the synchronous registration
			// ACK. Keep the exact replay envelope until that ACK's accepted target is
			// durably recorded; markBuildRegistrationAccepted clears it atomically.
			if rec.RegistrationTargetSet {
				rec.RegistrationImageRepo = ""
				rec.RegistrationRegistryAuth = ""
			}
			if types.IsTransientID(e.TemplateID) {
				rec.TemplateID = e.TemplateID
			}
			if e.PersistID != "" {
				rec.PersistID = e.PersistID
			}
			rec.Reason = e.Reason
		})
	}
	terminal := state == BuildReady || state == BuildError
	var writeErr error
	if terminal {
		writeErr = retryTerminalBuildStore(ctx, update)
	} else {
		writeErr = update(ctx)
	}
	if writeErr != nil {
		return fmt.Errorf("persist build %s state %s: %w", e.BuildID, state, writeErr)
	}
	return nil
}

// applyBuildDelete removes only the projection bound to this exact node. The
// node's SQLite deletion is the lifecycle decision; Registry has no terminal
// timer of its own.
func (r *Registry) applyBuildDelete(ctx context.Context, nodeID, buildID, templateID string) error {
	if nodeID == "" || buildID == "" {
		return nil
	}
	for attempt := 0; attempt < 5; attempt++ {
		ref, refRevision, found, err := r.lookupNodeBuildRefVersion(ctx, nodeID, buildID)
		if err != nil {
			return fmt.Errorf("lookup build delete owner: %w", err)
		}
		if !found || (ref.TemplateID != "" && ref.TemplateID != templateID) {
			return nil
		}
		record, revision, recordFound, err := r.stores.getRouteBuildShard(ctx, ref.Group, buildID)
		if err != nil {
			return fmt.Errorf("read build delete projection: %w", err)
		}
		if recordFound && templateID != "" && record.TemplateID != templateID {
			return nil
		}
		if recordFound && record.NodeID == nodeID {
			// BuildStarting is the Registry's immutable pre-accept dispatch
			// intent, not a projection of a node row. An empty reconnect snapshot
			// cannot prove that an ambiguously delivered registration had no side
			// effect, so only a definitive command result may remove this binding.
			if record.State == BuildStarting {
				return nil
			}
			deleted, err := r.stores.deleteRouteBuildShardIfRevision(ctx, ref.Group, buildID, revision)
			if err != nil {
				return fmt.Errorf("delete build %s projection: %w", buildID, err)
			}
			if !deleted {
				continue
			}
		}
		// The route CAS and owner-ref CAS fence different shards. A same-ID
		// registration may recreate the route and refresh this ref between them;
		// deleting only the captured revision preserves that replacement.
		if _, err := r.stores.removeNodeBuildRefShardAtRevision(ctx, nodeID, buildID, refRevision); err != nil {
			return fmt.Errorf("remove build %s owner ref: %w", buildID, err)
		}
		return nil
	}
	return fmt.Errorf("delete build %s projection conflicted", buildID)
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

func (r *Registry) lookupNodeBuildRefVersion(ctx context.Context, nodeID, buildID string) (clusterstate.NodeBuildRef, uint64, bool, error) {
	var ref clusterstate.NodeBuildRef
	var revision uint64
	var found bool
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		ref, revision, found, err = r.stores.getNodeBuildRefShardVersion(ctx, nodeID, buildID)
		if err == nil || !transientRouteRead(err) {
			return ref, revision, found, err
		}
		select {
		case <-ctx.Done():
			return clusterstate.NodeBuildRef{}, 0, false, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		}
	}
	return ref, revision, found, err
}

// ResolveBuild maps a group's build_id to its node (router restart recovery: the
// router's in-memory build map is lost, but route_link replicated build state is
// still group-sharded).
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
	incomplete := false
	err := r.stores.RangeBuildsInGroup(ctx, group, func(b *BuildRecord) error {
		if types.ValidateTransientID(b.TemplateID) != nil {
			incomplete = true
		}
		if b.TemplateID == templateID {
			if match != nil {
				return fmt.Errorf("ambiguous transient build identity")
			}
			copy := *b
			match = &copy
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if match != nil {
		return r.resolveBuildRecord(ctx, match)
	}
	if incomplete {
		return nil, false, fmt.Errorf("build transient projection requires node full sync")
	}
	return nil, false, nil
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
