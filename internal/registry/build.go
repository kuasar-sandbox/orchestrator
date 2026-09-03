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
	BuildID     string        `json:"build_id"`
	TemplateID  string        `json:"template_id"`
	NodeID      string        `json:"node_id"`
	APIEndpoint string        `json:"api_endpoint"`
	Profile     types.Profile `json:"profile"`
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
	excluded := placementExclusions{}
	var lastFailure error
	if req.BuildID != "" {
		if rec, found, err := r.stores.GetBuildInGroup(ctx, req.Group, req.BuildID); err != nil {
			return nil, err
		} else if found {
			if !sameBuildRegistrationDefinition(rec, req.Profile, resources, req.Metadata, mmdsDigest, req.TemplateID) {
				return nil, fmt.Errorf("registry: build %s immutable definition conflicts with existing registration", req.BuildID)
			}
			if rec.State != BuildStarting {
				return r.buildReserveResult(ctx, rec), nil
			}
			rec.registrationMMDSSecrets = cloneStringMap(mmdsSecrets)
			// A prior dispatch had an ambiguous result. Re-establish the node
			// ownership index and replay the exact stored intent to the same node;
			// neither current placement nor Provider output may change it.
			if err := r.stores.AddNodeBuildRef(ctx, rec.NodeID, ref); err != nil {
				return nil, err
			}
			ack, err := r.dispatchBuildRegistration(ctx, rec)
			if err != nil {
				return nil, fmt.Errorf("registry: build_register remains ambiguous on node %s: %w", rec.NodeID, err)
			}
			if ack != nil && ack.Status == routesync.AckAccepted {
				registered, err := r.markBuildRegistrationAccepted(ctx, rec.Group, rec.BuildID, rec.NodeID)
				if err != nil {
					return nil, err
				}
				return r.buildReserveResult(ctx, registered), nil
			}
			if !definitiveBuildRegistrationRejection(ack) {
				return nil, ambiguousBuildRegistrationError(rec.NodeID, ack)
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
			APISecretFingerprint:         placement.APISecretFingerprint,
			Resources:                    cloneBuildResources(resources),
			RegistrationConfig:           cloneStringMap(req.Metadata),
			RegistrationMMDSValuesDigest: mmdsDigest,
			RegistrationImageRepo:        placement.ImageRepo,
			RegistrationRegistryAuth:     placement.RegistryAuth,
			State:                        BuildStarting, TemplateID: templateID,
			registrationMMDSSecrets: cloneStringMap(mmdsSecrets),
		}
		if err := r.stores.PutBuild(ctx, rec); err != nil {
			return nil, err
		}
		if err := r.stores.AddNodeBuildRef(ctx, id, ref); err != nil {
			_ = r.stores.DeleteBuild(ctx, req.Group, buildID)
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
		registered, err := r.markBuildRegistrationAccepted(ctx, rec.Group, rec.BuildID, rec.NodeID)
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

func sameBuildRegistrationDefinition(rec *BuildRecord, profile types.Profile, resources *routesync.BuildResources, config map[string]string, mmdsDigest, requestedTemplateID string) bool {
	return rec != nil && rec.Profile == profile && rec.Resources != nil && resources != nil &&
		*rec.Resources == *resources && maps.Equal(rec.RegistrationConfig, config) && rec.RegistrationMMDSValuesDigest == mmdsDigest &&
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
	cmd := &routesync.Command{
		CmdID: newID(), Kind: routesync.CmdBuildRegister,
		BuildID: rec.BuildID, TemplateRef: rec.TemplateID, Profile: string(rec.Profile),
		BuildResources: cloneBuildResources(rec.Resources), Config: cloneStringMap(rec.RegistrationConfig),
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

func (r *Registry) removeBuildRegistrationIntent(ctx context.Context, rec *BuildRecord) error {
	if err := r.stores.DeleteBuild(ctx, rec.Group, rec.BuildID); err != nil {
		return err
	}
	return r.stores.RemoveNodeBuildRef(ctx, rec.NodeID, rec.BuildID)
}

func (r *Registry) markBuildRegistrationAccepted(ctx context.Context, group, buildID, nodeID string) (*BuildRecord, error) {
	for attempt := 0; attempt < 5; attempt++ {
		current, revision, found, err := r.stores.getRouteBuildShard(ctx, group, buildID)
		if err != nil {
			return nil, err
		}
		if !found || current.NodeID != nodeID {
			return nil, fmt.Errorf("registry: build %s registration intent changed before acceptance", buildID)
		}
		if current.State != BuildStarting {
			return current, nil
		}
		current.State = BuildRegistered
		current.RegistrationImageRepo = ""
		current.RegistrationRegistryAuth = ""
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
	}
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
	if !found {
		return nil
	}
	rec, found, err := r.stores.GetBuildInGroup(ctx, ref.Group, e.BuildID)
	if err != nil {
		return fmt.Errorf("read build record: %w", err)
	}
	if !found {
		return nil
	}
	if rec.NodeID != "" && nodeID != "" && rec.NodeID != nodeID {
		return nil
	}
	rec.State = BuildState(e.State)
	rec.RegistrationImageRepo = ""
	rec.RegistrationRegistryAuth = ""
	if e.TemplateID != "" {
		rec.TemplateID = e.TemplateID
	}
	rec.Reason = e.Reason
	terminal := !rec.occupies()
	put := func(writeCtx context.Context) error { return r.stores.PutBuild(writeCtx, rec) }
	var writeErr error
	if terminal {
		writeErr = retryTerminalBuildStore(ctx, put)
	} else {
		writeErr = put(ctx)
	}
	if writeErr != nil {
		return fmt.Errorf("persist build %s state %s: %w", rec.BuildID, rec.State, writeErr)
	}
	return nil
}

// applyBuildDelete removes only the projection bound to this exact node. The
// node's SQLite deletion is the lifecycle decision; Registry has no terminal
// timer of its own.
func (r *Registry) applyBuildDelete(ctx context.Context, nodeID, buildID string) error {
	if nodeID == "" || buildID == "" {
		return nil
	}
	for attempt := 0; attempt < 5; attempt++ {
		ref, refRevision, found, err := r.lookupNodeBuildRefVersion(ctx, nodeID, buildID)
		if err != nil {
			return fmt.Errorf("lookup build delete owner: %w", err)
		}
		if !found {
			return nil
		}
		record, revision, recordFound, err := r.stores.getRouteBuildShard(ctx, ref.Group, buildID)
		if err != nil {
			return fmt.Errorf("read build delete projection: %w", err)
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
		if err := r.applyBuildDelete(ctx, nodeID, baseline.BuildID); err != nil {
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
func (r *Registry) ResolveBuild(ctx context.Context, group, buildID string) (*BuildReserveResult, bool) {
	b, found, err := r.stores.GetBuildInGroup(ctx, group, buildID)
	if err != nil || !found || b.State == BuildStarting {
		return nil, false
	}
	return &BuildReserveResult{
		BuildID: b.BuildID, TemplateID: b.TemplateID, NodeID: b.NodeID,
		APIEndpoint: r.nodeAPIEndpoint(ctx, b.NodeID), Profile: b.Profile,
	}, true
}
