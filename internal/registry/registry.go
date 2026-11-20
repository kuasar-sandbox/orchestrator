package registry

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/observebudget"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	nodestore "github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
)

// ErrNoNode is returned when no eligible node can host a sandbox.
var ErrNoNode = errors.New("registry: no eligible node")

// ErrNodeGone is returned when the node owner cannot reach the target node.
var ErrNodeGone = errors.New("registry: node owner cannot reach node")

var (
	errMissingImportSourceLease = errors.New("registry: import source lease fields are required")
	errInvalidSandboxConfig     = errors.New("registry: invalid sandbox config")
	errStaleImportSourceLease   = errors.New("registry: stale import source lease")
	ErrReserveBadRequest        = errors.New("registry: invalid reserve request")
	ErrReserveConflict          = errors.New("registry: retained sandbox occupies this route key")
	ErrReserveUnauthorized      = errors.New("registry: reserve credential is required")
	ErrReserveForbidden         = errors.New("registry: reserve credential is invalid")
	ErrSandboxNotFound          = errors.New("registry: sandbox not found")
)

const lifecycleAckTimeout = 5 * time.Second
const selectorPatchWriteConcurrency = 16

// nodeConn is the registry's handle to one connected node's channel — it sends
// commands toward the node. channel.go implements it over the wire; tests fake it.
type nodeConn interface {
	id() string
	send(*routesync.Command) error
}

// Placer suggests a node for a new sandbox: the placer suggests, the registry
// commits by CAS.
// PlaceRequest is a placement ask. Build marks a build placement (resource-aware,
// docs/cluster.md#groups-and-placement); TargetRuntimeDigest lets the placer prefer compatible
// node runtimes when the caller knows the required runtime identity.
type PlaceRequest struct {
	Group               string
	RouteKey            string
	SandboxID           string
	Config              map[string]string
	Build               bool
	BuildResources      *routesync.BuildResources
	TargetRuntimeDigest string
	ExcludeNodeIDs      []string
}

// Placement is a placer answer plus the group-derived material the registry must
// place on node commands.
type Placement struct {
	NodeID               string
	TemplateRef          string
	TargetPort           int
	Config               map[string]string
	APISecretFingerprint string
	ImageRepo            string
	RegistryAuth         string
}

// Placer suggests a node and group-derived create/build material. Production
// placement calls placer over placer_link; the builtin placer is the size-1/test
// fallback and only fills NodeID.
type Placer interface {
	Place(ctx context.Context, req PlaceRequest) (*Placement, error)
}

// Registry owns the typed route, node and import-source services and accepts
// node_link streams.
type Registry struct {
	stores      *Stores
	placer      Placer
	parkTimeout time.Duration
	log         *slog.Logger

	mu    sync.Mutex
	nodes map[string]nodeConn // node_id -> channel
	// buildFrameFences serialize Build projection mutation with replacement of
	// the active node-link session. Entries remain stable for the process
	// lifetime so a delayed frame can never escape onto a newly allocated lock.
	buildFrameFences map[string]*nodeBuildFrameFence
	inflight         map[string]*reserveCall
	reserveLeaders   chan struct{}                     // single-flight ReserveSandbox per (group,route_key)
	acks             map[string]chan *routesync.CmdAck // cmd_id -> command acknowledgement waiter

	localNodeOwner     NodeOwner
	nodeOwner          NodeOwner
	deadAfter          time.Duration
	reaperCtx          context.Context
	nodeLinkRelayMu    sync.RWMutex
	nodeLinkRelayPeers map[string]NodeLinkRelayPeer

	placerMu         sync.Mutex
	scaleReadyLabel  string
	placerLabel      string
	placerPeerSource PlacerPeerSource
	placerSeedJoiner PlacerSeedJoiner
	placerReplicas   int
	minReadyPlacers  int
	placerTimeout    time.Duration
}

type PlacerPeer struct {
	ID         string
	Advertise  string
	ReadyLabel string
}

type PlacerPeerSource func(readyLabel string) []PlacerPeer

type PlacerSeedJoiner func(ctx context.Context, id, label, advertise string) error

type ImportSourceLease struct {
	SourceID      string `json:"source_id"`
	OwnerID       string `json:"owner_id"`
	RunID         string `json:"run_id"`
	Term          uint64 `json:"term"`
	Cursor        string `json:"cursor,omitempty"`
	Round         uint64 `json:"round,omitempty"`
	NextRunUnixMs int64  `json:"next_run_unix_ms,omitempty"`
	LastError     string `json:"last_error,omitempty"`
	ExpiresUnixMs int64  `json:"expires_unix_ms"`
}

// reserveCall is one in-flight ReserveSandbox; joiners wait on done, the channel
// reader fills result + closes done when the sandbox goes running.
type reserveCall struct {
	done       chan struct{}
	finishOnce sync.Once
	result     *ReserveResult
	err        error
	// re-Place context (docs/cluster.md#route-link): a rejected create re-places once on another node
	// before failing the Reserve. orig is the pre-reserve record to restore on
	// timeout/reject while the row is still RESERVED.
	group, routeKey   string
	expectedSandboxID string
	orig              *SandboxRecord
	found             bool
	createConfig      map[string]string
	rollbackFence     reserveRollbackFence
}

type reserveRollbackFence struct {
	SandboxID         string
	NodeSandboxID     string
	SandboxGeneration uint64
	Revision          metadata.Version
}

func (f reserveRollbackFence) valid() bool {
	return f.SandboxID != "" && f.NodeSandboxID != "" && f.Revision.IsCommitted()
}

func (f reserveRollbackFence) matches(rec *SandboxRecord, revision metadata.Version) bool {
	return f.valid() && rec != nil && rec.State == StateReserved &&
		revision == f.Revision && rec.SandboxID == f.SandboxID &&
		rec.NodeSandboxID == f.NodeSandboxID && rec.SandboxGeneration == f.SandboxGeneration
}

// ReserveResult is the operation result returned to a trusted router. Route is
// always the complete authoritative forwarding record; operation-specific
// results are present only after their matching node command is accepted.
type ReserveResult struct {
	Route       RouteResolve                 `json:"route"`
	Connect     *routesync.ConnectResult     `json:"connect,omitempty"`
	ExecSession *routesync.ExecSessionResult `json:"exec_session,omitempty"`
}

// New builds a Registry. Production callers set a placer_link placer explicitly.
func New(stores *Stores, placer Placer, parkTimeout time.Duration, log *slog.Logger) *Registry {
	if placer == nil {
		placer = noPlacer{}
	}
	if parkTimeout <= 0 {
		parkTimeout = 30 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	r := &Registry{
		stores:           stores,
		placer:           placer,
		parkTimeout:      parkTimeout,
		log:              log,
		nodes:            make(map[string]nodeConn),
		buildFrameFences: make(map[string]*nodeBuildFrameFence),
		inflight:         make(map[string]*reserveCall),
		reserveLeaders:   make(chan struct{}, observebudget.DefaultReserveLeaderLimit),
		acks:             make(map[string]chan *routesync.CmdAck),
		placerReplicas:   1,
		minReadyPlacers:  1,
		placerTimeout:    2 * time.Second,
	}
	localOwner := newLocalNodeOwner(r)
	r.localNodeOwner = localOwner
	r.nodeOwner = localOwner
	return r
}

// applySelectorPatch renews the placer-selected node credential-pair cache. The
// placer owns selector/shuffle decisions; registry only writes the current key
// lease into node_link for the explicit nodes in the patch. Old keys are not
// actively deleted: node_link and node side TTLs expire entries that stop being
// renewed.
func (r *Registry) applySelectorPatch(ctx context.Context, p *routesync.SelectorPatch) error {
	if err := r.checkSelectorPatchLease(ctx, p); err != nil {
		return err
	}
	pair := clusterstate.NodeKeyPair{
		APISecretFingerprint:   p.APISecretFingerprint,
		APISecretType:          p.APISecretType,
		APISecret:              p.APISecret,
		APISecretRef:           p.APISecretRef,
		ManifestKeyFingerprint: p.ManifestKeyFingerprint,
		ManifestKeyType:        p.ManifestKeyType,
		ManifestKey:            p.ManifestKey,
		ManifestKeyRef:         p.ManifestKeyRef,
	}
	if selectorPatchPairEmpty(pair) {
		return nil
	}
	var err error
	pair, err = normalizeNodeKeyPair(pair)
	if err != nil {
		return err
	}
	if len(p.NodeIDs) == 0 {
		return nil
	}
	pair.ExpiresUnix = time.Now().Add(keyLeaseTTL).Unix()
	return r.putKeyPairTargets(ctx, p.NodeIDs, pair)
}

func selectorPatchPairEmpty(pair clusterstate.NodeKeyPair) bool {
	return pair.APISecretFingerprint == "" && pair.APISecretType == "" && pair.APISecret == "" && pair.APISecretRef == "" &&
		pair.ManifestKeyFingerprint == "" && pair.ManifestKeyType == "" && pair.ManifestKey == "" && pair.ManifestKeyRef == ""
}

func (r *Registry) putKeyPairTargets(ctx context.Context, nodeIDs []string, pair clusterstate.NodeKeyPair) error {
	if r.nodeOwner == nil || len(nodeIDs) == 0 {
		return nil
	}
	workers := len(nodeIDs)
	if workers > selectorPatchWriteConcurrency {
		workers = selectorPatchWriteConcurrency
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan string)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-workCtx.Done():
					return
				case nodeID, ok := <-jobs:
					if !ok {
						return
					}
					err := r.nodeOwner.PutKeyPair(workCtx, nodeID, pair)
					if err == nil || errors.Is(err, ErrNodeGone) {
						continue
					}
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
			}
		}()
	}
	seen := make(map[string]struct{}, len(nodeIDs))
sendTargets:
	for _, nodeID := range nodeIDs {
		if nodeID == "" {
			continue
		}
		if _, duplicate := seen[nodeID]; duplicate {
			continue
		}
		seen[nodeID] = struct{}{}
		select {
		case jobs <- nodeID:
		case <-workCtx.Done():
			break sendTargets
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return ctx.Err()
	}
}

func (r *Registry) checkSelectorPatchLease(ctx context.Context, p *routesync.SelectorPatch) error {
	if p == nil || p.ImportSourceID == "" || p.ImportOwnerID == "" || p.ImportRunID == "" || p.ImportTerm == 0 {
		return errMissingImportSourceLease
	}
	if !r.stores.CheckPlacerLinkSourceLease(ctx, p.ImportSourceID, p.ImportOwnerID, p.ImportRunID, p.ImportTerm) {
		return errStaleImportSourceLease
	}
	return nil
}

// SetPlacerReadyLabel sets the registry membership label a placer must advertise
// before it is used for Place requests.
func (r *Registry) SetPlacerReadyLabel(label string) {
	r.placerMu.Lock()
	r.scaleReadyLabel = label
	r.placerMu.Unlock()
}

func (r *Registry) SetPlacerMemberlistLabel(label string) {
	r.placerMu.Lock()
	r.placerLabel = label
	r.placerMu.Unlock()
}

func (r *Registry) placerMemberlistLabel() string {
	r.placerMu.Lock()
	defer r.placerMu.Unlock()
	return r.placerLabel
}

func (r *Registry) SetPlacerPeerSource(source PlacerPeerSource) {
	r.placerMu.Lock()
	r.placerPeerSource = source
	r.placerMu.Unlock()
}

func (r *Registry) SetPlacerSeedJoiner(joiner PlacerSeedJoiner) {
	r.placerMu.Lock()
	r.placerSeedJoiner = joiner
	r.placerMu.Unlock()
}

func (r *Registry) joinPlacerSeed(ctx context.Context, id, label, advertise string) error {
	r.placerMu.Lock()
	joiner := r.placerSeedJoiner
	r.placerMu.Unlock()
	if joiner == nil {
		return nil
	}
	return joiner(ctx, id, label, advertise)
}

func (r *Registry) SetPlacerPolicy(replicaCount, minReady int, timeout time.Duration) {
	if replicaCount <= 0 {
		replicaCount = 1
	}
	if minReady <= 0 {
		minReady = 1
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	r.placerMu.Lock()
	r.placerReplicas = replicaCount
	r.minReadyPlacers = minReady
	r.placerTimeout = timeout
	r.placerMu.Unlock()
}

func (r *Registry) scalePolicy() (replicaCount, minReady int, timeout time.Duration) {
	r.placerMu.Lock()
	defer r.placerMu.Unlock()
	return r.placerReplicas, r.minReadyPlacers, r.placerTimeout
}

func (r *Registry) readyPlacerPeers(time.Duration) []PlacerPeer {
	r.placerMu.Lock()
	source := r.placerPeerSource
	readyLabel := r.scaleReadyLabel
	r.placerMu.Unlock()
	if source == nil {
		return nil
	}
	src := source(readyLabel)
	out := make([]PlacerPeer, 0, len(src))
	for _, peer := range src {
		if peer.ID == "" || peer.Advertise == "" {
			continue
		}
		if readyLabel != "" && peer.ReadyLabel != "" && peer.ReadyLabel != readyLabel {
			continue
		}
		out = append(out, peer)
	}
	return out
}

// SetPlacer wires the placer implementation.
func (r *Registry) SetPlacer(p Placer) { r.placer = p }

// SetNodeOwner replaces the local node-owner adapter. Reserve/build/key/orphan
// flows stay the same; execution-state mutation moves behind this interface.
func (r *Registry) SetNodeOwner(owner NodeOwner) {
	r.nodeOwner = owner
}

func (r *Registry) SetRemoteNodeOwners(remotes map[string]NodeOwner) {
	r.nodeOwner = newRoutingNodeOwner(r, r.localNodeOwner, remotes)
}

func (r *Registry) SetNodeLinkRelayPeers(peers map[string]NodeLinkRelayPeer) {
	cp := make(map[string]NodeLinkRelayPeer, len(peers))
	for id, peer := range peers {
		if id == "" || peer.Endpoint == "" {
			continue
		}
		if peer.Client == nil {
			peer.Client = http.DefaultClient
		}
		peer.Endpoint = strings.TrimRight(peer.Endpoint, "/")
		peer.RedirectEndpoint = strings.TrimRight(peer.RedirectEndpoint, "/")
		cp[id] = peer
	}
	r.nodeLinkRelayMu.Lock()
	r.nodeLinkRelayPeers = cp
	r.nodeLinkRelayMu.Unlock()
}

func (r *Registry) LocalNodeOwner() NodeOwner { return r.localNodeOwner }

// Stores exposes the typed store layer used by registry route/node/build state.
func (r *Registry) Stores() *Stores { return r.stores }

func flightKey(group, routeKey string) string { return group + "\x00" + routeKey }

type createOptions struct {
	idleTimeout *int64
	workspaces  []workspace.Entry
}

// reserveCreate resolves (group, route_key) to a running sandbox, placing +
// creating (or resuming a PAUSED sandbox) on a node and waiting for the node to
// report it running, single-flight per key (cluster.md).
func (r *Registry) reserveCreate(ctx context.Context, group, routeKey string, createConfig map[string]string, autoPauseMemory, autoReset bool, options ...createOptions) (*ReserveResult, error) {
	var opts createOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if group == "" || routeKey == "" {
		return nil, fmt.Errorf("registry: group and route_key are required")
	}
	createConfig, createCredentials, err := normalizeSandboxReserveConfig(createConfig)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
	}
	rec, rev, found, err := r.getSandboxForReserve(ctx, group, routeKey)
	if err != nil {
		return nil, err
	}
	if found && (rec.State == StateDead || rec.State == StateDeleting) {
		return nil, ErrReserveConflict
	}
	if found && rec.State == StateReady {
		profile, err := types.ParseProfile(rec.Profile)
		if err != nil {
			return nil, fmt.Errorf("registry: invalid ready Sandbox profile: %w", err)
		}
		if err := validateTrafficConfigForProfile(createConfig, profile); err != nil {
			return nil, fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
		}
		res, live, err := r.readyResultFromRecord(ctx, rec, rev)
		if err != nil {
			return nil, err
		}
		if live {
			return res, nil
		}
		// Definitive node loss: fall through to re-place (dead-node sweep also resets it).
	}

	key := flightKey(group, routeKey)
	r.mu.Lock()
	if call, ok := r.inflight[key]; ok {
		r.mu.Unlock()
		return waitCall(ctx, call)
	}
	call := &reserveCall{done: make(chan struct{}), group: group, routeKey: routeKey, orig: rec, found: found, createConfig: createConfig}
	if found && rec != nil {
		call.expectedSandboxID = rec.SandboxID
	}
	r.inflight[key] = call
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.inflight, key)
		r.mu.Unlock()
	}()

	select {
	case r.reserveLeaders <- struct{}{}:
		defer func() { <-r.reserveLeaders }()
	default:
		err := &metadata.CapacityError{Resource: "reserve leaders"}
		r.finish(key, nil, err)
		return nil, err
	}
	ctx, releaseObservation, err := r.stores.observations.ReserveObservation(ctx)
	if err != nil {
		r.finish(key, nil, err)
		return nil, err
	}
	defer releaseObservation()
	if err := r.startReserve(ctx, group, routeKey, rec, rev, found, createConfig, createCredentials, autoPauseMemory, autoReset, opts); err != nil {
		r.finish(key, nil, err)
		if fence, ok := r.reserveRollbackFence(key); ok && !errors.Is(err, metadata.ErrUnknown) {
			dropCurrentRef := !errors.Is(err, errNodeSandboxIDConflict)
			r.rollbackReserveAtFence(group, routeKey, fence, rec, found, dropCurrentRef)
		}
		return waitCall(ctx, call)
	}
	if r.reserveExpectedSandboxID(key) == "" {
		current, _, currentFound, currentErr := r.getSandboxForReserve(ctx, group, routeKey)
		if currentErr != nil {
			r.finish(key, nil, currentErr)
			if fence, ok := r.reserveRollbackFence(key); ok {
				r.rollbackReserveAtFence(group, routeKey, fence, rec, found, false)
			}
			return waitCall(ctx, call)
		}
		if !currentFound || current.SandboxID == "" {
			r.finish(key, nil, ErrSandboxNotFound)
			if fence, ok := r.reserveRollbackFence(key); ok {
				r.rollbackReserveAtFence(group, routeKey, fence, rec, found, false)
			}
			return waitCall(ctx, call)
		}
		r.setReserveExpectedSandboxID(key, current.SandboxID)
	}
	wctx, cancel := context.WithTimeout(ctx, r.parkTimeout)
	defer cancel()
	res, rerr := r.waitReserveCall(wctx, call)
	if rerr != nil {
		// Park timeout / caller cancel: undo a RESERVED that never reached READY, so
		// it doesn't strand the route. Keep the ownership ref because command
		// delivery may have succeeded; a late live event must still be deleted as
		// registry-owned rather than ignored as a node-local sandbox.
		if fence, ok := r.reserveRollbackFence(key); ok && !errors.Is(rerr, metadata.ErrUnknown) && !errors.Is(rerr, metadata.ErrCapacity) {
			r.rollbackReserveAtFence(group, routeKey, fence, rec, found, false)
		}
		r.finish(key, nil, rerr)
		return nil, rerr
	}
	r.finish(key, res, nil)
	return res, nil
}

func (r *Registry) getSandboxForReserve(ctx context.Context, group, routeKey string) (*SandboxRecord, metadata.Version, bool, error) {
	for {
		rec, rev, found, err := r.stores.GetSandbox(ctx, group, routeKey)
		if !transientRouteRead(err) {
			return rec, rev, found, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, metadata.Version{}, false, ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *Registry) rollbackReserveAtFence(group, routeKey string, fence reserveRollbackFence, orig *SandboxRecord, found, dropCurrentRef bool) bool {
	if !fence.valid() {
		return false
	}
	ctx := context.Background()
	cur, rev, curFound, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil || !curFound || !fence.matches(cur, rev) {
		return false
	}
	return r.rollbackReservedAtRevision(ctx, group, routeKey, cur, rev, orig, found, dropCurrentRef)
}

func (r *Registry) rollbackReservedAtRevision(ctx context.Context, group, routeKey string, cur *SandboxRecord, rev metadata.Version, orig *SandboxRecord, found, dropCurrentRef bool) bool {
	if found && orig == nil {
		return false
	}
	var restore *SandboxRecord
	if found {
		copy := *orig
		restore = &copy
	}
	_, err := r.stores.routes.RollbackIntent(ctx, metadata.RouteKey{Group: group, RouteKey: routeKey}, rev, restore, dropCurrentRef)
	return err == nil
}

// startReserve drives the placement + command for the leader of a single-flight.
// It does not wait — the channel reader signals completion when the node reports
// the sandbox running (finish, via applyRoute).
func (r *Registry) startReserve(ctx context.Context, group, routeKey string, rec *SandboxRecord, rev metadata.Version, found bool, createConfig map[string]string, createCredentials *sandboxcfg.Credentials, autoPauseMemory, autoReset bool, options ...createOptions) error {
	var opts createOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if found && (rec.State == StateDead || rec.State == StateDeleting) {
		return ErrReserveConflict
	}
	// PAUSING/PAUSED keep their current owner. Connect waits behind the node's
	// Pause lifecycle lock; Registry must not replace a capture with a Create.
	if found && rec.State == StatePausing && rec.NodeID == "" {
		return ErrNodeGone
	}
	if found && (rec.State == StatePausing || rec.State == StatePaused) && rec.NodeID != "" {
		_, stableID, err := replacementCredentials(rec)
		if err != nil {
			return err
		}
		if r.nodeOwner == nil {
			return ErrNodeGone
		}
		reserved := *rec
		reservedRev := rev
		if rec.State == StatePaused {
			reserved.State = StateReserved
			var ok bool
			reservedRev, ok, err = r.stores.commitSandboxIntent(ctx, &reserved, rev)
			if err != nil || !ok {
				return cas(err, ok)
			}
			r.setReserveRollbackFence(flightKey(group, routeKey), &reserved, reservedRev)

		}
		ccmd := &routesync.Command{
			CmdID: newID(), Kind: routesync.CmdConnect, SID: rec.NodeSandboxID,
			Profile: rec.Profile,
			Cluster: &routesync.ClusterSandboxContext{
				Group: group, RouteKey: routeKey, StableID: stableID,
			},
			APISecretFingerprint: rec.APISecretFingerprint,
		}
		ack, err := r.nodeOwner.SendCommandAndWait(ctx, rec.NodeID, ccmd, lifecycleAckTimeout)
		if err != nil {
			if rec.State == StatePaused {
				_ = r.rollbackReservedAtRevision(ctx, group, routeKey, &reserved, reservedRev, rec, true, true)
			}
			return err
		}
		if ack == nil || ack.Status != routesync.AckAccepted {
			if rec.State == StatePaused {
				_ = r.rollbackReservedAtRevision(ctx, group, routeKey, &reserved, reservedRev, rec, true, true)
			}
			reason := ""
			if ack != nil {
				reason = ack.Reason
			}
			return fmt.Errorf("registry: connect rejected: %s", reason)
		}
		return nil
	}

	// NONE: place + create.
	var replaceReady *SandboxRecord
	if found && rec != nil && rec.State == StateReady {
		copy := *rec
		replaceReady = &copy
	}
	return r.placeAndCreate(ctx, group, routeKey, createConfig, createCredentials, replaceReady, autoPauseMemory, autoReset, opts)
}

// placeAndCreate places a node and sends the create command, CASing the RESERVED
// record. node_list is only a catalog: the selected node owner validates its
// live connection before commit. An unusable candidate is excluded from the next
// placement request so stale catalog entries cannot prevent a live alternative.
func (r *Registry) placeAndCreate(ctx context.Context, group, routeKey string, createConfig map[string]string, createCredentials *sandboxcfg.Credentials, replaceReady *SandboxRecord, autoPauseMemory, autoReset bool, options ...createOptions) error {
	var opts createOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if _, found := createConfig[sandboxcfg.NsCredentials]; found {
		return errors.New("registry: create credentials were not separated from config")
	}
	idleTimeout := types.CloneIdleTimeout(opts.idleTimeout)
	workspaceDigest, err := workspace.InputDigest(opts.workspaces)
	if err != nil {
		return fmt.Errorf("%w: invalid workspace declaration", errInvalidSandboxConfig)
	}
	workflowCredentials := cloneCreateCredentials(createCredentials)
	workflowReplacement := replaceReady
	sandboxID := "sb-" + newID()
	nextGeneration := uint64(0)
	if replaceReady != nil {
		if !validNodeSandboxIdentity(replaceReady.SandboxID, replaceReady.NodeSandboxID, replaceReady.SandboxGeneration) ||
			replaceReady.NextSandboxGeneration <= replaceReady.SandboxGeneration {
			return errors.New("registry: replacement sandbox identity is invalid")
		}
		sandboxID = replaceReady.SandboxID
		nextGeneration = replaceReady.NextSandboxGeneration
		var err error
		workflowCredentials, _, err = replacementCredentials(replaceReady)
		if err != nil {
			return err
		}
		// A Ready replacement continues the same logical sandbox. Freeze the
		// original TTL capture policy together with its stable identity and
		// materialized credentials instead of adopting a retrying Create body.
		autoPauseMemory = replaceReady.AutoPauseMemory
		idleTimeout = types.CloneIdleTimeout(replaceReady.IdleTimeout)
		autoReset = replaceReady.AutoReset
	}
	excluded := placementExclusions{}
	casConflicts := 0
	var lastFailure error
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		cur, curRev, curFound, err := r.stores.GetSandbox(ctx, group, routeKey)
		if err != nil {
			return err
		}
		if curFound && (cur.State == StateDead || cur.State == StateDeleting) {
			return ErrReserveConflict
		}
		if curFound && cur.State == StatePausing {
			return r.startReserve(ctx, group, routeKey, cur, curRev, true, createConfig, createCredentials, autoPauseMemory, autoReset, opts)
		}
		if curFound && cur.State == StateReady && !sameSandboxGeneration(cur, replaceReady) {
			return nil // a concurrent replacement already won; the running route finishes the Reserve
		}
		if curFound {
			if cur.SandboxID == "" {
				return errors.New("registry: sandbox route is missing stable identity")
			}
			if replaceReady != nil && cur.SandboxID != replaceReady.SandboxID {
				return errors.New("registry: replacement stable sandbox identity mismatch")
			}
			sandboxID = cur.SandboxID
			if cur.NextSandboxGeneration > nextGeneration {
				nextGeneration = cur.NextSandboxGeneration
			}
		}
		selectedCredentials := cloneCreateCredentials(workflowCredentials)
		selectedReplacement := workflowReplacement
		binding := workflowReplacement
		if curFound && cur.State == StateReserved {
			// The first committed RESERVED row freezes the create credential
			// selection and TTL capture policy. A retry or competing Reserve must
			// not adopt either value from its own request after that point.
			if cur.WorkspacesDigest != workspaceDigest {
				// Another request owns this reservation. Wait for its authoritative route.
				return nil
			}
			binding = cur
			autoPauseMemory = cur.AutoPauseMemory
			idleTimeout = types.CloneIdleTimeout(cur.IdleTimeout)
			autoReset = cur.AutoReset
			if hasRouteCredentials(cur) {
				selectedCredentials, _, err = replacementCredentials(cur)
				if err != nil {
					return err
				}
				selectedReplacement = cur
			} else {
				selectedCredentials = cloneCreateCredentials(cur.CreateCredentials)
				selectedReplacement = nil
			}
		}
		placement, perr := r.placer.Place(ctx, PlaceRequest{
			Group: group, RouteKey: routeKey, SandboxID: sandboxID, Config: createConfig,
			ExcludeNodeIDs: excluded.values(),
		})
		if perr != nil {
			if errors.Is(perr, ErrNoNode) && lastFailure != nil {
				return lastFailure
			}
			return perr
		}
		if placement == nil || placement.NodeID == "" {
			return ErrNoNode
		}
		if !validFullFingerprint(placement.APISecretFingerprint) {
			return errors.New("registry: placement is missing a valid API secret fingerprint")
		}
		template, err := types.ParseTemplateID(placement.TemplateRef)
		if err != nil {
			return fmt.Errorf("registry: invalid placement template: %w", err)
		}
		if binding != nil && placement.APISecretFingerprint != binding.APISecretFingerprint {
			return errors.New("registry: replacement credential binding mismatch")
		}
		if binding != nil && string(template.Profile) != binding.Profile {
			return errors.New("registry: replacement profile mismatch")
		}
		config, err := sandboxcfg.MergeCreateMetadata(placement.Config, createConfig)
		if err != nil {
			return fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
		}
		if err := validateTrafficConfigForProfile(config, template.Profile); err != nil {
			return fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
		}
		config, err = attachCreateCredentials(config, template.Profile, selectedCredentials)
		if err != nil {
			return fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
		}
		nodeID := placement.NodeID
		if excluded.has(nodeID) {
			if lastFailure != nil {
				return lastFailure
			}
			return ErrNoNode
		}
		if err := r.nodeRuntimeLive(ctx, nodeID); err != nil {
			if !errors.Is(err, ErrNodeGone) {
				return err
			}
			lastFailure = err
			excluded.add(nodeID)
			continue
		}
		if nextGeneration == ^uint64(0) {
			return errors.New("registry: sandbox generation exhausted")
		}
		generation := nextGeneration
		nodeSandboxID := EncodeNodeSandboxID(sandboxID, generation)
		if !validNodeSandboxIdentity(sandboxID, nodeSandboxID, generation) {
			return errors.New("registry: generated node sandbox identity is invalid")
		}
		stableID := sandboxID
		if selectedReplacement != nil {
			stableID = selectedReplacement.StableID
		}
		if stableID != sandboxID {
			return errors.New("registry: stable ID does not match SandboxID")
		}
		cmd := &routesync.Command{
			CmdID: newID(), Kind: routesync.CmdCreate, SID: nodeSandboxID,
			TemplateRef: placement.TemplateRef, Profile: string(template.Profile), Config: config, Workspaces: opts.workspaces,
			Cluster: &routesync.ClusterSandboxContext{
				Group: group, RouteKey: routeKey, StableID: stableID,
			},
			APISecretFingerprint: placement.APISecretFingerprint,
			AutoPauseMemory:      cloneOptionalBool(&autoPauseMemory), IdleTimeout: types.CloneIdleTimeout(idleTimeout),
			AutoReset: cloneOptionalBool(&autoReset),
		}
		if err := workspace.CheckEnvelope(config, opts.workspaces); err != nil {
			return fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
		}
		if err := routesync.ValidateMessage(&routesync.Msg{Type: routesync.TypeCommand, Cmd: cmd}); err != nil {
			return fmt.Errorf("%w: %v", errInvalidSandboxConfig, err)
		}
		expect := metadata.Version{}
		if curFound {
			expect = curRev
		}
		reserved := &SandboxRecord{
			Group: group, RouteKey: routeKey,
			SandboxID: sandboxID, NodeSandboxID: nodeSandboxID,
			SandboxGeneration: generation, NextSandboxGeneration: generation + 1,
			State: StateReserved, NodeID: nodeID,
			TemplateID: placement.TemplateRef, Profile: string(template.Profile),
			APISecretFingerprint: placement.APISecretFingerprint,
			CreateCredentials:    cloneCreateCredentials(selectedCredentials), WorkspacesDigest: workspaceDigest,
			TargetPort:      placement.TargetPort,
			AutoPauseMemory: autoPauseMemory, IdleTimeout: types.CloneIdleTimeout(idleTimeout),
			AutoReset: autoReset,
		}
		copyRouteCredentials(reserved, selectedReplacement)
		reservedRev, ok, cerr := r.stores.commitSandboxIntent(ctx, reserved, expect)
		if cerr != nil {
			if errors.Is(cerr, metadata.ErrOwnershipConflict) {
				// The transaction definitely aborted. Keep the stable identity,
				// advance its node generation and retry placement on this node.
				nextGeneration = generation + 1
				continue
			}
			return cerr
		} else if !ok {
			if casConflicts == 0 {
				casConflicts++
				continue
			}
			return ErrNoNode
		}
		r.setReserveRollbackFence(flightKey(group, routeKey), reserved, reservedRev)
		nextGeneration = reserved.NextSandboxGeneration
		workflowCredentials = cloneCreateCredentials(selectedCredentials)
		if hasRouteCredentials(reserved) {
			copy := *reserved
			copy.CreateCredentials = cloneCreateCredentials(reserved.CreateCredentials)
			workflowReplacement = &copy
		} else {
			workflowReplacement = nil
		}

		if r.nodeOwner == nil {
			if !r.rollbackReservedAtRevision(ctx, group, routeKey, reserved, reservedRev,
				replaceReady, replaceReady != nil, true) {
				return ErrNodeGone
			}
			lastFailure = ErrNodeGone
			excluded.add(nodeID)
			continue
		}
		ack, err := r.nodeOwner.SendCommandAndWait(ctx, nodeID, cmd, lifecycleAckTimeout)
		if commandAckTimedOut(ctx, err) {
			return nil // delivery is ambiguous; wait for the authoritative route event
		}
		if err != nil {
			if !errors.Is(err, ErrNodeGone) {
				return nil // delivery is ambiguous; keep RESERVED and wait for the route event
			}
			if !r.rollbackReservedAtRevision(ctx, group, routeKey, reserved, reservedRev,
				replaceReady, replaceReady != nil, true) {
				return err
			}
			lastFailure = err
			excluded.add(nodeID)
			continue
		}
		if ack == nil || ack.Status != routesync.AckAccepted {
			reason := ""
			if ack != nil {
				reason = ack.Reason
			}
			lastFailure = fmt.Errorf("registry: create rejected: %s", reason)
			if !r.rollbackReservedAtRevision(ctx, group, routeKey, reserved, reservedRev,
				replaceReady, replaceReady != nil, true) {
				return lastFailure
			}
			excluded.add(nodeID)
			continue
		}
		return nil
	}
}

// normalizeSandboxReserveConfig validates request-scoped namespaces before any
// READY fast path, placement, route mutation, or lifecycle command. Credentials
// are parsed once and removed from ordinary config at this boundary.
func normalizeSandboxReserveConfig(config map[string]string) (map[string]string, *sandboxcfg.Credentials, error) {
	config, err := sandboxcfg.NormalizeRestoreMetadata(config)
	if err != nil {
		return nil, nil, err
	}
	config, err = sandboxcfg.NormalizeResourceMetadata(config)
	if err != nil {
		return nil, nil, err
	}
	config, err = sandboxcfg.NormalizeCheckpointMetadata(config)
	if err != nil {
		return nil, nil, err
	}
	config, err = sandboxcfg.NormalizeTrafficMetadata(config)
	if err != nil {
		return nil, nil, err
	}
	_, present := config[sandboxcfg.NsCredentials]
	credentials, cleaned, err := sandboxcfg.ExtractCredentials(config)
	if err != nil {
		return nil, nil, err
	}
	if !present {
		return cleaned, nil, nil
	}
	return cleaned, &credentials, nil
}

func validateTrafficConfigForProfile(config map[string]string, profile types.Profile) error {
	raw, present := config[sandboxcfg.NsTraffic]
	if !present {
		return nil
	}
	patch, err := sandboxcfg.ParseTrafficPatch(raw)
	if err != nil {
		return err
	}
	return sandboxcfg.ValidateTrafficForProfile(profile, patch)
}

func attachCreateCredentials(config map[string]string, profile types.Profile, credentials *sandboxcfg.Credentials) (map[string]string, error) {
	if credentials == nil {
		return config, nil
	}
	if err := sandboxcfg.ValidateCredentialsForProfile(profile, *credentials); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(credentials)
	if err != nil {
		return nil, errors.New("registry: encode sandbox credentials")
	}
	if config == nil {
		config = map[string]string{}
	}
	config[sandboxcfg.NsCredentials] = string(canonical)
	return config, nil
}

func cloneCreateCredentials(credentials *sandboxcfg.Credentials) *sandboxcfg.Credentials {
	if credentials == nil {
		return nil
	}
	clone := *credentials
	return &clone
}

func replacementCredentials(record *SandboxRecord) (*sandboxcfg.Credentials, string, error) {
	if record == nil || !hasRouteCredentials(record) {
		return nil, "", errors.New("registry: replacement route credentials are unavailable")
	}
	route := &routesync.RouteEntry{
		Profile: record.Profile, StableID: record.StableID,
		APISecret: record.APISecret, APISecretFingerprint: record.APISecretFingerprint,
		ManifestKeyFingerprint: record.ManifestKeyFingerprint,
		ServiceSecret:          record.ServiceSecret, EnvdAccessToken: record.EnvdAccessToken,
		TrafficAccessToken: record.TrafficAccessToken, ForwardAccessToken: record.ForwardAccessToken,
	}
	if err := validateRouteCredentials(route, record.APISecretFingerprint, record.SandboxID); err != nil {
		return nil, "", errors.New("registry: replacement route credentials are invalid")
	}
	return &sandboxcfg.Credentials{
		ServiceSecret: record.ServiceSecret, EnvdAccessToken: record.EnvdAccessToken,
		TrafficAccessToken: record.TrafficAccessToken,
	}, record.StableID, nil
}

func copyRouteCredentials(dst, src *SandboxRecord) {
	if dst == nil || src == nil {
		return
	}
	dst.APISecretFingerprint = src.APISecretFingerprint
	dst.ManifestKeyFingerprint = src.ManifestKeyFingerprint
	dst.StableID = src.StableID
	dst.APISecret = src.APISecret
	dst.ServiceSecret = src.ServiceSecret
	dst.EnvdAccessToken = src.EnvdAccessToken
	dst.TrafficAccessToken = src.TrafficAccessToken
	dst.ForwardAccessToken = src.ForwardAccessToken
}

// sendAndWait sends a command and blocks until the node acknowledges receipt or
// the caller's context/timeout ends.
func (r *Registry) sendAndWait(ctx context.Context, conn nodeConn, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error) {
	ch := make(chan *routesync.CmdAck, 1)
	r.mu.Lock()
	r.acks[cmd.CmdID] = ch
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.acks, cmd.CmdID)
		r.mu.Unlock()
	}()
	if err := conn.send(cmd); err != nil {
		return nil, err
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case ack := <-ch:
		return ack, nil
	case <-wctx.Done():
		return nil, wctx.Err()
	}
}

// ackCommand handles a node's CmdAck and wakes the matching SendCommandAndWait
// waiter. Rejection/retry policy is handled by the command caller.
func (r *Registry) ackCommand(ack *routesync.CmdAck) {
	if ack == nil {
		return
	}
	r.mu.Lock()
	ch, isWait := r.acks[ack.CmdID]
	r.mu.Unlock()
	if isWait {
		select {
		case ch <- ack:
		default:
		}
	}
}

// applyRoute is called by the channel reader for each sandbox route the node
// streams up. It converges SandboxStore and, on a running route, satisfies a
// waiting ReserveSandbox (docs/cluster.md#route-link).
func (r *Registry) applyRoute(ctx context.Context, nodeID string, e *routesync.RouteEntry) {
	if e == nil || e.SandboxID == "" {
		return
	}
	ref, found, err := r.lookupNodeSandboxRef(ctx, nodeID, e.SandboxID)
	if err != nil {
		r.log.Warn("registry: lookup node sandbox", "node", nodeID, "sid", e.SandboxID, "err", err)
		return
	}
	if !found {
		// The node may host sandboxes created outside the cluster route plane.
		// Only an ownership ref authorizes this registry to mutate one.
		return
	}
	if e.State == routesync.StateStarting {
		// Keep the Registry-owned reservation/paused state while the node makes
		// starting visible to its proxy MMDS. Only running/paused are terminal
		// route facts for the cluster state machine.
		return
	}
	rec := &SandboxRecord{
		InstanceID: e.InstanceID, StateVersion: e.PressureVersion,
		Group: ref.Group, RouteKey: ref.RouteKey,
		SandboxID: ref.SandboxID, NodeSandboxID: ref.NodeSandboxID,
		SandboxGeneration: ref.SandboxGeneration, NodeID: nodeID,
		ArtifactLocation: e.ArtifactLocation, TemplateID: e.TemplateID, Profile: ref.Profile,
		APISecretFingerprint:   ref.APISecretFingerprint,
		ManifestKeyFingerprint: e.ManifestKeyFingerprint,
		StableID:               e.StableID,
		APISecret:              e.APISecret,
		ServiceSecret:          e.ServiceSecret,
		EnvdAccessToken:        e.EnvdAccessToken,
		TrafficAccessToken:     e.TrafficAccessToken,
		ForwardAccessToken:     e.ForwardAccessToken,
		LastActive:             time.Now().Unix(),
	}
	switch e.State {
	case routesync.StateRunning:
		rec.State = StateReady
	case routesync.StatePausing:
		rec.State = StatePausing
	case routesync.StatePaused:
		rec.State = StatePaused
	case routesync.StateDead:
		rec.State = StateDead
	case routesync.StateDeleting:
		rec.State = StateDeleting
	default:
		r.log.Warn("registry: ignored unknown route state", "node", nodeID, "sid", e.SandboxID, "state", e.State)
		return
	}
	r.applyLiveRoute(ctx, nodeID, e, rec)
}

func validateRouteCredentials(e *routesync.RouteEntry, expectedAPISecretFingerprint, expectedStableID string) error {
	apiSecretFingerprint, err := nodestore.APISecretHash(e.APISecret)
	if err != nil || apiSecretFingerprint != e.APISecretFingerprint ||
		apiSecretFingerprint != expectedAPISecretFingerprint {
		return errors.New("registry: route API secret binding is invalid")
	}
	if !validFullFingerprint(e.ManifestKeyFingerprint) {
		return errors.New("registry: route manifest key fingerprint is invalid")
	}
	if e.StableID == "" || !utf8.ValidString(e.StableID) {
		return errors.New("registry: route stable ID is invalid")
	}
	if expectedStableID != "" && e.StableID != expectedStableID {
		return errors.New("registry: route stable ID binding is invalid")
	}
	if err := keys.VerifyForwardAccessToken(e.ForwardAccessToken, e.ServiceSecret, e.StableID); err != nil {
		return errors.New("registry: route forward credential is invalid")
	}
	if !sandboxcfg.ValidE2BAccessToken(e.EnvdAccessToken) ||
		!sandboxcfg.ValidE2BAccessToken(e.TrafficAccessToken) {
		return errors.New("registry: route e2b credential is invalid")
	}
	switch types.Profile(e.Profile) {
	case types.ProfileE2B:
		if e.EnvdAccessToken == "" || e.TrafficAccessToken == "" {
			return errors.New("registry: route e2b credentials are required")
		}
	case types.ProfileBare:
		if e.EnvdAccessToken != "" || e.TrafficAccessToken != "" {
			return errors.New("registry: bare route contains e2b credentials")
		}
	default:
		return errors.New("registry: route profile is invalid")
	}
	return nil
}

func sameRouteCredentials(a, b *SandboxRecord) bool {
	return a.APISecretFingerprint == b.APISecretFingerprint &&
		a.ManifestKeyFingerprint == b.ManifestKeyFingerprint &&
		a.StableID == b.StableID &&
		constantTimeStringEqual(a.APISecret, b.APISecret) &&
		constantTimeStringEqual(a.ServiceSecret, b.ServiceSecret) &&
		constantTimeStringEqual(a.EnvdAccessToken, b.EnvdAccessToken) &&
		constantTimeStringEqual(a.TrafficAccessToken, b.TrafficAccessToken) &&
		constantTimeStringEqual(a.ForwardAccessToken, b.ForwardAccessToken)
}

func constantTimeStringEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func hasRouteCredentials(record *SandboxRecord) bool {
	return record != nil && (record.ManifestKeyFingerprint != "" ||
		record.StableID != "" || record.APISecret != "" ||
		record.ServiceSecret != "" || record.EnvdAccessToken != "" ||
		record.TrafficAccessToken != "" || record.ForwardAccessToken != "")
}

func (r *Registry) lookupNodeSandboxRef(ctx context.Context, nodeID, nodeSandboxID string) (clusterstate.NodeSandboxRef, bool, error) {
	var ref clusterstate.NodeSandboxRef
	var found bool
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		ref, found, err = r.stores.GetNodeSandboxRef(ctx, nodeID, nodeSandboxID)
		if err == nil || !transientRouteRead(err) {
			return ref, found, err
		}
		select {
		case <-ctx.Done():
			return clusterstate.NodeSandboxRef{}, false, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		}
	}
	return ref, found, err
}

func (r *Registry) applyLiveRoute(ctx context.Context, nodeID string, e *routesync.RouteEntry, rec *SandboxRecord) {
	const attempts = 5
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(attempt) * 10 * time.Millisecond):
			}
		}
		if r.tryApplyLiveRoute(ctx, nodeID, e, rec) {
			return
		}
	}
	r.log.Warn("registry: route report did not converge", "group", rec.Group, "route_key", rec.RouteKey, "sid", e.SandboxID)
}

func (r *Registry) tryApplyLiveRoute(ctx context.Context, nodeID string, e *routesync.RouteEntry, rec *SandboxRecord) bool {
	ref := metadata.NodeSandboxRef{Group: rec.Group, RouteKey: rec.RouteKey, SandboxID: rec.SandboxID, NodeSandboxID: rec.NodeSandboxID, SandboxGeneration: rec.SandboxGeneration, Profile: rec.Profile, APISecretFingerprint: rec.APISecretFingerprint}
	registration, err := nodeStreamRegistration(ctx, nodeID)
	if err != nil {
		return true
	}
	result, err := r.stores.routes.ApplyRouteReport(ctx, registration, ref, *e)
	if err != nil {
		r.log.Warn("registry: apply route report", "group", rec.Group, "route_key", rec.RouteKey, "err", err)
		return true
	}
	switch result.Disposition {
	case metadata.ReportOrphan:
		r.deleteOrphanSandbox(ctx, nodeID, e.SandboxID, ref.Group, ref.RouteKey, ref.APISecretFingerprint)
	case metadata.ReportWritten:
		next := result.Record.Value
		if next.State == StateReady {
			route, err := r.routeFromRecord(ctx, &next, result.Record.Version)
			if err != nil {
				r.finish(flightKey(next.Group, next.RouteKey), nil, err)
			} else {
				r.finish(flightKey(next.Group, next.RouteKey), &ReserveResult{Route: *route}, nil)
			}
		} else if next.State == StateDead || next.State == StateDeleting {
			r.finish(flightKey(next.Group, next.RouteKey), nil, ErrReserveConflict)
		}
	}
	return true
}

func (r *Registry) deleteOrphanSandbox(ctx context.Context, nodeID, sid, group, routeKey, apiSecretFingerprint string) {
	if r.nodeOwner == nil || sid == "" {
		return
	}
	if err := r.nodeOwner.DeleteSandbox(ctx, nodeID, sid, apiSecretFingerprint); err != nil && !errors.Is(err, ErrNodeGone) {
		r.log.Warn("registry: delete orphan sandbox", "node", nodeID, "sid", sid, "group", group, "route_key", routeKey, "err", err)
	}
}

// applyDelete converges a removed route only while it still names the reporting
// node and sandbox. A late DEAD from a replaced instance must not delete its
// successor's route.
func (r *Registry) applyDelete(ctx context.Context, nodeID, nodeSandboxID, group, routeKey, apiSecretFingerprint string) bool {
	if nodeID == "" || nodeSandboxID == "" || group == "" || routeKey == "" || !validFullFingerprint(apiSecretFingerprint) {
		return false
	}
	// A conflict can be replayed by the next authoritative node snapshot. In
	// particular, an Unknown must not be reread as absence and clear ownership.
	return r.tryApplyDelete(ctx, nodeID, nodeSandboxID, group, routeKey, apiSecretFingerprint)
}

func (r *Registry) tryApplyDelete(ctx context.Context, nodeID, nodeSandboxID, group, routeKey, apiSecretFingerprint string) bool {
	rec, rev, found, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil {
		return false
	}
	if found && rec.NodeID == nodeID && rec.NodeSandboxID == nodeSandboxID && rec.APISecretFingerprint == apiSecretFingerprint {
		if applies, rolledBack := r.tryRollbackCreateReserveDelete(ctx, group, routeKey, rec, rev); applies {
			return rolledBack
		}
	}
	ref, refFound, err := r.stores.GetNodeSandboxRef(ctx, nodeID, nodeSandboxID)
	if err != nil {
		return false
	}
	if !refFound {
		return true
	}
	if ref.Group != group || ref.RouteKey != routeKey || ref.APISecretFingerprint != apiSecretFingerprint {
		return true
	}
	registration, err := nodeStreamRegistration(ctx, nodeID)
	if err != nil {
		return false
	}
	err = r.stores.routes.ApplyRouteDelete(ctx, registration, rev, metadata.NodeSandboxRef(ref), nil)
	return err == nil
}

// tryRollbackCreateReserveDelete preserves the pre-reserve route when a node
// reports that its accepted create candidate failed. Deleting the matching
// RESERVED row first would destroy the revision fence that reserveCreate later
// uses to restore a replaced route (or remove a fresh reservation). A paused
// resume is excluded: its ordinary launch failure reports paused, while an
// actual Delete remains authoritative for that existing sandbox.
func (r *Registry) tryRollbackCreateReserveDelete(ctx context.Context, group, routeKey string, rec *SandboxRecord, rev metadata.Version) (applies, rolledBack bool) {
	if rec == nil || rec.State != StateReserved {
		return false, false
	}
	key := flightKey(group, routeKey)
	r.mu.Lock()
	call := r.inflight[key]
	if call == nil || !call.rollbackFence.matches(rec, rev) || (call.orig != nil && call.orig.State == StatePaused) {
		r.mu.Unlock()
		return false, false
	}
	orig, found := call.orig, call.found
	r.mu.Unlock()
	registration, err := nodeStreamRegistration(ctx, rec.NodeID)
	if err != nil {
		return true, false
	}
	var restore *SandboxRecord
	if found {
		restore = orig
	}
	err = r.stores.routes.ApplyRouteDelete(ctx, registration, rev, metadata.SandboxRef(*rec), restore)
	return true, err == nil
}

func (r *Registry) applyNodeFullSnapshot(ctx context.Context, nodeID string, expected []clusterstate.NodeSandboxRef, seen map[string]struct{}) {
	if nodeID == "" {
		return
	}
	for _, ref := range expected {
		if ref.Group == "" || ref.RouteKey == "" ||
			!validNodeSandboxIdentity(ref.SandboxID, ref.NodeSandboxID, ref.SandboxGeneration) ||
			!validFullFingerprint(ref.APISecretFingerprint) || !types.Profile(ref.Profile).Valid() {
			continue
		}
		if _, ok := seen[ref.NodeSandboxID]; ok {
			continue
		}
		current, found, err := r.stores.GetNodeSandboxRef(ctx, nodeID, ref.NodeSandboxID)
		if err != nil || !found || current.Group != ref.Group || current.RouteKey != ref.RouteKey ||
			current.SandboxID != ref.SandboxID || current.SandboxGeneration != ref.SandboxGeneration ||
			current.Profile != ref.Profile || current.APISecretFingerprint != ref.APISecretFingerprint {
			continue
		}
		r.applyDelete(ctx, nodeID, ref.NodeSandboxID, ref.Group, ref.RouteKey, ref.APISecretFingerprint)
	}
}

// applyDeleteBySID converges a route delete. The node-link owner resolves the
// node-local sandbox ID through the per-node ownership table.
func (r *Registry) applyDeleteBySID(ctx context.Context, nodeID, nodeSandboxID string) {
	ref, found, err := r.lookupNodeSandboxRef(ctx, nodeID, nodeSandboxID)
	if err != nil || !found {
		return
	}
	r.applyDelete(ctx, nodeID, nodeSandboxID, ref.Group, ref.RouteKey, ref.APISecretFingerprint)
}

// finish satisfies the single-flight call for key (idempotent — first wins). It
func (r *Registry) finish(key string, res *ReserveResult, err error) {
	r.mu.Lock()
	call := r.inflight[key]
	if call != nil && res != nil && call.expectedSandboxID != "" && res.Route.SandboxID != call.expectedSandboxID {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	if call == nil {
		return
	}
	call.finishOnce.Do(func() {
		call.result, call.err = res, err
		close(call.done)
	})
}

func (r *Registry) setReserveRollbackFence(key string, rec *SandboxRecord, revision metadata.Version) {
	fence := reserveRollbackFence{Revision: revision}
	if rec != nil {
		fence.SandboxID = rec.SandboxID
		fence.NodeSandboxID = rec.NodeSandboxID
		fence.SandboxGeneration = rec.SandboxGeneration
	}
	if !fence.valid() {
		return
	}
	r.mu.Lock()
	if call := r.inflight[key]; call != nil && (call.expectedSandboxID == "" || call.expectedSandboxID == fence.SandboxID) {
		call.expectedSandboxID = fence.SandboxID
		call.rollbackFence = fence
	}
	r.mu.Unlock()
}

func (r *Registry) setReserveExpectedSandboxID(key, sandboxID string) {
	if sandboxID == "" {
		return
	}
	r.mu.Lock()
	if call := r.inflight[key]; call != nil && (call.expectedSandboxID == "" || call.expectedSandboxID == sandboxID) {
		call.expectedSandboxID = sandboxID
	}
	r.mu.Unlock()
}

func (r *Registry) reserveRollbackFence(key string) (reserveRollbackFence, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if call := r.inflight[key]; call != nil && call.rollbackFence.valid() {
		return call.rollbackFence, true
	}
	return reserveRollbackFence{}, false
}

func (r *Registry) reserveExpectedSandboxID(key string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if call := r.inflight[key]; call != nil {
		return call.expectedSandboxID
	}
	return ""
}

func waitCall(ctx context.Context, call *reserveCall) (*ReserveResult, error) {
	select {
	case <-call.done:
		return call.result, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *Registry) waitReserveCall(ctx context.Context, call *reserveCall) (*ReserveResult, error) {
	for {
		select {
		case <-call.done:
			return call.result, call.err
		default:
		}
		current, observation, err := r.stores.routes.ReadAndObserveRoute(ctx, metadata.RouteKey{Group: call.group, RouteKey: call.routeKey})
		if err != nil {
			return nil, err
		}
		expected := r.reserveExpectedSandboxID(flightKey(call.group, call.routeKey))
		if expected != "" {
			if !current.Exists || current.Value.SandboxID != expected {
				observation.Cancel()
				return nil, ErrSandboxNotFound
			}
			if current.Value.State == StateReady {
				result, ready, err := r.readyResultFromRecord(ctx, &current.Value, current.Version)
				if err != nil || ready {
					observation.Cancel()
					return result, err
				}
			}
		}
		select {
		case <-call.done:
			observation.Cancel()
			return call.result, call.err
		case <-ctx.Done():
			observation.Cancel()
			return nil, ctx.Err()
		case <-observation.Done():
		}
		err = observation.Err()
		observation.Cancel()
		if err != nil {
			return nil, err
		}
	}
}

func transientRouteRead(err error) bool {
	return errors.Is(err, metadata.ErrUnavailable) || errors.Is(err, metadata.ErrConflict)
}

func (r *Registry) reserveReadyResult(ctx context.Context, group, routeKey, expectedSandboxID string) (*ReserveResult, bool, error) {
	rec, rev, found, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil {
		return nil, false, err
	}
	if !found || rec.SandboxID != expectedSandboxID {
		return nil, false, ErrSandboxNotFound
	}
	if rec.State != StateReady {
		return nil, false, nil
	}
	return r.readyResultFromRecord(ctx, rec, rev)
}

func (r *Registry) readyResultFromRecord(ctx context.Context, rec *SandboxRecord, routeRevision metadata.Version) (*ReserveResult, bool, error) {
	if rec == nil || rec.NodeID == "" || r.nodeOwner == nil {
		return nil, false, nil
	}
	if _, _, err := replacementCredentials(rec); err != nil {
		return nil, false, err
	}
	node, found, err := r.nodeOwner.Runtime(ctx, rec.NodeID)
	if errors.Is(err, ErrNodeGone) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !found || node == nil {
		return nil, false, fmt.Errorf("registry: runtime profile for connected node %q is unavailable", rec.NodeID)
	}
	route, err := r.routeFromRecord(ctx, rec, routeRevision)
	if err != nil {
		return nil, false, err
	}
	route.APIEndpoint = node.APIEndpoint
	route.DataEndpoint = node.DataEndpoint
	return &ReserveResult{Route: *route}, true, nil
}

// DeleteSandboxRoute sends the authoritative delete command for an exact
// group-scoped route. The caller supplies the stable public SandboxID so a
// request for a deleted lineage cannot affect a route-key reuse.
func (r *Registry) DeleteSandboxRoute(ctx context.Context, group, routeKey, sandboxID string) (bool, error) {
	if group == "" || routeKey == "" || sandboxID == "" {
		return false, nil
	}
	rec, rev, found, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil || !found {
		return false, err
	}
	if rec.SandboxID != sandboxID {
		return false, nil
	}
	if r.nodeOwner == nil || rec.NodeID == "" {
		if rec.State == StateDead || rec.State == StateDeleting {
			return false, ErrNodeGone
		}
		return r.stores.DeleteSandboxIfRevision(ctx, group, routeKey, rev)
	}
	if err := r.nodeOwner.DeleteSandbox(ctx, rec.NodeID, rec.NodeSandboxID, rec.APISecretFingerprint); err != nil {
		if rec.State == StateDead || rec.State == StateDeleting {
			return false, err
		}
		if !errors.Is(err, ErrNodeGone) {
			return false, err
		}
		err := r.stores.routes.DeleteRouteAndOwnership(ctx, metadata.RouteKey{Group: group, RouteKey: routeKey}, rev, rec.NodeID, metadata.SandboxRef(*rec))
		return err == nil, err
	}
	return true, nil
}

// --- node channel registry ---

func (r *Registry) node(id string) (nodeConn, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.nodes[id]
	return c, ok
}

func (r *Registry) nodeBuildFrameFence(id string) *nodeBuildFrameFence {
	r.mu.Lock()
	defer r.mu.Unlock()
	fence := r.buildFrameFences[id]
	if fence == nil {
		fence = &nodeBuildFrameFence{}
		r.buildFrameFences[id] = fence
	}
	return fence
}

// nodeAPIEndpoint returns a node's control API endpoint, or "" if the node is
// unknown.
func (r *Registry) nodeAPIEndpoint(ctx context.Context, nodeID string) string {
	if r.nodeOwner == nil {
		return ""
	}
	if n, found, _ := r.nodeOwner.Runtime(ctx, nodeID); found && n != nil {
		return n.APIEndpoint
	}
	return ""
}

// nodeDataEndpoint returns a node's data-plane endpoint, or "" if the node is
// unknown.
func (r *Registry) nodeDataEndpoint(ctx context.Context, nodeID string) string {
	if r.nodeOwner == nil {
		return ""
	}
	if n, found, _ := r.nodeOwner.Runtime(ctx, nodeID); found && n != nil {
		return n.DataEndpoint
	}
	return ""
}

func (r *Registry) addNode(c nodeConn) {
	fence := r.nodeBuildFrameFence(c.id())
	fence.mu.Lock()
	defer fence.mu.Unlock()
	r.mu.Lock()
	// A second connection for the same node id replaces the first. Holding the
	// per-node Build fence makes every mutation from the old read stream finish
	// before the new session becomes authoritative.
	r.nodes[c.id()] = c
	fence.active, _ = c.(*nodeChannel)
	r.mu.Unlock()
}

func (r *Registry) removeNode(c nodeConn) {
	fence := r.nodeBuildFrameFence(c.id())
	fence.mu.Lock()
	removed := false
	r.mu.Lock()
	if r.nodes[c.id()] == c {
		delete(r.nodes, c.id())
		removed = true
	}
	if channel, ok := c.(*nodeChannel); ok && fence.active == channel {
		fence.active = nil
	}
	r.mu.Unlock()
	fence.mu.Unlock()
	if removed {
		r.scheduleNodeReap(c.id())
	}
}

// updateNodeRegister claims one profile revision for this registration. A reap
// that deletes the profile between read and write makes the CAS retry from an
// empty node view, so stale child refs cannot be replayed into the new session.
func (r *Registry) updateNodeRegister(ctx context.Context, nr *routesync.NodeRegister) (*NodeRecord, error) {
	for attempt := 0; attempt < 5; attempt++ {
		current, err := r.stores.nodes.ReadNodeProfile(ctx, nr.NodeID)
		if err != nil {
			return nil, err
		}
		next := current.Value
		next.NodeID = nr.NodeID
		next.State = metadata.NodeLive
		next.Labels = cloneStringMap(nr.Labels)
		next.Capacity = nr.Capacity
		next.BuildRegistrationCapacity = cloneBuildAdmissionLimit(nr.BuildRegistrationCapacity)
		next.BuildExecutionCapacity = cloneBuildAdmissionLimit(nr.BuildExecutionCapacity)
		next.APIEndpoint, next.DataEndpoint, next.RuntimeDigest = nr.APIEndpoint, nr.DataEndpoint, nr.RuntimeDigest
		next.LastHeartbeatUnix = time.Now().Unix()
		next.LinkOwner = r.stores.WriterID()
		registered, err := r.stores.nodes.Register(ctx, current.Version, next)
		if errors.Is(err, metadata.ErrConflict) {
			continue
		}
		if err != nil {
			return nil, err
		}
		rec, found, err := r.stores.GetNode(ctx, nr.NodeID)
		if err != nil {
			return nil, err
		}
		if !found || rec.Registration != registered.Registration {
			return nil, metadata.ErrConflict
		}
		return rec, nil
	}
	return nil, metadata.ErrConflict
}

// updateHeartbeat is fenced by the registration captured when this stream opened.
func (r *Registry) updateHeartbeat(ctx context.Context, nodeID string, hb *routesync.Heartbeat) {
	_, _, err := r.updateNodeProfile(ctx, nodeID, false, func(rec *NodeRecord) {
		rec.Zone, rec.Allocated, rec.Pool = hb.Zone, hb.Allocated, hb.Pool
		rec.BuildRegistrationUsage, rec.BuildExecutionUsage = hb.BuildRegistrationUsage, hb.BuildExecutionUsage
		rec.Counts, rec.Draining = hb.Counts, hb.Draining
		rec.LastHeartbeatUnix = time.Now().Unix()
	})
	if err != nil && !errors.Is(err, metadata.ErrConflict) {
		r.log.Warn("node-link: heartbeat update failed", "node", nodeID, "err", err)
	}
}

func (r *Registry) refreshNodeKeyPairs(ctx context.Context, rec *NodeRecord) {
	if rec == nil || rec.NodeID == "" || len(rec.KeyPairs) == 0 {
		return
	}
	if owner, ok := r.localNodeOwner.(*localNodeOwner); ok {
		owner.RefreshKeyPairs(ctx, rec.NodeID, rec.KeyPairs)
	}
	_ = r.stores.PruneExpiredNodeKeyPairs(ctx, rec.NodeID, time.Now().Unix())
}

func (r *Registry) updateNodeResume(ctx context.Context, nodeID, token string) {
	_, _, _ = r.updateNodeProfile(ctx, nodeID, false, func(rec *NodeRecord) {
		rec.ResumeToken = token
	})
}

func (r *Registry) updateNodeProfile(ctx context.Context, nodeID string, create bool, mutate func(*NodeRecord)) (*NodeRecord, bool, error) {
	if nodeID == "" {
		return nil, false, nil
	}
	registration, ok := ctx.Value(nodeRegistrationContextKey{}).(metadata.Registration)
	if !ok || registration.NodeID != nodeID || !registration.Fence.IsCommitted() {
		return nil, false, metadata.ErrConflict
	}
	rec, found, err := r.stores.GetNodeProfile(ctx, nodeID)
	if err != nil || !found {
		return nil, found, err
	}
	if rec.Reaping || rec.Registration != registration {
		return nil, false, metadata.ErrConflict
	}
	expected := rec.Version
	mutate(rec)
	version, err := r.stores.nodes.UpdateProfile(ctx, registration, expected, nodeProfile(rec))
	if err != nil {
		return nil, false, err
	}
	rec.Version = version
	return rec, true, nil
}

func (r *Registry) getNodeForLinkUpdate(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	return r.stores.GetNode(ctx, nodeID)
}

// RunReaper configures event-driven dead-node cleanup. A node-link disconnect
// schedules cleanup for that node only; recovery enumerates durable reap claims.
func (r *Registry) RunReaper(ctx context.Context, deadAfter time.Duration) {
	if deadAfter <= 0 {
		deadAfter = 30 * time.Second
	}
	r.mu.Lock()
	r.reaperCtx = ctx
	r.deadAfter = deadAfter
	r.mu.Unlock()
	// Reaping markers survive Registry crashes. Only this bounded index is scanned.
	resume := func() {
		for cursor := metadata.Cursor(""); ; {
			page, err := r.stores.nodes.ListReaping(ctx, cursor, 128)
			if err != nil {
				r.log.Warn("registry: list reaping nodes", "err", err)
				return
			}
			for _, claim := range page.Items {
				r.cleanupReap(ctx, claim)
			}
			if page.Next == "" {
				return
			}
			cursor = page.Next
		}
	}
	resume()
	ticker := time.NewTicker(deadAfter)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resume()
		}
	}
}

func (r *Registry) scheduleNodeReap(nodeID string) {
	r.mu.Lock()
	ctx := r.reaperCtx
	deadAfter := r.deadAfter
	r.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if deadAfter <= 0 {
		deadAfter = 30 * time.Second
	}
	go func() {
		timer := time.NewTimer(deadAfter)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			r.sweepNode(ctx, nodeID, deadAfter)
		}
	}()
}

// sweepNode acquires a durable lifecycle claim before any cleanup. Reconnects
// replace its fence; every bounded cleanup transaction rechecks that same claim.
func (r *Registry) sweepNode(ctx context.Context, nodeID string, deadAfter time.Duration) {
	if nodeID == "" {
		return
	}
	if _, connected := r.node(nodeID); connected {
		return
	}
	rec, found, err := r.stores.GetNodeProfile(ctx, nodeID)
	if err != nil || !found {
		return
	}
	if rec.Reaping {
		claim := metadata.ReapClaim{NodeID: nodeID, LinkOwner: rec.LinkOwner, ProfileVersion: rec.Version, Fence: rec.Registration.Fence}
		r.cleanupReap(ctx, claim)
		return
	}
	cutoff := time.Now().Add(-deadAfter).Unix()
	// Heartbeats use whole seconds. Equality is expired: the one-shot
	// disconnect timer has already waited deadAfter and must not silently
	// abandon cleanup because both timestamps rounded to the same second.
	if rec.LastHeartbeatUnix <= 0 || rec.LastHeartbeatUnix > cutoff {
		return
	}
	claim, err := r.stores.nodes.BeginReap(ctx, nodeID, rec.Version)
	if err != nil {
		return
	}
	r.cleanupReap(ctx, claim)
}
func (r *Registry) cleanupReap(ctx context.Context, claim metadata.ReapClaim) {
	for cursor := metadata.Cursor(""); ; {
		page, err := r.stores.ownership.ListSandboxOwners(ctx, claim.NodeID, cursor, 128)
		if err != nil {
			return
		}
		for _, ref := range page.Items {
			route, err := r.stores.routes.ReadRoute(ctx, metadata.RouteKey{Group: ref.Value.Group, RouteKey: ref.Value.RouteKey})
			if err != nil {
				return
			}
			allowReserved := !r.hasInflight(flightKey(ref.Value.Group, ref.Value.RouteKey))
			if err := r.stores.nodes.CleanupSandbox(ctx, claim, ref, route.Version, allowReserved); err != nil {
				return
			}
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	for cursor := metadata.Cursor(""); ; {
		page, err := r.stores.ownership.ListBuildOwners(ctx, claim.NodeID, cursor, 128)
		if err != nil {
			return
		}
		for _, ref := range page.Items {
			build, err := r.stores.builds.ReadBuild(ctx, metadata.BuildKey{Group: ref.Value.Group, BuildID: ref.Value.BuildID})
			if err != nil {
				return
			}
			if err := r.stores.nodes.CleanupBuild(ctx, claim, ref, build.Version); err != nil {
				return
			}
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	for cursor := metadata.Cursor(""); ; {
		page, err := r.stores.keyPairs.ListKeyPairs(ctx, claim.NodeID, cursor, 128)
		if err != nil {
			return
		}
		for _, pair := range page.Items {
			if err := r.stores.nodes.CleanupKeyPair(ctx, claim, pair); err != nil {
				return
			}
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	if _, err := r.stores.nodes.FinalizeReap(ctx, claim); err != nil {
		r.log.Warn("registry: finalize reaping node", "node", claim.NodeID, "err", err)
	}
}

func (r *Registry) hasInflight(key string) bool {
	r.mu.Lock()
	_, ok := r.inflight[key]
	r.mu.Unlock()
	return ok
}

// mergeConfig folds a group's sandbox_config defaults under the create config
// (create overrides group); returns create unchanged when there are no defaults.
func mergeConfig(group, create map[string]string) map[string]string {
	if len(group) == 0 {
		return create
	}
	merged := make(map[string]string, len(group)+len(create))
	for k, v := range group {
		merged[k] = v
	}
	for k, v := range create {
		merged[k] = v
	}
	return merged
}

func cas(err error, ok bool) error {
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("registry: reserve CAS conflict")
	}
	return nil
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// fullFingerprint is the lowercase full SHA-256 fingerprint used by node_link
// key-pair records and lifecycle commands.
func fullFingerprint(secretHex string) string {
	raw, err := hex.DecodeString(secretHex)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}
