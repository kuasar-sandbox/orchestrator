package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

// ErrNoNode is returned when no eligible node can host a sandbox.
var ErrNoNode = errors.New("registry: no eligible node")

// ErrNodeGone is returned when the node owner cannot reach the target node.
var ErrNodeGone = errors.New("registry: node owner cannot reach node")

var (
	errMissingImportSourceLease = errors.New("registry: import source lease fields are required")
	errStaleImportSourceLease   = errors.New("registry: stale import source lease")
)

const lifecycleAckTimeout = 5 * time.Second
const nodeListProjectionRetryInterval = 200 * time.Millisecond
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
// cluster-placer.md); TargetRuntimeDigest lets the placer prefer compatible
// node runtimes when the caller knows the required runtime identity.
type PlaceRequest struct {
	Group               string
	RouteKey            string
	SandboxID           string
	Config              map[string]string
	Build               bool
	TargetRuntimeDigest string
	ExcludeNodeIDs      []string
}

// Placement is a placer answer plus the group-derived material the registry must
// place on node commands. Registry route owners persist AccessToken in route_link
// and never call a sandbox-group provider on the hot/read path.
type Placement struct {
	NodeID         string
	TemplateRef    string
	TargetPort     int
	Config         map[string]string
	KeyFingerprint string
	AccessToken    string
	ImageRepo      string
	RegistryAuth   string
}

// Placer suggests a node and group-derived create/build material. Production
// placement calls placer over placer_link; the builtin placer is the size-1/test
// fallback and only fills NodeID.
type Placer interface {
	Place(ctx context.Context, req PlaceRequest) (*Placement, error)
}

// Registry owns the route_link/node_link/placer_link shardkv views and accepts
// node_link streams.
type Registry struct {
	stores      *Stores
	placer      Placer
	parkTimeout time.Duration
	log         *slog.Logger

	mu       sync.Mutex
	nodes    map[string]nodeConn               // node_id -> channel
	inflight map[string]*reserveCall           // single-flight ReserveSandbox per (group,route_key)
	acks     map[string]chan *routesync.CmdAck // cmd_id -> command acknowledgement waiter

	localNodeOwner      NodeOwner
	nodeOwner           NodeOwner
	deadAfter           time.Duration
	reaperCtx           context.Context
	nodeLinkRelayMu     sync.RWMutex
	nodeLinkRelayPeers  map[string]NodeLinkRelayPeer
	nodeListProjectMu   sync.Mutex
	nodeListProject     map[string]clusterstate.NodeListEntry
	nodeListProjectCtx  context.Context
	nodeListProjectGen  uint64
	nodeListProjectRoot bool
	nodeListProjectRun  bool

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
	// re-Place context (§7.4): a rejected create re-places once on another node
	// before failing the Reserve. orig is the pre-reserve record to restore on
	// timeout/reject while the row is still RESERVED.
	group, routeKey string
	orig            *SandboxRecord
	found           bool
	createConfig    map[string]string
}

// ReserveResult is what a satisfied ReserveSandbox returns (cluster.md). The
// router injects AccessToken and forwards to the node's DataEndpoint.
type ReserveResult struct {
	NodeID             string `json:"node_id"`
	SID                string `json:"sid"`
	AccessToken        string `json:"access_token"`
	TrafficAccessToken string `json:"traffic_access_token,omitempty"`
	TargetPort         int    `json:"target_port,omitempty"`
	DataEndpoint       string `json:"data_endpoint"`
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
		stores:          stores,
		placer:          placer,
		parkTimeout:     parkTimeout,
		log:             log,
		nodes:           make(map[string]nodeConn),
		inflight:        make(map[string]*reserveCall),
		acks:            make(map[string]chan *routesync.CmdAck),
		nodeListProject: make(map[string]clusterstate.NodeListEntry),
		placerReplicas:  1,
		minReadyPlacers: 1,
		placerTimeout:   2 * time.Second,
	}
	localOwner := newLocalNodeOwner(r)
	r.localNodeOwner = localOwner
	r.nodeOwner = localOwner
	return r
}

// applySelectorPatch renews the placer-selected node manifest-key cache. The
// placer owns selector/shuffle decisions; registry only writes the current key
// lease into node_link for the explicit nodes in the patch. Old keys are not
// actively deleted: node_link and node side TTLs expire entries that stop being
// renewed.
func (r *Registry) applySelectorPatch(ctx context.Context, p *routesync.SelectorPatch) error {
	if err := r.checkSelectorPatchLease(ctx, p); err != nil {
		return err
	}
	if p.KeyFingerprint == "" || len(p.NodeIDs) == 0 {
		return nil
	}
	keyType, keyValue := p.ManifestKeyType, p.ManifestKey
	if keyType == clusterstate.SecretRef {
		keyValue = p.ManifestKeyRef
	}
	if keyType == "" && keyValue != "" {
		keyType = clusterstate.SecretInline
	}
	if keyValue == "" {
		return nil
	}
	expiresUnix := time.Now().Add(keyLeaseTTL).Unix()
	return r.putManifestKeyTargets(ctx, p.NodeIDs, p.KeyFingerprint, keyType, keyValue, expiresUnix)
}

func (r *Registry) putManifestKeyTargets(ctx context.Context, nodeIDs []string, fingerprint, keyType, keyValue string, expiresUnix int64) error {
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
					err := r.nodeOwner.PutManifestKey(workCtx, nodeID, fingerprint, keyType, keyValue, expiresUnix)
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

// ReserveSandbox resolves (group, route_key) to a running sandbox, placing +
// creating (or resuming a PAUSED sandbox) on a node and waiting for the node to
// report it running, single-flight per key (cluster.md).
func (r *Registry) ReserveSandbox(ctx context.Context, group, routeKey string, createConfig map[string]string) (*ReserveResult, error) {
	if group == "" || routeKey == "" {
		return nil, fmt.Errorf("registry: group and route_key are required")
	}
	var err error
	createConfig, err = sandboxcfg.NormalizeRestoreMetadata(createConfig)
	if err != nil {
		return nil, err
	}
	rec, rev, found, err := r.getSandboxForReserve(ctx, group, routeKey)
	if err != nil {
		return nil, err
	}
	if found && rec.State == StateReady {
		res, live, err := r.readyResultFromRecord(ctx, rec)
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
	r.inflight[key] = call
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.inflight, key)
		r.mu.Unlock()
	}()

	if err := r.startReserve(ctx, group, routeKey, rec, rev, found, createConfig); err != nil {
		r.finish(key, nil, err)
		if errors.Is(err, errNodeSandboxIDConflict) {
			r.rollbackReserveRetainingOwnership(group, routeKey, rec, found)
		} else {
			r.rollbackReserve(group, routeKey, rec, found)
		}
		return waitCall(ctx, call)
	}
	wctx, cancel := context.WithTimeout(ctx, r.parkTimeout)
	defer cancel()
	res, rerr := r.waitReserveCall(wctx, call)
	if rerr != nil {
		// Park timeout / caller cancel: undo a RESERVED that never reached READY, so
		// it doesn't strand the route. Keep the ownership ref because command
		// delivery may have succeeded; a late live event must still be deleted as
		// registry-owned rather than ignored as a node-local sandbox.
		r.rollbackReserveRetainingOwnership(group, routeKey, rec, found)
		r.finish(key, nil, rerr)
		return nil, rerr
	}
	r.finish(key, res, nil)
	return res, nil
}

func (r *Registry) getSandboxForReserve(ctx context.Context, group, routeKey string) (*SandboxRecord, int64, bool, error) {
	for {
		rec, rev, found, err := r.stores.GetSandbox(ctx, group, routeKey)
		if !transientRouteRead(err) {
			return rec, rev, found, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, 0, false, ctx.Err()
		case <-timer.C:
		}
	}
}

// rollbackReserve restores a (group, route_key) to its pre-reserve state when a
// Reserve fails without reaching READY (cluster.md): a pre-existing PAUSED
// row is put back and a fresh one is deleted, but only while the row is still
// RESERVED (a late running route may have won).
func (r *Registry) rollbackReserve(group, routeKey string, orig *SandboxRecord, found bool) {
	r.rollbackReserveWithRefPolicy(group, routeKey, orig, found, true)
}

func (r *Registry) rollbackReserveRetainingOwnership(group, routeKey string, orig *SandboxRecord, found bool) {
	r.rollbackReserveWithRefPolicy(group, routeKey, orig, found, false)
}

func (r *Registry) rollbackReserveWithRefPolicy(group, routeKey string, orig *SandboxRecord, found, dropCurrentRef bool) {
	ctx := context.Background() // must complete even if the caller's ctx is done
	cur, rev, curFound, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil || !curFound || cur.State != StateReserved {
		return // already resolved (READY) / gone — nothing to roll back
	}
	r.rollbackReservedAtRevision(ctx, group, routeKey, cur, rev, orig, found, dropCurrentRef)
}

func (r *Registry) rollbackReservedAtRevision(ctx context.Context, group, routeKey string, cur *SandboxRecord, rev int64, orig *SandboxRecord, found, dropCurrentRef bool) bool {
	if found {
		if orig == nil {
			return false
		}
		if _, ok, err := r.stores.CASSandbox(ctx, orig, rev); err != nil || !ok {
			return false
		}
		if dropCurrentRef && cur.NodeID != "" && (cur.NodeID != orig.NodeID || cur.SID != orig.SID) {
			_ = r.stores.RemoveNodeSandboxRef(ctx, cur.NodeID, cur.SID)
		}
		if orig.NodeID != "" {
			_ = r.stores.AddNodeSandboxRef(ctx, orig.NodeID, clusterstate.NodeSandboxRef{
				Group: group, RouteKey: routeKey, SandboxID: orig.SID,
			})
		}
		return true
	}
	return r.deleteSandboxAtRevision(ctx, group, routeKey, cur, rev, dropCurrentRef)
}

func (r *Registry) deleteSandboxAtRevision(ctx context.Context, group, routeKey string, cur *SandboxRecord, rev int64, dropCurrentRef bool) bool {
	deleted, err := r.stores.DeleteSandboxIfRevision(ctx, group, routeKey, rev)
	if err != nil || !deleted {
		return false
	}
	if dropCurrentRef && cur != nil && cur.NodeID != "" {
		_ = r.stores.RemoveNodeSandboxRef(ctx, cur.NodeID, cur.SID)
	}
	return true
}

// startReserve drives the placement + command for the leader of a single-flight.
// It does not wait — the channel reader signals completion when the node reports
// the sandbox running (finish, via applyRoute).
func (r *Registry) startReserve(ctx context.Context, group, routeKey string, rec *SandboxRecord, rev int64, found bool, createConfig map[string]string) error {
	// PAUSED: resume on the same node (no placement).
	if found && rec.State == StatePaused && rec.NodeID != "" {
		if r.nodeOwner == nil {
			return ErrNodeGone
		}
		reserved := *rec
		reserved.State = StateReserved
		if _, ok, err := r.stores.CASSandbox(ctx, &reserved, rev); err != nil || !ok {
			return cas(err, ok)
		}
		if err := r.stores.AddNodeSandboxRef(ctx, rec.NodeID, clusterstate.NodeSandboxRef{Group: group, RouteKey: routeKey, SandboxID: rec.SID}); err != nil {
			if errors.Is(err, errNodeSandboxIDConflict) {
				r.rollbackReserveRetainingOwnership(group, routeKey, rec, true)
			} else {
				r.rollbackReserve(group, routeKey, rec, true)
			}
			return err
		}
		ccmd := &routesync.Command{CmdID: newID(), Kind: routesync.CmdConnect, SID: rec.SID}
		ack, err := r.nodeOwner.SendCommandAndWait(ctx, rec.NodeID, ccmd, lifecycleAckTimeout)
		if err != nil {
			r.rollbackReserve(group, routeKey, rec, true)
			return err
		}
		if ack == nil || ack.Status != routesync.AckAccepted {
			r.rollbackReserve(group, routeKey, rec, true)
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
	return r.placeAndCreate(ctx, group, routeKey, createConfig, replaceReady)
}

// placeAndCreate places a node and sends the create command, CASing the RESERVED
// record. node_list is only a catalog: the selected node owner validates its
// live connection before commit. An unusable candidate is excluded from the next
// placement request so stale catalog entries cannot prevent a live alternative.
func (r *Registry) placeAndCreate(ctx context.Context, group, routeKey string, createConfig map[string]string, replaceReady *SandboxRecord) error {
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
		if curFound && cur.State == StateReady && !sameSandboxGeneration(cur, replaceReady) {
			return nil // a concurrent replacement already won; the running route finishes the Reserve
		}
		sid := "sb-" + newID()
		placement, perr := r.placer.Place(ctx, PlaceRequest{
			Group: group, RouteKey: routeKey, SandboxID: sid, Config: createConfig,
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
		config := sandboxcfg.MergeCreateMetadata(placement.Config, createConfig)
		metadata, err := clusterstate.WithObjectLocation(config, clusterstate.ObjectLocation{Group: group, RouteKey: routeKey})
		if err != nil {
			return err
		}
		ref, err := clusterstate.NodeSandboxRefFromMetadata(sid, metadata)
		if err != nil {
			return err
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
		expect := int64(0)
		if curFound {
			expect = curRev
		}
		reserved := &SandboxRecord{
			Group: group, RouteKey: routeKey, SID: sid, State: StateReserved, NodeID: nodeID,
			TemplateID: placement.TemplateRef, AccessToken: placement.AccessToken,
			TargetPort: placement.TargetPort,
		}
		reservedRev, ok, cerr := r.stores.CASSandbox(ctx, reserved, expect)
		if cerr != nil {
			return cerr
		} else if !ok {
			if casConflicts == 0 {
				casConflicts++
				continue
			}
			return ErrNoNode
		}
		if err := r.stores.AddNodeSandboxRef(ctx, nodeID, ref); err != nil {
			if errors.Is(err, errNodeSandboxIDConflict) {
				if !r.rollbackReservedAtRevision(ctx, group, routeKey, reserved, reservedRev,
					replaceReady, replaceReady != nil, false) {
					return err
				}
				continue
			}
			// Keep RESERVED in place so ReserveSandbox's outer rollback can
			// restore the previous row (or remove a newly-created row).
			return err
		}

		cmd := &routesync.Command{
			CmdID: newID(), Kind: routesync.CmdCreate, SID: sid,
			TemplateRef: placement.TemplateRef, Config: metadata,
			KeyFingerprint: placement.KeyFingerprint, AccessToken: placement.AccessToken,
		}
		if r.nodeOwner == nil {
			r.rollbackReserve(group, routeKey, replaceReady, replaceReady != nil)
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
			r.rollbackReserve(group, routeKey, replaceReady, replaceReady != nil)
			lastFailure = err
			excluded.add(nodeID)
			continue
		}
		if ack == nil || ack.Status != routesync.AckAccepted {
			r.rollbackReserve(group, routeKey, replaceReady, replaceReady != nil)
			reason := ""
			if ack != nil {
				reason = ack.Reason
			}
			lastFailure = fmt.Errorf("registry: create rejected: %s", reason)
			excluded.add(nodeID)
			continue
		}
		return nil
	}
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
// waiting ReserveSandbox (cluster.md / §7.2).
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
	if e.State == routesync.StateDead {
		if r.applyDelete(ctx, nodeID, e.SandboxID, ref.Group, ref.RouteKey) {
			_ = r.stores.RemoveNodeSandboxRef(ctx, nodeID, e.SandboxID)
		}
		return
	}
	rec := &SandboxRecord{
		Group: ref.Group, RouteKey: ref.RouteKey, SID: e.SandboxID, NodeID: nodeID,
		SnapLoc: e.SnapshotLocation, TemplateID: e.TemplateID, AccessToken: e.AccessToken,
		TrafficAccessToken: e.TrafficAccessToken,
		LastActive:         time.Now().Unix(),
	}
	switch e.State {
	case routesync.StateRunning:
		rec.State = StateReady
	case routesync.StatePaused:
		rec.State = StatePaused
	default:
		r.log.Warn("registry: ignored unknown route state", "node", nodeID, "sid", e.SandboxID, "state", e.State)
		return
	}
	r.applyLiveRoute(ctx, nodeID, e, rec)
}

func (r *Registry) lookupNodeSandboxRef(ctx context.Context, nodeID, sandboxID string) (clusterstate.NodeSandboxRef, bool, error) {
	var ref clusterstate.NodeSandboxRef
	var found bool
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		ref, found, err = r.stores.GetNodeSandboxRef(ctx, nodeID, sandboxID)
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
	// Route owner fences orphan reports. A node reporting a sandbox whose
	// (group, route_key, sandbox_id) is absent or has been replaced means the
	// sandbox has already been deleted/replaced in control state; tell that node
	// to kill its local copy instead of resurrecting the route.
	cur, rev, found, err := r.stores.GetSandbox(ctx, rec.Group, rec.RouteKey)
	if err != nil {
		if transientRouteRead(err) {
			return false
		}
		r.log.Warn("registry: read sandbox route", "group", rec.Group, "err", err)
		return true
	}
	if !found || (cur.SID != "" && cur.SID != e.SandboxID) || (cur.NodeID != "" && cur.NodeID != nodeID) {
		r.deleteOrphanSandbox(ctx, nodeID, e.SandboxID, rec.Group, rec.RouteKey)
		return true
	}
	if rec.TemplateID == "" {
		rec.TemplateID = cur.TemplateID
	}
	if rec.AccessToken == "" {
		rec.AccessToken = cur.AccessToken
	}
	if rec.TrafficAccessToken == "" {
		rec.TrafficAccessToken = cur.TrafficAccessToken
	}
	if rec.TargetPort == 0 {
		rec.TargetPort = cur.TargetPort
	}
	if _, ok, err := r.stores.CASSandbox(ctx, rec, rev); err != nil || !ok {
		if transientRouteRead(err) {
			return false
		}
		if err == nil && !ok {
			return false
		}
		r.log.Warn("registry: put sandbox route", "group", rec.Group, "err", err)
		return true
	}
	if rec.State == StateReady {
		r.finish(flightKey(rec.Group, rec.RouteKey), &ReserveResult{
			NodeID: nodeID, SID: e.SandboxID, AccessToken: rec.AccessToken,
			TrafficAccessToken: rec.TrafficAccessToken, TargetPort: rec.TargetPort,
			DataEndpoint: r.nodeDataEndpoint(ctx, nodeID),
		}, nil)
	}
	return true
}

func (r *Registry) deleteOrphanSandbox(ctx context.Context, nodeID, sid, group, routeKey string) {
	if r.nodeOwner == nil || sid == "" {
		return
	}
	if err := r.nodeOwner.DeleteSandbox(ctx, nodeID, sid); err != nil && !errors.Is(err, ErrNodeGone) {
		r.log.Warn("registry: delete orphan sandbox", "node", nodeID, "sid", sid, "group", group, "route_key", routeKey, "err", err)
	}
}

// applyDelete converges a removed route only while it still names the reporting
// node and sandbox. A late DEAD from a replaced instance must not delete its
// successor's route.
func (r *Registry) applyDelete(ctx context.Context, nodeID, sandboxID, group, routeKey string) bool {
	if nodeID == "" || sandboxID == "" || group == "" || routeKey == "" {
		return false
	}
	const attempts = 5
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(time.Duration(attempt) * 10 * time.Millisecond):
			}
		}
		if r.tryApplyDelete(ctx, nodeID, sandboxID, group, routeKey) {
			return true
		}
	}
	r.log.Warn("registry: delete route report did not converge", "node", nodeID, "sid", sandboxID, "group", group, "route_key", routeKey)
	return false
}

func (r *Registry) tryApplyDelete(ctx context.Context, nodeID, sandboxID, group, routeKey string) bool {
	rec, rev, found, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil {
		return false
	}
	if !found {
		return true
	}
	if rec.NodeID != nodeID || rec.SID != sandboxID {
		return true
	}
	deleted, err := r.stores.DeleteSandboxIfRevision(ctx, group, routeKey, rev)
	if err != nil {
		return false
	}
	return deleted
}

func (r *Registry) applyNodeFullSnapshot(ctx context.Context, nodeID string, expected []clusterstate.NodeSandboxRef, seen map[string]struct{}) {
	if nodeID == "" {
		return
	}
	for _, ref := range expected {
		if ref.SandboxID == "" || ref.Group == "" || ref.RouteKey == "" {
			continue
		}
		if _, ok := seen[ref.SandboxID]; ok {
			continue
		}
		current, found, err := r.stores.GetNodeSandboxRef(ctx, nodeID, ref.SandboxID)
		if err != nil || !found || current.Group != ref.Group || current.RouteKey != ref.RouteKey {
			continue
		}
		if r.applyDelete(ctx, nodeID, ref.SandboxID, ref.Group, ref.RouteKey) {
			_ = r.stores.RemoveNodeSandboxRef(ctx, nodeID, ref.SandboxID)
		}
	}
}

// applyDeleteBySID converges a route delete. The node-link owner resolves its
// group route through the per-node sandbox table.
func (r *Registry) applyDeleteBySID(ctx context.Context, nodeID, sid string) {
	ref, found, err := r.lookupNodeSandboxRef(ctx, nodeID, sid)
	if err != nil || !found {
		return
	}
	if r.applyDelete(ctx, nodeID, sid, ref.Group, ref.RouteKey) {
		_ = r.stores.RemoveNodeSandboxRef(ctx, nodeID, sid)
	}
}

// finish satisfies the single-flight call for key (idempotent — first wins). It
func (r *Registry) finish(key string, res *ReserveResult, err error) {
	r.mu.Lock()
	call := r.inflight[key]
	r.mu.Unlock()
	if call == nil {
		return
	}
	call.finishOnce.Do(func() {
		call.result, call.err = res, err
		close(call.done)
	})
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
	rev, _ := r.stores.RouteGroupRev(ctx, call.group)
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	ch, _ := r.stores.WatchRouteGroup(watchCtx, call.group, rev)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-call.done:
			return call.result, call.err
		default:
		}
		if res, ready, err := r.reserveReadyResult(ctx, call.group, call.routeKey); transientRouteRead(err) {
			// A route owner may be accepting the node's READY update concurrently;
			// keep the parked reserve until the event/read settles or times out.
		} else if err != nil {
			return nil, err
		} else if ready {
			return res, nil
		}
		select {
		case <-call.done:
			return call.result, call.err
		case <-ch:
		case <-ticker.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func transientRouteRead(err error) bool {
	return errors.Is(err, clusterstate.ErrQuorum) || errors.Is(err, clusterstate.ErrConflict)
}

func (r *Registry) reserveReadyResult(ctx context.Context, group, routeKey string) (*ReserveResult, bool, error) {
	rec, _, found, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil || !found || rec.State != StateReady {
		return nil, false, err
	}
	return r.readyResultFromRecord(ctx, rec)
}

func (r *Registry) readyResultFromRecord(ctx context.Context, rec *SandboxRecord) (*ReserveResult, bool, error) {
	if rec == nil || rec.NodeID == "" || r.nodeOwner == nil {
		return nil, false, nil
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
	return &ReserveResult{
		NodeID: rec.NodeID, SID: rec.SID, AccessToken: rec.AccessToken,
		TrafficAccessToken: rec.TrafficAccessToken, TargetPort: rec.TargetPort,
		DataEndpoint: node.DataEndpoint,
	}, true, nil
}

// DeleteSandboxRoute sends the authoritative delete command for an exact
// group-scoped route. The caller must provide group + route_key + current sid so
// a stale client cannot delete a replacement sandbox generation.
func (r *Registry) DeleteSandboxRoute(ctx context.Context, group, routeKey, sid string) (bool, error) {
	if group == "" || routeKey == "" || sid == "" {
		return false, nil
	}
	rec, rev, found, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil || !found {
		return false, err
	}
	if rec.SID != sid {
		return false, nil
	}
	if r.nodeOwner == nil || rec.NodeID == "" {
		return r.stores.DeleteSandboxIfRevision(ctx, group, routeKey, rev)
	}
	if err := r.nodeOwner.DeleteSandbox(ctx, rec.NodeID, sid); err != nil {
		if !errors.Is(err, ErrNodeGone) {
			return false, err
		}
		deleted, err := r.stores.DeleteSandboxIfRevision(ctx, group, routeKey, rev)
		if err != nil || !deleted {
			return false, err
		}
		_ = r.stores.RemoveNodeSandboxRef(ctx, rec.NodeID, sid)
		return true, nil
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

// nodeDataEndpoint returns a node's data-plane endpoint (the router forwards to
// it), or "" if the node is unknown.
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
	r.mu.Lock()
	// A second connection for the same node id replaces the first (the old
	// channel's writer then fails and tears itself down).
	r.nodes[c.id()] = c
	r.mu.Unlock()
}

func (r *Registry) removeNode(c nodeConn) {
	removed := false
	r.mu.Lock()
	if r.nodes[c.id()] == c {
		delete(r.nodes, c.id())
		removed = true
	}
	r.mu.Unlock()
	if removed {
		r.scheduleNodeReap(c.id())
	}
}

// updateNodeRegister claims one profile revision for this registration. A reap
// that deletes the profile between read and write makes the CAS retry from an
// empty node view, so stale child refs cannot be replayed into the new session.
func (r *Registry) updateNodeRegister(ctx context.Context, nr *routesync.NodeRegister) (*NodeRecord, error) {
	rec, _, err := r.updateNodeProfile(ctx, nr.NodeID, true, func(rec *NodeRecord) {
		rec.Labels = nr.Labels
		rec.Capacity = nr.Capacity
		rec.BuildCapacity = nr.BuildCapacity
		rec.DataEndpoint = nr.DataEndpoint
		rec.RuntimeDigest = nr.RuntimeDigest
		rec.LastHeartbeatUnix = time.Now().Unix()
		rec.LinkOwner = r.stores.WriterID()
	})
	return rec, err
}

func (r *Registry) projectRegisteredNode(ctx context.Context, rec *NodeRecord) {
	if rec == nil {
		return
	}
	if _, live := r.node(rec.NodeID); !live {
		return
	}
	r.putNodeListProjection(ctx, rec)
}

// updateHeartbeat folds a node's water level into its record + stamps liveness.
func (r *Registry) updateHeartbeat(ctx context.Context, nodeID string, hb *routesync.Heartbeat) {
	oldDraining := false
	rec, found, err := r.updateNodeProfile(ctx, nodeID, false, func(rec *NodeRecord) {
		oldDraining = rec.Draining
		rec.Zone, rec.Allocated, rec.Pool = hb.Zone, hb.Allocated, hb.Pool
		rec.BuildAlloc, rec.Counts, rec.Draining = hb.BuildAlloc, hb.Counts, hb.Draining
		rec.LastHeartbeatUnix = time.Now().Unix()
	})
	if err != nil || !found {
		return
	}
	if rec.Draining != oldDraining {
		r.putNodeListProjection(ctx, rec)
		return
	}
	r.retryPendingNodeListProjection(ctx, rec)
}

func (r *Registry) putNodeListProjection(ctx context.Context, rec *NodeRecord) {
	if r == nil || r.stores == nil || rec == nil || rec.NodeID == "" {
		return
	}
	r.applyNodeListProjection(ctx, projectNodeListRecord(rec))
}

func (r *Registry) deleteNodeListProjection(ctx context.Context, nodeID string, source clusterstate.RecordMeta) {
	if r == nil || r.stores == nil || nodeID == "" {
		return
	}
	r.applyNodeListProjection(ctx, clusterstate.NodeListEntry{NodeID: nodeID, SourceMeta: source, Deleted: true})
}

func (r *Registry) applyNodeListProjection(ctx context.Context, entry clusterstate.NodeListEntry) {
	if err := r.writeNodeListProjection(ctx, entry); err == nil {
		r.clearPendingNodeListProjection(entry)
		return
	} else if ctx.Err() != nil {
		return
	} else if !isNodeListProjectionRetryable(err) {
		r.log.Warn("node-link: node_list projection rejected", "node", entry.NodeID, "deleted", entry.Deleted, "err", err)
		return
	}
	r.enqueueNodeListProjection(ctx, entry)
}

func (r *Registry) writeNodeListProjection(ctx context.Context, entry clusterstate.NodeListEntry) error {
	if entry.Deleted {
		return r.stores.DeleteNodeListWithSource(ctx, entry.NodeID, entry.SourceMeta)
	}
	return r.stores.PutNodeListEntry(ctx, entry)
}

func (r *Registry) retryPendingNodeListProjection(ctx context.Context, rec *NodeRecord) {
	if r == nil || rec == nil || rec.NodeID == "" {
		return
	}
	r.nodeListProjectMu.Lock()
	_, pending := r.nodeListProject[rec.NodeID]
	r.nodeListProjectMu.Unlock()
	if pending {
		r.putNodeListProjection(ctx, rec)
	}
}

func (r *Registry) enqueueNodeListProjection(ctx context.Context, entry clusterstate.NodeListEntry) {
	if r == nil || entry.NodeID == "" || ctx.Err() != nil {
		return
	}
	retryCtx := ctx
	lifecycleCtx := false
	if entry.Deleted {
		r.mu.Lock()
		if r.reaperCtx != nil {
			retryCtx = r.reaperCtx
			lifecycleCtx = true
		}
		r.mu.Unlock()
	}
	next := cloneNodeListProjection(entry)
	r.nodeListProjectMu.Lock()
	if cur, found := r.nodeListProject[next.NodeID]; !found || nodeListProjectionNewer(next, cur) {
		r.nodeListProject[next.NodeID] = next
	}
	replaceCtx := r.nodeListProjectCtx == nil || r.nodeListProjectCtx.Err() != nil
	if lifecycleCtx && !r.nodeListProjectRoot {
		replaceCtx = true
	}
	if replaceCtx {
		r.nodeListProjectCtx = retryCtx
		r.nodeListProjectGen++
		r.nodeListProjectRoot = lifecycleCtx
	}
	if !r.nodeListProjectRun {
		r.nodeListProjectRun = true
		go r.runNodeListProjectionRetry()
	}
	r.nodeListProjectMu.Unlock()
}

func (r *Registry) runNodeListProjectionRetry() {
	ticker := time.NewTicker(nodeListProjectionRetryInterval)
	defer ticker.Stop()
	for {
		r.nodeListProjectMu.Lock()
		ctx := r.nodeListProjectCtx
		ctxGen := r.nodeListProjectGen
		if ctx == nil || ctx.Err() != nil {
			r.nodeListProjectRun = false
			r.nodeListProjectCtx = nil
			r.nodeListProjectRoot = false
			r.nodeListProjectMu.Unlock()
			return
		}
		r.nodeListProjectMu.Unlock()
		r.flushNodeListProjection(ctx)
		r.nodeListProjectMu.Lock()
		if len(r.nodeListProject) == 0 {
			r.nodeListProjectRun = false
			r.nodeListProjectCtx = nil
			r.nodeListProjectRoot = false
			r.nodeListProjectMu.Unlock()
			return
		}
		nextCtxGen := r.nodeListProjectGen
		r.nodeListProjectMu.Unlock()
		if nextCtxGen != ctxGen {
			continue
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
		}
	}
}

func (r *Registry) flushNodeListProjection(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		return
	}
	r.nodeListProjectMu.Lock()
	batch := make([]clusterstate.NodeListEntry, 0, len(r.nodeListProject))
	for _, entry := range r.nodeListProject {
		batch = append(batch, cloneNodeListProjection(entry))
	}
	r.nodeListProjectMu.Unlock()
	for _, entry := range batch {
		r.flushNodeListProjectionEntry(ctx, entry)
	}
}

func (r *Registry) flushNodeListProjectionEntry(ctx context.Context, entry clusterstate.NodeListEntry) {
	if !r.nodeListProjectionPending(entry) {
		return
	}
	if !entry.Deleted {
		if _, connected := r.node(entry.NodeID); !connected {
			r.clearPendingNodeListProjection(entry)
			return
		}
	}
	if err := r.writeNodeListProjection(ctx, entry); err != nil {
		if ctx.Err() != nil {
			return
		}
		if !isNodeListProjectionRetryable(err) {
			r.log.Warn("node-link: node_list projection retry rejected", "node", entry.NodeID, "deleted", entry.Deleted, "err", err)
			r.clearPendingNodeListProjection(entry)
		}
		return
	}
	r.clearPendingNodeListProjection(entry)
}

func (r *Registry) nodeListProjectionPending(entry clusterstate.NodeListEntry) bool {
	if r == nil || entry.NodeID == "" {
		return false
	}
	r.nodeListProjectMu.Lock()
	defer r.nodeListProjectMu.Unlock()
	cur, found := r.nodeListProject[entry.NodeID]
	return found && compareProjectionSource(cur, entry) == 0 && cur.Deleted == entry.Deleted
}

func (r *Registry) clearPendingNodeListProjection(entry clusterstate.NodeListEntry) {
	if r == nil || entry.NodeID == "" {
		return
	}
	r.nodeListProjectMu.Lock()
	defer r.nodeListProjectMu.Unlock()
	cur, found := r.nodeListProject[entry.NodeID]
	if !found || !nodeListProjectionNewer(cur, entry) {
		delete(r.nodeListProject, entry.NodeID)
	}
}

func cloneNodeListProjection(entry clusterstate.NodeListEntry) clusterstate.NodeListEntry {
	entry.Labels = cloneStringMap(entry.Labels)
	entry.BuildCapacity = cloneBuildResources(entry.BuildCapacity)
	return entry
}

func isNodeListProjectionRetryable(err error) bool {
	return errors.Is(err, shardkv.ErrQuorum) ||
		errors.Is(err, shardkv.ErrConflict) ||
		errors.Is(err, shardkv.ErrReplicaUnavailable) ||
		errors.Is(err, context.DeadlineExceeded)
}

func (r *Registry) refreshNodeManifestKeys(ctx context.Context, rec *NodeRecord) {
	if rec == nil || rec.NodeID == "" || len(rec.ManifestKeys) == 0 {
		return
	}
	if owner, ok := r.localNodeOwner.(*localNodeOwner); ok {
		owner.RefreshManifestKeys(ctx, rec.NodeID, rec.ManifestKeys)
	}
	_ = r.stores.PruneExpiredNodeManifestKeys(ctx, rec.NodeID, time.Now().Unix())
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
	for attempt := 0; attempt < 5; attempt++ {
		rec, found, err := r.getNodeForLinkUpdate(ctx, nodeID)
		if err != nil {
			return nil, false, err
		}
		if !found {
			if !create {
				return nil, false, nil
			}
			rec = &NodeRecord{NodeID: nodeID}
		}
		expectRev := rec.Meta.Rev
		mutate(rec)
		if _, ok, err := r.stores.casNodeProfileShard(ctx, rec, expectRev); err != nil {
			return nil, false, err
		} else if ok {
			return rec, true, nil
		}
		if !create {
			return nil, false, shardkv.ErrConflict
		}
	}
	return nil, false, shardkv.ErrConflict
}

func (r *Registry) getNodeForLinkUpdate(ctx context.Context, nodeID string) (*NodeRecord, bool, error) {
	rec, found, err := r.stores.GetNode(ctx, nodeID)
	if errors.Is(err, shardkv.ErrInvalidView) {
		return r.stores.GetNodeProfile(ctx, nodeID)
	}
	return rec, found, err
}

// RunReaper configures event-driven dead-node cleanup. A node-link disconnect
// schedules cleanup for that node only; registry never scans node shards globally.
func (r *Registry) RunReaper(ctx context.Context, deadAfter time.Duration) {
	if deadAfter <= 0 {
		deadAfter = 30 * time.Second
	}
	r.mu.Lock()
	r.reaperCtx = ctx
	r.deadAfter = deadAfter
	r.mu.Unlock()
	r.nodeListProjectMu.Lock()
	pendingDelete := false
	for _, entry := range r.nodeListProject {
		if entry.Deleted {
			pendingDelete = true
			break
		}
	}
	if pendingDelete {
		r.nodeListProjectCtx = ctx
		r.nodeListProjectGen++
		r.nodeListProjectRoot = true
		if !r.nodeListProjectRun {
			r.nodeListProjectRun = true
			go r.runNodeListProjectionRetry()
		}
	}
	r.nodeListProjectMu.Unlock()
	<-ctx.Done()
}

func (r *Registry) RunCompactor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			stats, err := r.stores.Compact(ctx, now)
			if err != nil {
				r.log.Warn("registry: shardkv compaction failed", "err", err)
				continue
			}
			if stats.Tombstones > 0 || stats.Shards > 0 {
				r.log.Debug("registry: shardkv compacted", "tombstones", stats.Tombstones, "shards", stats.Shards)
			}
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

type sandboxReapPlan struct {
	ref         nodeReapSandboxRef
	record      *SandboxRecord
	revision    int64
	deleteRoute bool
}

type buildReapPlan struct {
	ref       nodeReapBuildRef
	record    *BuildRecord
	revision  uint64
	markDead  bool
	removeRef bool
}

// sweepNode claims a stale profile revision before applying any destructive
// cleanup. Route/build and child-ref writes are then limited to revisions read
// before that claim, so a reconnect can safely win either side of the CAS.
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
	cutoff := time.Now().Add(-deadAfter).Unix()
	if rec.LastHeartbeatUnix <= 0 || rec.LastHeartbeatUnix >= cutoff {
		return
	}
	snapshot, err := r.stores.snapshotNodeReapShard(ctx, nodeID)
	if err != nil || snapshot == nil {
		return
	}
	sandboxPlans := make([]sandboxReapPlan, 0, len(snapshot.Sandboxes))
	for _, child := range snapshot.Sandboxes {
		ref := child.Ref
		s, rev, routeFound, err := r.stores.GetSandbox(ctx, ref.Group, ref.RouteKey)
		if err != nil {
			return
		}
		plan := sandboxReapPlan{ref: child}
		if !routeFound || s.NodeID != nodeID || s.SID != ref.SandboxID {
			sandboxPlans = append(sandboxPlans, plan)
			continue
		}
		shouldReset := s.State == StateReady || s.State == StatePaused
		if s.State == StateReserved {
			if r.hasInflight(flightKey(s.Group, s.RouteKey)) {
				continue
			}
			shouldReset = true
		}
		if !shouldReset {
			sandboxPlans = append(sandboxPlans, plan)
			continue
		}
		plan.record, plan.revision, plan.deleteRoute = s, rev, true
		sandboxPlans = append(sandboxPlans, plan)
	}
	buildPlans := make([]buildReapPlan, 0, len(snapshot.Builds))
	for _, child := range snapshot.Builds {
		ref := child.Ref
		b, rev, buildFound, err := r.stores.getRouteBuildShard(ctx, ref.Group, ref.BuildID)
		if err != nil {
			return
		}
		plan := buildReapPlan{ref: child}
		if !buildFound || b.NodeID != nodeID {
			plan.removeRef = true
			buildPlans = append(buildPlans, plan)
			continue
		}
		if !b.occupies() {
			buildPlans = append(buildPlans, plan)
			continue
		}
		plan.record, plan.revision, plan.markDead = b, rev, true
		buildPlans = append(buildPlans, plan)
	}
	claimed, err := r.stores.claimNodeProfileReapShard(ctx, nodeID, rec.Meta.Rev)
	if err != nil || !claimed {
		return
	}
	r.deleteNodeListProjection(ctx, nodeID, rec.Meta)
	reset := 0
	for _, plan := range sandboxPlans {
		if plan.deleteRoute && !r.deleteSandboxAtRevision(ctx, plan.record.Group, plan.record.RouteKey, plan.record, plan.revision, false) {
			continue
		}
		if _, err := r.stores.removeNodeSandboxRefShardAtRevision(ctx, nodeID, plan.ref.Ref.SandboxID, plan.ref.Revision); err != nil {
			r.log.Warn("registry: remove reaped sandbox ownership", "node", nodeID, "sandbox", plan.ref.Ref.SandboxID, "err", err)
			continue
		}
		if plan.deleteRoute {
			reset++
		}
	}
	deadBuilds := 0
	for _, plan := range buildPlans {
		if plan.markDead {
			plan.record.State, plan.record.Reason = BuildError, "node disconnected"
			if _, ok, err := r.stores.casRouteBuildShard(ctx, plan.record, plan.revision); err != nil || !ok {
				continue
			}
			r.releaseBuildAdmission(nodeID, plan.record.Group, plan.record.BuildID)
			deadBuilds++
		}
		if !plan.removeRef {
			continue
		}
		if _, err := r.stores.removeNodeBuildRefShardAtRevision(ctx, nodeID, plan.ref.Ref.BuildID, plan.ref.Revision); err != nil {
			r.log.Warn("registry: remove reaped build ownership", "node", nodeID, "build", plan.ref.Ref.BuildID, "err", err)
		}
	}
	for _, key := range snapshot.ManifestKeys {
		if _, err := r.stores.dropNodeManifestKeyShardAtRevision(ctx, nodeID, key.Fingerprint, key.Revision); err != nil {
			r.log.Warn("registry: remove reaped manifest key", "node", nodeID, "fingerprint", key.Fingerprint, "err", err)
		}
	}
	r.log.Warn("registry: swept dead node", "node", nodeID, "sandboxes_reset", reset, "builds_errored", deadBuilds)
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

// keyFingerprint is the fingerprint the node matches against its manifest-key
// allowlist (= store.ManifestKeyHash: hex(apikey.Fingerprint(rawKey))).
func keyFingerprint(manifestKeyHex string) string {
	raw, err := hex.DecodeString(manifestKeyHex)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(apikey.Fingerprint(raw))
}
