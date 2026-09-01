// Package registry is the cluster control plane's route/node owner and node_link
// hub. route_link, node_link, node_list, and placer_link records go through the
// registry-owned quorum kernel; sandbox-group provider state lives on
// placer/provider side.
package registry

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

const nodeListTombstoneRetention = time.Hour

// SandboxState mirrors cluster.md route state. A missing/dead sandbox is
// represented by no route row.
type SandboxState string

const (
	StateNone     SandboxState = "none"
	StateReserved SandboxState = "reserved"
	StateReady    SandboxState = "ready"
	StatePaused   SandboxState = "paused"
)

// NodeRecord is the registry-facing node_link view.
type NodeRecord struct {
	Meta                      clusterstate.RecordMeta        `json:"meta,omitempty"`
	NodeID                    string                         `json:"node_id"`
	Labels                    map[string]string              `json:"labels,omitempty"`
	Capacity                  int                            `json:"capacity,omitempty"`
	BuildRegistrationCapacity *routesync.BuildAdmissionLimit `json:"build_registration_capacity,omitempty"`
	BuildExecutionCapacity    *routesync.BuildAdmissionLimit `json:"build_execution_capacity,omitempty"`
	DataEndpoint              string                         `json:"data_endpoint,omitempty"`
	RuntimeDigest             string                         `json:"runtime_digest,omitempty"`
	Zone                      string                         `json:"zone,omitempty"`
	Allocated                 int64                          `json:"allocated,omitempty"`
	Pool                      int64                          `json:"pool,omitempty"`
	BuildRegistrationUsage    *routesync.BuildAdmissionUsage `json:"build_registration_usage,omitempty"`
	BuildExecutionUsage       *routesync.BuildAdmissionUsage `json:"build_execution_usage,omitempty"`
	Counts                    int                            `json:"counts,omitempty"`
	Draining                  bool                           `json:"draining,omitempty"`
	// LastHeartbeatUnix is the last sign of life (register or heartbeat); the
	// dead-node sweep resets a disconnected node whose last beat predates
	// node_dead_after.
	LastHeartbeatUnix int64                         `json:"last_heartbeat_unix,omitempty"`
	ResumeToken       string                        `json:"resume_token,omitempty"`
	LinkOwner         string                        `json:"link_owner,omitempty"`
	KeyPairs          []clusterstate.NodeKeyPair    `json:"key_pairs,omitempty"`
	Sandboxes         []clusterstate.NodeSandboxRef `json:"sandboxes,omitempty"`
	Builds            []clusterstate.NodeBuildRef   `json:"builds,omitempty"`
}

// SandboxRecord is the registry-facing route_link view, keyed by
// (group, route_key).
type SandboxRecord struct {
	Group                  string                    `json:"group"`
	RouteKey               string                    `json:"route_key"`
	SandboxID              string                    `json:"sandbox_id,omitempty"`
	NodeSandboxID          string                    `json:"node_sandbox_id,omitempty"`
	SandboxGeneration      uint64                    `json:"sandbox_generation"`
	NextSandboxGeneration  uint64                    `json:"next_sandbox_generation"`
	State                  SandboxState              `json:"state"`
	NodeID                 string                    `json:"node_id,omitempty"`
	SnapLoc                string                    `json:"snap_loc,omitempty"`
	TemplateID             string                    `json:"template_id,omitempty"`
	Profile                string                    `json:"profile,omitempty"`
	APISecretFingerprint   string                    `json:"api_secret_fingerprint,omitempty"`
	ManifestKeyFingerprint string                    `json:"manifest_key_fingerprint,omitempty"`
	CreateCredentials      *sandboxcfg.Credentials   `json:"create_credentials,omitempty"`
	StableID               string                    `json:"stable_id,omitempty"`
	APISecret              string                    `json:"api_secret,omitempty"`
	ServiceSecret          string                    `json:"service_secret,omitempty"`
	EnvdAccessToken        string                    `json:"envd_access_token,omitempty"`
	TrafficAccessToken     string                    `json:"traffic_access_token,omitempty"`
	ForwardAccessToken     string                    `json:"forward_access_token,omitempty"`
	TargetPort             int                       `json:"target_port,omitempty"`
	LastActive             int64                     `json:"last_active,omitempty"`
	BuildID                string                    `json:"build_id,omitempty"`
	BuildState             BuildState                `json:"build_state,omitempty"`
	BuildResources         *routesync.BuildResources `json:"build_resources,omitempty"`
	BuildReason            string                    `json:"build_reason,omitempty"`
	CreatedU               int64                     `json:"created_unix,omitempty"`
}

type WatchEventType int

const (
	WatchEventPut WatchEventType = iota + 1
	WatchEventDelete
	WatchEventReset
	WatchEventBookmark
)

// WatchEvent is the registry typed watch surface exposed by node_list and
// route_link group watches. It is projected from shardkv watch events.
type WatchEvent struct {
	Type  WatchEventType
	Key   string
	Value []byte
	Rev   int64
	Token string
}

var ErrWatchCompacted = shardkv.ErrCompacted

// Stores wraps shardkv with registry-specific record codecs.
type Stores struct {
	writerID            string
	memberViews         []clusterstate.MemberView
	routeOwnerCount     int
	nodeOwnerCount      int
	nodeListOwnerCount  int
	scaleLinkOwnerCount int
	replicaMu           sync.RWMutex

	nodeListMu             sync.Mutex
	nodeListWatchRetention int

	shardMu        sync.RWMutex
	shardStore     *shardkv.Store
	shardTransport shardkv.Transport
	shardReady     shardkv.MemberReadyProvider
}

// NewStores builds a size-1 registry shardkv store.
func NewStores() *Stores {
	return NewClusterStores("registry", clusterstate.MemberView{Version: 1, Members: []string{"registry"}}, 1, 1, 1, 1)
}

func NewClusterStores(
	writerID string,
	view clusterstate.MemberView,
	routeOwnerCount, nodeOwnerCount, nodeListOwnerCount, scaleLinkOwnerCount int,
) *Stores {
	if writerID == "" {
		writerID = "registry"
	}
	if len(view.Members) == 0 {
		view = clusterstate.MemberView{Version: 1, Members: []string{writerID}}
	}
	if routeOwnerCount <= 0 {
		routeOwnerCount = 1
	}
	if nodeOwnerCount <= 0 {
		nodeOwnerCount = 1
	}
	if nodeListOwnerCount <= 0 {
		nodeListOwnerCount = 1
	}
	if scaleLinkOwnerCount <= 0 {
		scaleLinkOwnerCount = routeOwnerCount
	}
	stores := &Stores{
		writerID:               writerID,
		memberViews:            []clusterstate.MemberView{view},
		routeOwnerCount:        routeOwnerCount,
		nodeOwnerCount:         nodeOwnerCount,
		nodeListOwnerCount:     nodeListOwnerCount,
		scaleLinkOwnerCount:    scaleLinkOwnerCount,
		nodeListWatchRetention: 10000,
	}
	stores.rebuildShardStore()
	return stores
}

func NewClusterStoresWithViews(
	writerID string,
	views []clusterstate.MemberView,
	routeOwnerCount, nodeOwnerCount, nodeListOwnerCount, scaleLinkOwnerCount int,
) *Stores {
	if len(views) == 0 {
		views = []clusterstate.MemberView{{Version: 1, Members: []string{writerID}}}
	}
	stores := NewClusterStores(writerID, views[0], routeOwnerCount, nodeOwnerCount, nodeListOwnerCount, scaleLinkOwnerCount)
	stores.SetMemberViews(views)
	return stores
}

func (s *Stores) WriterID() string { return s.writerID }

func (s *Stores) NodeOwnerCandidates(ctx context.Context, nodeID string) ([]string, error) {
	store := s.ShardStore()
	if store == nil {
		return nil, errors.New("registry: shard store is not initialized")
	}
	sh, err := store.Shard(shardkv.Namespace(clusterstate.NamespaceNodeLink), clusterstate.NodeLinkShard(nodeID))
	if err != nil {
		return nil, err
	}
	view, err := sh.View(ctx)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	for _, set := range view.WriteSets {
		for _, member := range set.Members {
			id := string(member)
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			out = append(out, id)
		}
	}
	if len(out) == 0 && s.writerID != "" {
		out = append(out, s.writerID)
	}
	return out, nil
}

func (s *Stores) SetNodeListWatchRetention(n int) {
	if n <= 0 {
		n = 10000
	}
	s.nodeListMu.Lock()
	s.nodeListWatchRetention = n
	s.nodeListMu.Unlock()
	s.rebuildShardStore()
}

func (s *Stores) SetMemberViews(views []clusterstate.MemberView) {
	if len(views) == 0 {
		return
	}
	clean := make([]clusterstate.MemberView, 0, len(views))
	seen := map[int64]bool{}
	for _, view := range views {
		if len(view.Members) == 0 || seen[view.Version] {
			continue
		}
		seen[view.Version] = true
		clean = append(clean, view)
	}
	if len(clean) == 0 {
		return
	}
	s.replicaMu.Lock()
	s.memberViews = clean
	s.replicaMu.Unlock()
	s.rebuildShardStore()
}

func (s *Stores) SetClusterTopology(
	views []clusterstate.MemberView,
	routeOwnerCount, nodeOwnerCount int,
) {
	if len(views) == 0 {
		return
	}
	cleanViews := make([]clusterstate.MemberView, 0, len(views))
	seenViews := map[int64]bool{}
	for _, view := range views {
		if len(view.Members) == 0 || seenViews[view.Version] {
			continue
		}
		seenViews[view.Version] = true
		cleanViews = append(cleanViews, view)
	}
	if len(cleanViews) == 0 {
		return
	}
	if routeOwnerCount <= 0 {
		routeOwnerCount = 1
	}
	if nodeOwnerCount <= 0 {
		nodeOwnerCount = 1
	}
	s.replicaMu.Lock()
	s.memberViews = cleanViews
	s.routeOwnerCount = routeOwnerCount
	s.nodeOwnerCount = nodeOwnerCount
	s.replicaMu.Unlock()
	s.rebuildShardStore()
}

func (s *Stores) SetNodeListTopology(views []clusterstate.MemberView, ownerCount int) {
	cleanViews := make([]clusterstate.MemberView, 0, len(views))
	seenVersion := map[int64]bool{}
	for _, view := range views {
		if len(view.Members) == 0 || seenVersion[view.Version] {
			continue
		}
		seenVersion[view.Version] = true
		cleanViews = append(cleanViews, view)
	}
	if len(cleanViews) == 0 {
		return
	}
	if ownerCount <= 0 {
		ownerCount = 1
	}
	s.replicaMu.Lock()
	s.memberViews = cleanViews
	s.nodeListOwnerCount = ownerCount
	s.replicaMu.Unlock()
	s.rebuildShardStore()
}

func (s *Stores) SetPlacerLinkTopology(views []clusterstate.MemberView, ownerCount int) {
	cleanViews := make([]clusterstate.MemberView, 0, len(views))
	seenVersion := map[int64]bool{}
	for _, view := range views {
		if len(view.Members) == 0 || seenVersion[view.Version] {
			continue
		}
		seenVersion[view.Version] = true
		cleanViews = append(cleanViews, view)
	}
	if len(cleanViews) == 0 {
		return
	}
	if ownerCount <= 0 {
		ownerCount = s.routeOwnerCount
	}
	s.replicaMu.Lock()
	s.memberViews = cleanViews
	s.scaleLinkOwnerCount = ownerCount
	s.replicaMu.Unlock()
	s.rebuildShardStore()
}

func (s *Stores) ShardStore() *shardkv.Store {
	s.shardMu.RLock()
	defer s.shardMu.RUnlock()
	return s.shardStore
}

func (s *Stores) Compact(ctx context.Context, now time.Time) (shardkv.GCStats, error) {
	store := s.ShardStore()
	if store == nil {
		return shardkv.GCStats{}, nil
	}
	stats, err := store.Compact(ctx, now)
	if err != nil {
		return stats, err
	}
	if now.IsZero() {
		now = time.Now()
	}
	_, err = s.compactNodeListValueTombstones(ctx, now, nodeListTombstoneRetention)
	return stats, err
}

func (s *Stores) SetShardTransport(transport shardkv.Transport, ready shardkv.MemberReadyProvider) {
	s.shardMu.Lock()
	s.shardTransport = transport
	s.shardReady = ready
	s.shardMu.Unlock()
	s.rebuildShardStore()
}

func (s *Stores) rebuildShardStore() {
	if s == nil {
		return
	}
	s.replicaMu.RLock()
	views := append([]clusterstate.MemberView(nil), s.memberViews...)
	routeOwners, nodeOwners := s.routeOwnerCount, s.nodeOwnerCount
	nodeListOwners, scaleOwners := s.nodeListOwnerCount, s.scaleLinkOwnerCount
	writerID := s.writerID
	s.replicaMu.RUnlock()
	s.nodeListMu.Lock()
	nodeListWatchRetention := s.nodeListWatchRetention
	s.nodeListMu.Unlock()
	if routeOwners <= 0 {
		routeOwners = 1
	}
	if nodeOwners <= 0 {
		nodeOwners = 1
	}
	if nodeListOwners <= 0 {
		nodeListOwners = 1
	}
	if scaleOwners <= 0 {
		scaleOwners = routeOwners
	}
	if nodeListWatchRetention <= 0 {
		nodeListWatchRetention = 10000
	}
	clusterViews := make([]shardkv.ClusterView, 0, len(views))
	for _, view := range views {
		if len(view.Members) == 0 {
			continue
		}
		members := make([]shardkv.MemberID, 0, len(view.Members))
		for _, member := range view.Members {
			if member != "" {
				members = append(members, shardkv.MemberID(member))
			}
		}
		clusterViews = append(clusterViews, shardkv.ClusterView{
			Version:  view.Version,
			Label:    view.LabelOrDefault(),
			Members:  members,
			ReadOnly: view.ReadOnly,
		})
	}
	if len(clusterViews) == 0 {
		clusterViews = []shardkv.ClusterView{{Version: 1, Label: "membership.1", Members: []shardkv.MemberID{shardkv.MemberID(writerID)}}}
	}
	resolver, err := shardkv.NewMaglevResolver(shardkv.MaglevResolverConfig{
		Local: shardkv.MemberID(writerID),
		Views: clusterViews,
		Layout: shardkv.Layout{Namespaces: map[shardkv.Namespace]shardkv.NamespaceSpec{
			shardkv.Namespace(clusterstate.NamespaceRouteLink):  {ShardMemberCount: routeOwners, TombstoneRetention: time.Hour, WatchRetention: 10000, IdleShardTTL: time.Hour},
			shardkv.Namespace(clusterstate.NamespaceNodeLink):   {ShardMemberCount: nodeOwners, TombstoneRetention: time.Hour, WatchRetention: 10000, IdleShardTTL: time.Hour},
			shardkv.Namespace(clusterstate.NamespaceNodeList):   {ShardMemberCount: nodeListOwners, TombstoneRetention: nodeListTombstoneRetention, WatchRetention: nodeListWatchRetention, Pinned: true},
			shardkv.Namespace(clusterstate.NamespacePlacerLink): {ShardMemberCount: scaleOwners, TombstoneRetention: time.Hour, WatchRetention: 10000, IdleShardTTL: time.Hour},
		}},
	})
	if err != nil {
		return
	}
	s.shardMu.RLock()
	transport, ready := s.shardTransport, s.shardReady
	old := s.shardStore
	s.shardMu.RUnlock()
	if old != nil {
		_ = old.Configure(resolver, transport, ready, 10000)
		return
	}
	store, err := shardkv.NewStore(shardkv.StoreOptions{
		Local: shardkv.MemberID(writerID), Resolver: resolver, Transport: transport, Ready: ready,
		DefaultWatchRetention: 10000,
	})
	if err != nil {
		return
	}
	s.shardMu.Lock()
	s.shardStore = store
	s.shardMu.Unlock()
}

var (
	errPlacerLinkStaleLease = errors.New("registry: stale placer_link source lease")
)

func (s *Stores) AcquirePlacerLinkSourceLease(ctx context.Context, sourceID, ownerID, runID string, ttl time.Duration) (clusterstate.PlacerImportSourceState, bool, error) {
	return s.acquirePlacerImportSourceShard(ctx, sourceID, ownerID, runID, ttl)
}

func (s *Stores) CheckPlacerLinkSourceLease(ctx context.Context, sourceID, ownerID, runID string, term uint64) bool {
	return s.checkPlacerImportSourceShard(ctx, sourceID, ownerID, runID, term)
}

func (s *Stores) CheckpointPlacerLinkSource(ctx context.Context, sourceID, ownerID, runID string, term uint64, cursor string, complete bool, lastErr string) (clusterstate.PlacerImportSourceState, error) {
	return s.checkpointPlacerImportSourceShard(ctx, sourceID, ownerID, runID, term, cursor, complete, lastErr)
}

// --- node_link ---

func (s *Stores) PutNode(ctx context.Context, n *NodeRecord) error {
	if _, err := s.putNodeLink(ctx, n); err != nil {
		return err
	}
	return s.PutNodeList(ctx, n)
}

func (s *Stores) GetNode(ctx context.Context, id string) (*NodeRecord, bool, error) {
	return s.getNodeShard(ctx, id)
}

func (s *Stores) GetNodeProfile(ctx context.Context, id string) (*NodeRecord, bool, error) {
	return s.getNodeProfileShard(ctx, id)
}

func (s *Stores) PutNodeList(ctx context.Context, n *NodeRecord) error {
	entry := projectNodeListRecord(n)
	return s.PutNodeListEntry(ctx, entry)
}

func (s *Stores) AddNodeSandboxRef(ctx context.Context, nodeID string, ref clusterstate.NodeSandboxRef) error {
	if nodeID == "" || ref.Group == "" || ref.RouteKey == "" ||
		!validNodeSandboxIdentity(ref.SandboxID, ref.NodeSandboxID, ref.SandboxGeneration) ||
		!validFullFingerprint(ref.APISecretFingerprint) {
		return errors.New("registry: node sandbox ref requires a canonical stable/node identity, group, route_key, and API secret fingerprint")
	}
	return s.addNodeSandboxRefShard(ctx, nodeID, ref)
}

func (s *Stores) GetNodeSandboxRef(ctx context.Context, nodeID, nodeSandboxID string) (clusterstate.NodeSandboxRef, bool, error) {
	return s.getNodeSandboxRefShard(ctx, nodeID, nodeSandboxID)
}

func (s *Stores) RemoveNodeSandboxRef(ctx context.Context, nodeID, nodeSandboxID string) error {
	return s.removeNodeSandboxRefShard(ctx, nodeID, nodeSandboxID)
}

func (s *Stores) AddNodeBuildRef(ctx context.Context, nodeID string, ref clusterstate.NodeBuildRef) error {
	if nodeID == "" || ref.Group == "" || ref.BuildID == "" {
		return errors.New("registry: node build ref requires node_id, build_id, and group")
	}
	return s.addNodeBuildRefShard(ctx, nodeID, ref)
}

func (s *Stores) GetNodeBuildRef(ctx context.Context, nodeID, buildID string) (clusterstate.NodeBuildRef, bool, error) {
	return s.getNodeBuildRefShard(ctx, nodeID, buildID)
}

func (s *Stores) RemoveNodeBuildRef(ctx context.Context, nodeID, buildID string) error {
	return s.removeNodeBuildRefShard(ctx, nodeID, buildID)
}

func (s *Stores) UpsertNodeKeyPair(ctx context.Context, nodeID string, pair clusterstate.NodeKeyPair) error {
	if nodeID == "" {
		return nil
	}
	var err error
	pair, err = normalizeNodeKeyPair(pair)
	if err != nil {
		return err
	}
	return s.upsertNodeKeyPairShard(ctx, nodeID, pair)
}

func (s *Stores) MarkNodeKeyPairAcked(ctx context.Context, nodeID string, expected clusterstate.NodeKeyPair) (bool, error) {
	if nodeID == "" || expected.APISecretFingerprint == "" || expected.ExpiresUnix <= 0 {
		return false, nil
	}
	return s.markNodeKeyPairAckedShard(ctx, nodeID, expected)
}

func (s *Stores) DropNodeKeyPair(ctx context.Context, nodeID, apiSecretFingerprint string) error {
	if nodeID == "" || apiSecretFingerprint == "" {
		return nil
	}
	if !validFullFingerprint(apiSecretFingerprint) {
		return errors.New("registry: API secret fingerprint must be 64 lowercase hex characters")
	}
	return s.dropNodeKeyPairShard(ctx, nodeID, apiSecretFingerprint)
}

func (s *Stores) PruneExpiredNodeKeyPairs(ctx context.Context, nodeID string, nowUnix int64) error {
	if nodeID == "" || nowUnix <= 0 {
		return nil
	}
	rec, found, err := s.getNodeShard(ctx, nodeID)
	if err != nil || !found {
		return err
	}
	for _, pair := range rec.KeyPairs {
		if pair.ExpiresUnix > 0 && pair.ExpiresUnix <= nowUnix {
			if err := s.dropNodeKeyPairShard(ctx, nodeID, pair.APISecretFingerprint); err != nil {
				return err
			}
		}
	}
	return nil
}

func sortNodeKeyPairs(pairs []clusterstate.NodeKeyPair) {
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].APISecretFingerprint < pairs[j].APISecretFingerprint
	})
}

func sameNodeKeyPairMaterial(a, b clusterstate.NodeKeyPair) bool {
	apiSecretEqual := hmac.Equal([]byte(a.APISecret), []byte(b.APISecret))
	apiRefEqual := hmac.Equal([]byte(a.APISecretRef), []byte(b.APISecretRef))
	manifestKeyEqual := hmac.Equal([]byte(a.ManifestKey), []byte(b.ManifestKey))
	manifestRefEqual := hmac.Equal([]byte(a.ManifestKeyRef), []byte(b.ManifestKeyRef))
	return a.APISecretFingerprint == b.APISecretFingerprint &&
		a.APISecretType == b.APISecretType && apiSecretEqual && apiRefEqual &&
		a.ManifestKeyFingerprint == b.ManifestKeyFingerprint &&
		a.ManifestKeyType == b.ManifestKeyType && manifestKeyEqual && manifestRefEqual
}

func normalizeNodeKeyPair(pair clusterstate.NodeKeyPair) (clusterstate.NodeKeyPair, error) {
	if !validFullFingerprint(pair.APISecretFingerprint) {
		return clusterstate.NodeKeyPair{}, errors.New("registry: API secret fingerprint must be 64 lowercase hex characters")
	}
	if !validFullFingerprint(pair.ManifestKeyFingerprint) {
		return clusterstate.NodeKeyPair{}, errors.New("registry: manifest key fingerprint must be 64 lowercase hex characters")
	}
	var err error
	pair.APISecretType, err = normalizePairSecret(
		"API secret", pair.APISecretType, pair.APISecret, pair.APISecretRef, pair.APISecretFingerprint,
	)
	if err != nil {
		return clusterstate.NodeKeyPair{}, err
	}
	pair.ManifestKeyType, err = normalizePairSecret(
		"manifest key", pair.ManifestKeyType, pair.ManifestKey, pair.ManifestKeyRef, pair.ManifestKeyFingerprint,
	)
	if err != nil {
		return clusterstate.NodeKeyPair{}, err
	}
	return pair, nil
}

func normalizePairSecret(name, typ, value, ref, fingerprint string) (string, error) {
	if typ == "" && value != "" && ref == "" {
		typ = clusterstate.SecretInline
	}
	switch typ {
	case clusterstate.SecretInline:
		if value == "" || ref != "" {
			return "", fmt.Errorf("registry: %s inline carrier is incomplete", name)
		}
		if !validSecretValue(value) {
			return "", fmt.Errorf("registry: %s must be 64 lowercase hex characters", name)
		}
		raw, _ := hex.DecodeString(value)
		h := sha256.Sum256(raw)
		if hex.EncodeToString(h[:]) != fingerprint {
			return "", fmt.Errorf("registry: %s fingerprint does not match inline material", name)
		}
	case clusterstate.SecretRef:
		if ref == "" || value != "" {
			return "", fmt.Errorf("registry: %s ref carrier is incomplete", name)
		}
	default:
		return "", fmt.Errorf("registry: unsupported %s carrier type %q", name, typ)
	}
	return typ, nil
}

func validSecretValue(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validFullFingerprint(fingerprint string) bool {
	return validSecretValue(fingerprint)
}

func (s *Stores) putNodeLink(ctx context.Context, n *NodeRecord) (uint64, error) {
	return s.putNodeShard(ctx, n)
}

func projectNodeListRecord(n *NodeRecord) clusterstate.NodeListEntry {
	return clusterstate.ProjectNodeList(toClusterNode(n))
}

func nodeListProjectionNewer(next, cur clusterstate.NodeListEntry) bool {
	if cmp := compareProjectionSource(next, cur); cmp != 0 {
		return cmp > 0
	}
	if next.Deleted != cur.Deleted {
		return next.Deleted
	}
	return false
}

func compareProjectionSource(next, cur clusterstate.NodeListEntry) int {
	nextHasSource := !recordMetaZero(next.SourceMeta)
	curHasSource := !recordMetaZero(cur.SourceMeta)
	switch {
	case nextHasSource && curHasSource:
		return compareRecordMeta(next.SourceMeta, cur.SourceMeta)
	case nextHasSource:
		return 1
	case curHasSource:
		return -1
	default:
		return 0
	}
}

func compareRecordMeta(a, b clusterstate.RecordMeta) int {
	switch {
	case a.Ballot.Less(b.Ballot):
		return -1
	case b.Ballot.Less(a.Ballot):
		return 1
	case a.Rev < b.Rev:
		return -1
	case a.Rev > b.Rev:
		return 1
	default:
		return 0
	}
}

func recordMetaZero(m clusterstate.RecordMeta) bool {
	return m.Ballot.IsZero() && m.Rev == 0 && m.UpdatedAt.IsZero()
}

func (s *Stores) NodeListRev(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	sh, err := s.nodeListRecordSet()
	if err != nil {
		return 0, err
	}
	snap, err := sh.Snapshot(ctx)
	if err != nil {
		return 0, err
	}
	return int64(snap.Rev), nil
}

func (s *Stores) RangeNodeList(ctx context.Context, fn func(clusterstate.NodeListEntry) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.rangeNodeListShard(ctx, fn)
}

func (s *Stores) WatchNodeList(ctx context.Context, fromRev int64) (<-chan WatchEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fromRev < 0 {
		fromRev = 0
	}
	sh, err := s.nodeListRecordSet()
	if err != nil {
		return nil, err
	}
	watch, err := sh.WatchSince(ctx, uint64(fromRev))
	if errors.Is(err, shardkv.ErrCompacted) {
		return nil, ErrWatchCompacted
	}
	if err != nil {
		return nil, err
	}
	ch := make(chan WatchEvent, 1024)
	go func() {
		defer close(ch)
		for ev := range watch.Events {
			out, ok, err := nodeListWatchEvent(ev)
			if err != nil || !ok {
				continue
			}
			select {
			case ch <- out:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func (s *Stores) WatchNodeListToken(ctx context.Context, token string) (<-chan WatchEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sh, err := s.nodeListRecordSet()
	if err != nil {
		return nil, err
	}
	watch, err := sh.Watch(ctx, token)
	if err != nil {
		return nil, err
	}
	ch := make(chan WatchEvent, 1024)
	go func() {
		defer close(ch)
		for ev := range watch.Events {
			out, ok, err := nodeListWatchEvent(ev)
			if err != nil || !ok {
				continue
			}
			select {
			case ch <- out:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func (s *Stores) PutNodeListEntry(ctx context.Context, entry clusterstate.NodeListEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if entry.NodeID == "" {
		return fmt.Errorf("registry: node_list node_id is required")
	}
	entry.Deleted = false
	sh, err := s.nodeListRecordSet()
	if err != nil {
		return err
	}
	key := clusterstate.NodeListRecordKey(entry.NodeID)
	for attempt := 0; attempt < 5; attempt++ {
		curRec, found, err := sh.GetRecord(ctx, key)
		if err != nil {
			return err
		}
		var cur clusterstate.NodeListEntry
		if found {
			cur, err = clusterstate.DecodeShardValue[clusterstate.NodeListEntry](curRec.Value)
			if err != nil {
				return err
			}
		}
		if found && !nodeListProjectionNewer(entry, cur) {
			return nil
		}
		expect := uint64(0)
		if found && !curRec.Deleted {
			expect = curRec.Meta.Rev
		}
		value, err := clusterstate.EncodeShardValue(entry)
		if err != nil {
			return err
		}
		_, ok, err := sh.CAS(ctx, key, expect, value)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		return nil
	}
	return shardkv.ErrConflict
}

func (s *Stores) DeleteNodeList(ctx context.Context, nodeID string) error {
	return s.DeleteNodeListWithSource(ctx, nodeID, clusterstate.RecordMeta{})
}

func (s *Stores) DeleteNodeListWithSource(ctx context.Context, nodeID string, source clusterstate.RecordMeta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if nodeID == "" {
		return nil
	}
	sh, err := s.nodeListRecordSet()
	if err != nil {
		return err
	}
	key := clusterstate.NodeListRecordKey(nodeID)
	for attempt := 0; attempt < 5; attempt++ {
		curRec, found, err := sh.GetRecord(ctx, key)
		if err != nil {
			return err
		}
		var cur clusterstate.NodeListEntry
		if found {
			cur, err = clusterstate.DecodeShardValue[clusterstate.NodeListEntry](curRec.Value)
			if err != nil {
				return err
			}
		}
		tombstone := clusterstate.NodeListEntry{NodeID: nodeID, SourceMeta: source, Deleted: true}
		if recordMetaZero(source) {
			if !found {
				return nil
			}
			tombstone.SourceMeta = cur.SourceMeta
		}
		if found && !nodeListProjectionNewer(tombstone, cur) {
			return nil
		}
		value, err := clusterstate.EncodeShardValue(tombstone)
		if err != nil {
			return err
		}
		// Keep node-list deletion as an ordered value tombstone. A physical
		// shard tombstone cannot be revision-updated or revived by profile order.
		expect := uint64(0)
		if found && !curRec.Deleted {
			expect = curRec.Meta.Rev
		}
		_, ok, err := sh.CAS(ctx, key, expect, value)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		return nil
	}
	return shardkv.ErrConflict
}

// --- route_link ---

func (s *Stores) RouteGroupRev(ctx context.Context, group string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	sh, err := s.routeLinkRecordSet(group, clusterstate.RecordSetRouteSandbox)
	if err != nil {
		return 0, err
	}
	snap, err := sh.Snapshot(ctx)
	if err != nil {
		return 0, err
	}
	return int64(snap.Rev), nil
}

func (s *Stores) WatchRouteGroup(ctx context.Context, group string, fromRev int64) (<-chan WatchEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if group == "" {
		return nil, fmt.Errorf("registry: group is required for route watch")
	}
	if fromRev < 0 {
		fromRev = 0
	}
	sh, err := s.routeLinkRecordSet(group, clusterstate.RecordSetRouteSandbox)
	if err != nil {
		return nil, err
	}
	watch, err := sh.WatchSince(ctx, uint64(fromRev))
	if errors.Is(err, shardkv.ErrCompacted) {
		return nil, ErrWatchCompacted
	}
	if err != nil {
		return nil, err
	}
	ch := make(chan WatchEvent, 1024)
	go func() {
		defer close(ch)
		for ev := range watch.Events {
			out, ok, err := routeWatchEvent(ev)
			if err != nil || !ok {
				continue
			}
			select {
			case ch <- out:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

// GetSandbox returns the route_link record + its revision (for CAS).
func (s *Stores) GetSandbox(ctx context.Context, group, routeKey string) (rec *SandboxRecord, rev int64, found bool, err error) {
	rec, urev, found, err := s.getRouteSandboxShard(ctx, group, routeKey)
	return rec, int64(urev), found, err
}

func (s *Stores) PutSandbox(ctx context.Context, r *SandboxRecord) (int64, error) {
	_, rev, found, err := s.GetSandbox(ctx, r.Group, r.RouteKey)
	if err != nil {
		return 0, err
	}
	expect := int64(0)
	if found {
		expect = rev
	}
	rev, ok, err := s.CASSandbox(ctx, r, expect)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, clusterstate.ErrConflict
	}
	return rev, nil
}

// CASSandbox commits r only if the record's current revision is expectRev
// (expectRev 0 = create-only). Returns (newRev, committed).
func (s *Stores) CASSandbox(ctx context.Context, r *SandboxRecord, expectRev int64) (int64, bool, error) {
	if expectRev < 0 {
		return 0, false, fmt.Errorf("registry: negative route_link revision %d", expectRev)
	}
	rev, ok, err := s.casRouteSandboxShard(ctx, r, uint64(expectRev))
	if err != nil {
		return 0, false, err
	}
	return int64(rev), ok, nil
}

func (s *Stores) DeleteSandbox(ctx context.Context, group, routeKey string) error {
	return s.deleteRouteSandboxShard(ctx, group, routeKey)
}

func (s *Stores) DeleteSandboxIfRevision(ctx context.Context, group, routeKey string, expectRev int64) (bool, error) {
	if expectRev < 0 {
		return false, fmt.Errorf("registry: negative route_link revision %d", expectRev)
	}
	return s.deleteRouteSandboxShardIfRevision(ctx, group, routeKey, uint64(expectRev))
}

// RangeSandboxes streams a group's route_link rows.
func (s *Stores) RangeSandboxes(ctx context.Context, group string, fn func(*SandboxRecord) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if group == "" {
		return fmt.Errorf("registry: group is required for route_link list")
	}
	return s.rangeRouteSandboxesShard(ctx, group, fn)
}

func (s *Stores) RangeBuildsInGroup(ctx context.Context, group string, fn func(*BuildRecord) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if group == "" {
		return fmt.Errorf("registry: group is required for route_link list")
	}
	return s.rangeRouteBuildsShard(ctx, group, fn)
}

func toClusterNode(n *NodeRecord) clusterstate.NodeRecord {
	if n == nil {
		return clusterstate.NodeRecord{}
	}
	st := clusterstate.NodeLive
	if n.Draining {
		st = clusterstate.NodeDrained
	}
	return clusterstate.NodeRecord{
		Meta:                      n.Meta,
		NodeID:                    n.NodeID,
		State:                     st,
		Labels:                    cloneStringMap(n.Labels),
		Capacity:                  n.Capacity,
		BuildRegistrationCapacity: cloneBuildAdmissionLimit(n.BuildRegistrationCapacity),
		BuildExecutionCapacity:    cloneBuildAdmissionLimit(n.BuildExecutionCapacity),
		DataEndpoint:              n.DataEndpoint,
		RuntimeDigest:             n.RuntimeDigest,
		Zone:                      n.Zone,
		Allocated:                 n.Allocated,
		Pool:                      n.Pool,
		BuildRegistrationUsage:    cloneBuildAdmissionUsage(n.BuildRegistrationUsage),
		BuildExecutionUsage:       cloneBuildAdmissionUsage(n.BuildExecutionUsage),
		Counts:                    n.Counts,
		Draining:                  n.Draining,
		LastHeartbeatUnix:         n.LastHeartbeatUnix,
		ResumeToken:               n.ResumeToken,
		LinkOwner:                 n.LinkOwner,
		KeyPairs:                  cloneNodeKeyPairs(n.KeyPairs),
		Sandboxes:                 cloneNodeSandboxRefs(n.Sandboxes),
		Builds:                    cloneNodeBuildRefs(n.Builds),
	}
}

func cloneNodeRecord(n *NodeRecord) *NodeRecord {
	if n == nil {
		return nil
	}
	return &NodeRecord{
		Meta:                      n.Meta,
		NodeID:                    n.NodeID,
		Labels:                    cloneStringMap(n.Labels),
		Capacity:                  n.Capacity,
		BuildRegistrationCapacity: cloneBuildAdmissionLimit(n.BuildRegistrationCapacity),
		BuildExecutionCapacity:    cloneBuildAdmissionLimit(n.BuildExecutionCapacity),
		DataEndpoint:              n.DataEndpoint,
		RuntimeDigest:             n.RuntimeDigest,
		Zone:                      n.Zone,
		Allocated:                 n.Allocated,
		Pool:                      n.Pool,
		BuildRegistrationUsage:    cloneBuildAdmissionUsage(n.BuildRegistrationUsage),
		BuildExecutionUsage:       cloneBuildAdmissionUsage(n.BuildExecutionUsage),
		Counts:                    n.Counts,
		Draining:                  n.Draining,
		LastHeartbeatUnix:         n.LastHeartbeatUnix,
		ResumeToken:               n.ResumeToken,
		LinkOwner:                 n.LinkOwner,
		KeyPairs:                  cloneNodeKeyPairs(n.KeyPairs),
		Sandboxes:                 cloneNodeSandboxRefs(n.Sandboxes),
		Builds:                    cloneNodeBuildRefs(n.Builds),
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneNodeKeyPairs(in []clusterstate.NodeKeyPair) []clusterstate.NodeKeyPair {
	if len(in) == 0 {
		return nil
	}
	out := make([]clusterstate.NodeKeyPair, len(in))
	copy(out, in)
	return out
}

func cloneNodeSandboxRefs(in []clusterstate.NodeSandboxRef) []clusterstate.NodeSandboxRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]clusterstate.NodeSandboxRef, len(in))
	copy(out, in)
	return out
}

func cloneNodeBuildRefs(in []clusterstate.NodeBuildRef) []clusterstate.NodeBuildRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]clusterstate.NodeBuildRef, len(in))
	copy(out, in)
	return out
}
