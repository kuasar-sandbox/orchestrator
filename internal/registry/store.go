// Package registry implements the cluster control plane's domain services.
// Durable records are accessed only through typed metadata capabilities.
package registry

import (
	"context"
	"errors"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"time"
)

type SandboxState = metadata.SandboxState

const (
	StateNone     = metadata.StateNone
	StateReserved = metadata.StateReserved
	StateReady    = metadata.StateReady
	StatePaused   = metadata.StatePaused
	StatePausing  = metadata.StatePausing
	StateDead     = metadata.StateDead
	StateDeleting = metadata.StateDeleting
)

type SandboxRecord = metadata.SandboxRecord

type NodeRecord struct {
	Version                   metadata.Version               `json:"version"`
	Registration              metadata.Registration          `json:"-"`
	Reaping                   bool                           `json:"-"`
	NodeID                    string                         `json:"node_id"`
	Labels                    map[string]string              `json:"labels,omitempty"`
	Capacity                  int                            `json:"capacity,omitempty"`
	BuildRegistrationCapacity *routesync.BuildAdmissionLimit `json:"build_registration_capacity,omitempty"`
	BuildExecutionCapacity    *routesync.BuildAdmissionLimit `json:"build_execution_capacity,omitempty"`
	APIEndpoint               string                         `json:"api_endpoint,omitempty"`
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

// Stores keeps business storage capabilities separate from connection routing.
// It cannot expose arbitrary keys, record sets, native transactions or replicas.
type Stores struct {
	routes       metadata.RouteRepository
	builds       metadata.BuildRepository
	nodes        metadata.NodeRepository
	ownership    metadata.OwnershipRepository
	keyPairs     metadata.KeyPairRepository
	directory    metadata.NodeDirectory
	imports      metadata.ImportSourceRepository
	observations metadata.ObservationAdmission
	owners       metadata.ConnectionOwners
}

func NewStores(repos metadata.Repositories, owners metadata.ConnectionOwners) (*Stores, error) {
	if repos.Routes == nil || repos.Builds == nil || repos.Nodes == nil || repos.Ownership == nil || repos.KeyPairs == nil || repos.Directory == nil || repos.Imports == nil || repos.Observations == nil || owners == nil || owners.LocalID() == "" {
		return nil, errors.New("registry: all typed metadata repositories and connection owners are required")
	}
	return &Stores{routes: repos.Routes, builds: repos.Builds, nodes: repos.Nodes, ownership: repos.Ownership, keyPairs: repos.KeyPairs, directory: repos.Directory, imports: repos.Imports, observations: repos.Observations, owners: owners}, nil
}
func (s *Stores) WriterID() string { return s.owners.LocalID() }
func (s *Stores) NodeOwnerCandidates(ctx context.Context, nodeID string) ([]string, error) {
	return s.owners.Candidates(ctx, nodeID)
}

var errPlacerLinkStaleLease = metadata.ErrStaleLease

func (s *Stores) AcquirePlacerLinkSourceLease(ctx context.Context, sourceID, ownerID, runID string, ttl time.Duration) (clusterstate.PlacerImportSourceState, bool, error) {
	return s.imports.Acquire(ctx, sourceID, ownerID, runID, ttl)
}
func (s *Stores) CheckPlacerLinkSourceLease(ctx context.Context, sourceID, ownerID, runID string, term uint64) bool {
	valid, err := s.imports.Check(ctx, sourceID, ownerID, runID, term)
	return err == nil && valid
}
func (s *Stores) CheckpointPlacerLinkSource(ctx context.Context, sourceID, ownerID, runID string, term uint64, cursor string, complete bool, lastErr string) (clusterstate.PlacerImportSourceState, error) {
	return s.imports.Checkpoint(ctx, sourceID, ownerID, runID, term, cursor, complete, lastErr)
}

func (s *Stores) AddNodeSandboxRef(ctx context.Context, nodeID string, ref clusterstate.NodeSandboxRef) error {
	if nodeID == "" || ref.Group == "" || ref.RouteKey == "" || !validNodeSandboxIdentity(ref.SandboxID, ref.NodeSandboxID, ref.SandboxGeneration) || !validFullFingerprint(ref.APISecretFingerprint) {
		return errors.New("registry: invalid node sandbox ownership identity")
	}
	if err := metadata.ValidateSandboxRef(metadata.NodeSandboxRef(ref)); err != nil {
		return err
	}
	return s.ownership.EnsureSandboxOwner(ctx, nodeID, metadata.NodeSandboxRef(ref))
}
func (s *Stores) GetNodeSandboxRef(ctx context.Context, nodeID, sandboxID string) (clusterstate.NodeSandboxRef, bool, error) {
	v, err := s.ownership.ReadSandboxOwner(ctx, nodeID, sandboxID)
	return clusterstate.NodeSandboxRef(v.Value), v.Exists, err
}
func (s *Stores) RemoveNodeSandboxRef(ctx context.Context, nodeID, sandboxID string) error {
	v, err := s.ownership.ReadSandboxOwner(ctx, nodeID, sandboxID)
	if err != nil || !v.Exists {
		return err
	}
	return s.ownership.RemoveSandboxOwner(ctx, nodeID, sandboxID, v.Version)
}
func (s *Stores) AddNodeBuildRef(ctx context.Context, nodeID string, ref clusterstate.NodeBuildRef) error {
	if nodeID == "" || ref.Group == "" || ref.BuildID == "" {
		return errors.New("registry: node build ref requires node_id, build_id, and group")
	}
	return s.ownership.EnsureBuildOwner(ctx, nodeID, metadata.NodeBuildRef(ref))
}
func (s *Stores) GetNodeBuildRef(ctx context.Context, nodeID, buildID string) (clusterstate.NodeBuildRef, bool, error) {
	v, err := s.ownership.ReadBuildOwner(ctx, nodeID, buildID)
	return clusterstate.NodeBuildRef(v.Value), v.Exists, err
}
func (s *Stores) getNodeBuildRefVersion(ctx context.Context, nodeID, buildID string) (clusterstate.NodeBuildRef, metadata.Version, bool, error) {
	v, err := s.ownership.ReadBuildOwner(ctx, nodeID, buildID)
	return clusterstate.NodeBuildRef(v.Value), v.Version, v.Exists, err
}
func (s *Stores) RemoveNodeBuildRef(ctx context.Context, nodeID, buildID string) error {
	v, err := s.ownership.ReadBuildOwner(ctx, nodeID, buildID)
	if err != nil || !v.Exists {
		return err
	}
	return s.ownership.RemoveBuildOwner(ctx, nodeID, buildID, v.Version)
}
func (s *Stores) removeNodeBuildRefAtRevision(ctx context.Context, nodeID, buildID string, expected metadata.Version) (bool, error) {
	err := s.ownership.RemoveBuildOwner(ctx, nodeID, buildID, expected)
	if errors.Is(err, metadata.ErrConflict) {
		return false, nil
	}
	return err == nil, err
}

func (s *Stores) UpsertNodeKeyPair(ctx context.Context, nodeID string, pair clusterstate.NodeKeyPair) error {
	if nodeID == "" {
		return nil
	}
	pair, err := normalizeNodeKeyPair(pair)
	if err != nil {
		return err
	}
	_, err = s.keyPairs.PutKeyPair(ctx, nodeID, metadata.NodeKeyPair(pair))
	return err
}
func (s *Stores) MarkNodeKeyPairAcked(ctx context.Context, nodeID string, pair clusterstate.NodeKeyPair) (bool, error) {
	if nodeID == "" || pair.APISecretFingerprint == "" || pair.ExpiresUnix <= 0 {
		return false, nil
	}
	return s.keyPairs.AckKeyPair(ctx, nodeID, metadata.NodeKeyPair(pair))
}
func (s *Stores) DropNodeKeyPair(ctx context.Context, nodeID, fingerprint string) error {
	if nodeID == "" || fingerprint == "" {
		return nil
	}
	if !validFullFingerprint(fingerprint) {
		return errors.New("registry: invalid API secret fingerprint")
	}
	for cursor := metadata.Cursor(""); ; {
		page, err := s.keyPairs.ListKeyPairs(ctx, nodeID, cursor, 256)
		if err != nil {
			return err
		}
		for _, p := range page.Items {
			if p.Value.APISecretFingerprint == fingerprint {
				return s.keyPairs.DropKeyPair(ctx, nodeID, fingerprint, p.Version)
			}
		}
		if page.Next == "" {
			return nil
		}
		cursor = page.Next
	}
}
func (s *Stores) PruneExpiredNodeKeyPairs(ctx context.Context, nodeID string, now int64) error {
	for cursor := metadata.Cursor(""); ; {
		page, err := s.keyPairs.ListKeyPairs(ctx, nodeID, cursor, 256)
		if err != nil {
			return err
		}
		for _, p := range page.Items {
			if p.Value.ExpiresUnix > 0 && p.Value.ExpiresUnix <= now {
				if err := s.keyPairs.DropKeyPair(ctx, nodeID, p.Value.APISecretFingerprint, p.Version); err != nil && !errors.Is(err, metadata.ErrConflict) {
					return err
				}
			}
		}
		if page.Next == "" {
			return nil
		}
		cursor = page.Next
	}
}

func sameNodeKeyPairMaterial(a, b clusterstate.NodeKeyPair) bool {
	return metadata.SameKeyPairMaterial(metadata.NodeKeyPair(a), metadata.NodeKeyPair(b))
}
func normalizeNodeKeyPair(pair clusterstate.NodeKeyPair) (clusterstate.NodeKeyPair, error) {
	out, err := metadata.NormalizeKeyPair(metadata.NodeKeyPair(pair))
	return clusterstate.NodeKeyPair(out), err
}
func validSecretValue(value string) bool     { return metadata.ValidFingerprint(value) }
func validFullFingerprint(value string) bool { return metadata.ValidFingerprint(value) }

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
