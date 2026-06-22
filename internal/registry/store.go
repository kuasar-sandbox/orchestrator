// Package registry is the cluster control plane's durable state authority + node
// channel hub (cluster.md §4/§6/§7). It layers four logical tables on the shared
// clusterstore KV — node (global), sandbox-group config, sandbox + build
// (group-sharded) — and drives sandboxes onto nodes via node-link (node.md §10):
// ReserveSandbox places (a Placer suggests, the registry commits by CAS), sends
// a create/connect command down a node's channel, and waits for the node to
// report the sandbox running on its route stream.
//
// Phase 2 ships the sandbox path (node + group + sandbox tables, ReserveSandbox,
// the node-link server) with a built-in single-node Placer; build registry,
// scaler, router, and key distribution land in later phases.
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/secretbox"
)

// Key prefixes for the registry's logical tables on the shared clusterstore.
const (
	nodePrefix    = "node/"    // node/<node_id>              (global)
	groupPrefix   = "group/"   // group/<group>               (config; manifest_key sealed)
	sandboxPrefix = "sandbox/" // sandbox/<group>/<route_key> (group-sharded)
	buildPrefix   = "build/"   // build/<esc(group)>/<build_id> (group-sharded, §6.1)
)

// SandboxState mirrors cluster.md §7.1 (no WARM; warm = remote snapshot + restore).
type SandboxState string

const (
	StateNone     SandboxState = "none"
	StateReserved SandboxState = "reserved"
	StateReady    SandboxState = "ready"
	StatePaused   SandboxState = "paused"
	StateSaved    SandboxState = "saved"
)

// NodeRecord is the global node table row (cluster.md §6.1): identity + capacity
// from register, water level from heartbeat.
type NodeRecord struct {
	NodeID        string                    `json:"node_id"`
	Labels        map[string]string         `json:"labels,omitempty"`
	Capacity      int                       `json:"capacity,omitempty"`
	BuildCapacity *routesync.BuildResources `json:"build_capacity,omitempty"`
	DataEndpoint  string                    `json:"data_endpoint,omitempty"`
	RuntimeDigest string                    `json:"runtime_digest,omitempty"`
	Zone          string                    `json:"zone,omitempty"`
	Allocated     int64                     `json:"allocated,omitempty"`
	Pool          int64                     `json:"pool,omitempty"`
	BuildAlloc    *routesync.BuildResources `json:"build_alloc,omitempty"`
	Counts        int                       `json:"counts,omitempty"`
	Draining      bool                      `json:"draining,omitempty"`
	// LastHeartbeatUnix is the last sign of life (register or heartbeat); the
	// dead-node sweep (§11) resets a disconnected node whose last beat predates
	// node_dead_after.
	LastHeartbeatUnix int64 `json:"last_heartbeat_unix,omitempty"`
}

// SandboxRecord is a group-sharded sandbox row, keyed (group, route_key).
type SandboxRecord struct {
	Group          string       `json:"group"`
	RouteKey       string       `json:"route_key"`
	SID            string       `json:"sid,omitempty"`
	State          SandboxState `json:"state"`
	NodeID         string       `json:"node_id,omitempty"`
	AccessToken    string       `json:"access_token,omitempty"`
	MigrationToken string       `json:"migration_token,omitempty"`
	SnapLoc        string       `json:"snap_loc,omitempty"`
	TemplateID     string       `json:"template_id,omitempty"`
	LastActive     int64        `json:"last_active,omitempty"`
}

// GroupConfig is the sandbox-group config (cluster.md §6.2). The skeleton stores
// it whole via the store provider; the fine-grained provider split is Phase 7.
// ManifestKey (hex) is sealed at rest by the registry's box.
type GroupConfig struct {
	Group         string              `json:"group"` // group path (the store key)
	ProjectID     string              `json:"project_id,omitempty"`
	ManifestKey   string              `json:"manifest_key,omitempty"`  // hex; sealed on store, plain in memory
	RegistryAuth  string              `json:"registry_auth,omitempty"` // build image-pull creds (docker config.json); sealed on store
	SandboxConfig map[string]string   `json:"sandbox_config,omitempty"`
	ImageRepo     string              `json:"image_repo,omitempty"`
	TemplateRef   string              `json:"template_ref,omitempty"`
	NodeSelectors []map[string]string `json:"node_selectors,omitempty"`
	ShuffleLabels map[string]string   `json:"shuffle_labels,omitempty"`
}

// Stores wraps the shared KV with typed, per-table accessors. The box seals the
// group manifest_key at rest (cluster.md §6.2 store provider).
type Stores struct {
	kv  clusterstore.Store
	box *secretbox.Box
}

// NewStores builds the typed store layer over a clusterstore (box may be nil
// only if no group manifest_key is ever stored).
func NewStores(kv clusterstore.Store, box *secretbox.Box) *Stores {
	return &Stores{kv: kv, box: box}
}

func nodeKey(id string) string     { return nodePrefix + id }
func groupKey(group string) string { return groupPrefix + group }

// sandboxKey is sandbox/<esc(group)>/<esc(route_key)>. group AND route_key can
// contain '/', so each segment is URL-path-escaped: that makes the key injective
// (no aliasing of (group,route_key) pairs) and the per-group range prefix
// unambiguous (no bleed from a nested group like "/a" into "/a/b"). The value
// carries the unescaped group/route_key, so the key is never decoded.
func sandboxKey(group, routeKey string) string {
	return sandboxPrefix + url.PathEscape(group) + "/" + url.PathEscape(routeKey)
}
func sandboxGroupPrefix(group string) string { return sandboxPrefix + url.PathEscape(group) + "/" }

// --- node table (global) ---

func (s *Stores) PutNode(ctx context.Context, n *NodeRecord) error {
	b, err := json.Marshal(n)
	if err != nil {
		return err
	}
	_, err = s.kv.Put(ctx, nodeKey(n.NodeID), b)
	return err
}

func (s *Stores) GetNode(ctx context.Context, id string) (*NodeRecord, bool, error) {
	kv, found, err := s.kv.Get(ctx, nodeKey(id))
	if err != nil || !found {
		return nil, found, err
	}
	var n NodeRecord
	if err := json.Unmarshal(kv.Value, &n); err != nil {
		return nil, false, err
	}
	return &n, true, nil
}

func (s *Stores) DeleteNode(ctx context.Context, id string) error {
	_, err := s.kv.Delete(ctx, nodeKey(id))
	return err
}

// RangeNodes streams every node record (read-only callback).
func (s *Stores) RangeNodes(ctx context.Context, fn func(*NodeRecord) error) error {
	return s.kv.Range(ctx, nodePrefix, func(kv clusterstore.KV) error {
		var n NodeRecord
		if err := json.Unmarshal(kv.Value, &n); err != nil {
			return err
		}
		return fn(&n)
	})
}

// --- sandbox table (group-sharded) ---

// GetSandbox returns the (group, route_key) record + its store revision (for CAS).
func (s *Stores) GetSandbox(ctx context.Context, group, routeKey string) (rec *SandboxRecord, rev int64, found bool, err error) {
	kv, found, err := s.kv.Get(ctx, sandboxKey(group, routeKey))
	if err != nil || !found {
		return nil, 0, found, err
	}
	var r SandboxRecord
	if err := json.Unmarshal(kv.Value, &r); err != nil {
		return nil, 0, false, err
	}
	return &r, kv.ModRev, true, nil
}

func (s *Stores) PutSandbox(ctx context.Context, r *SandboxRecord) (int64, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return 0, err
	}
	return s.kv.Put(ctx, sandboxKey(r.Group, r.RouteKey), b)
}

// CASSandbox commits r only if the record's current revision is expectRev
// (expectRev 0 = create-only). Returns (newRev, committed).
func (s *Stores) CASSandbox(ctx context.Context, r *SandboxRecord, expectRev int64) (int64, bool, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return 0, false, err
	}
	return s.kv.CAS(ctx, sandboxKey(r.Group, r.RouteKey), expectRev, b)
}

func (s *Stores) DeleteSandbox(ctx context.Context, group, routeKey string) error {
	_, err := s.kv.Delete(ctx, sandboxKey(group, routeKey))
	return err
}

// RangeSandboxes streams a group's sandbox rows (cluster.md §8 list = this).
func (s *Stores) RangeSandboxes(ctx context.Context, group string, fn func(*SandboxRecord) error) error {
	return s.kv.Range(ctx, sandboxGroupPrefix(group), func(kv clusterstore.KV) error {
		var r SandboxRecord
		if err := json.Unmarshal(kv.Value, &r); err != nil {
			return err
		}
		return fn(&r)
	})
}

// RangeAllSandboxes streams every sandbox row across groups (the dead-node sweep
// scans these to reset a failed node's sandboxes, §11).
func (s *Stores) RangeAllSandboxes(ctx context.Context, fn func(*SandboxRecord) error) error {
	return s.kv.Range(ctx, sandboxPrefix, func(kv clusterstore.KV) error {
		var r SandboxRecord
		if err := json.Unmarshal(kv.Value, &r); err != nil {
			return err
		}
		return fn(&r)
	})
}

// --- group config (manifest_key sealed at rest) ---

func (s *Stores) PutGroup(ctx context.Context, g *GroupConfig) error {
	stored := *g
	if g.ManifestKey != "" || g.RegistryAuth != "" {
		if s.box == nil {
			return fmt.Errorf("registry: group %q has sealed secrets (manifest_key / registry_auth) but no encryption box configured", g.Group)
		}
		if g.ManifestKey != "" {
			enc, err := s.box.EncryptString(g.ManifestKey)
			if err != nil {
				return err
			}
			stored.ManifestKey = enc
		}
		if g.RegistryAuth != "" {
			enc, err := s.box.EncryptString(g.RegistryAuth)
			if err != nil {
				return err
			}
			stored.RegistryAuth = enc
		}
	}
	b, err := json.Marshal(&stored)
	if err != nil {
		return err
	}
	_, err = s.kv.Put(ctx, groupKey(g.Group), b)
	return err
}

// unsealGroup decrypts a group's sealed secrets (manifest_key + registry_auth) in
// place. Both are sealed at rest by PutGroup; both are plain in memory.
func (s *Stores) unsealGroup(g *GroupConfig) error {
	if s.box == nil {
		return nil
	}
	if g.ManifestKey != "" {
		dec, err := s.box.DecryptString(g.ManifestKey)
		if err != nil {
			return fmt.Errorf("registry: decrypt group manifest_key: %w", err)
		}
		g.ManifestKey = dec
	}
	if g.RegistryAuth != "" {
		dec, err := s.box.DecryptString(g.RegistryAuth)
		if err != nil {
			return fmt.Errorf("registry: decrypt group registry_auth: %w", err)
		}
		g.RegistryAuth = dec
	}
	return nil
}

// RangeGroups streams every group config (secrets decrypted) — the key
// distributor reconciles predistribution leases over these (§7.6).
func (s *Stores) RangeGroups(ctx context.Context, fn func(*GroupConfig) error) error {
	return s.kv.Range(ctx, groupPrefix, func(kv clusterstore.KV) error {
		var g GroupConfig
		if err := json.Unmarshal(kv.Value, &g); err != nil {
			return err
		}
		if err := s.unsealGroup(&g); err != nil {
			return err
		}
		return fn(&g)
	})
}

// GetGroupByID returns the group config for an exact group id (secrets decrypted),
// or (nil,false) if absent.
func (s *Stores) GetGroupByID(ctx context.Context, group string) (*GroupConfig, bool, error) {
	kv, found, err := s.kv.Get(ctx, groupKey(group))
	if err != nil || !found {
		return nil, found, err
	}
	var g GroupConfig
	if err := json.Unmarshal(kv.Value, &g); err != nil {
		return nil, false, err
	}
	if err := s.unsealGroup(&g); err != nil {
		return nil, false, err
	}
	return &g, true, nil
}
