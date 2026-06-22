package registry

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// BuildStore (cluster.md §6.1, the 4th table): builds are group-sharded
// (build/<esc(group)>/<build_id>), tracked through registered → building → ready
// /error. A registered/building build's resources occupy its node's build pool
// (§7.5 "RESERVED 即占用"); a terminal (ready/error) build no longer occupies.

// BuildState mirrors the node's build lifecycle for placement accounting.
type BuildState string

const (
	BuildRegistered BuildState = "registered"
	BuildBuilding   BuildState = "building"
	BuildReady      BuildState = "ready"
	BuildError      BuildState = "error"
)

// BuildRecord is the registry's view of a build (the node runs it + reports state).
type BuildRecord struct {
	Group      string                    `json:"group"`
	BuildID    string                    `json:"build_id"`
	NodeID     string                    `json:"node_id"`
	Resources  *routesync.BuildResources `json:"resources,omitempty"`
	State      BuildState                `json:"state"`
	TemplateID string                    `json:"template_id,omitempty"` // persist id once ready
	Reason     string                    `json:"reason,omitempty"`
	CreatedU   int64                     `json:"created_unix,omitempty"`
}

// occupies reports whether the build still holds its reserved build resources
// (registered/building do; ready/error released).
func (b *BuildRecord) occupies() bool { return b.State == BuildRegistered || b.State == BuildBuilding }

func buildKey(group, buildID string) string {
	return buildPrefix + url.PathEscape(group) + "/" + url.PathEscape(buildID)
}

func (s *Stores) PutBuild(ctx context.Context, b *BuildRecord) error {
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	_, err = s.kv.Put(ctx, buildKey(b.Group, b.BuildID), raw)
	return err
}

func (s *Stores) GetBuildInGroup(ctx context.Context, group, buildID string) (*BuildRecord, bool, error) {
	kv, found, err := s.kv.Get(ctx, buildKey(group, buildID))
	if err != nil || !found {
		return nil, found, err
	}
	var b BuildRecord
	if err := json.Unmarshal(kv.Value, &b); err != nil {
		return nil, false, err
	}
	return &b, true, nil
}

func (s *Stores) DeleteBuild(ctx context.Context, group, buildID string) error {
	_, err := s.kv.Delete(ctx, buildKey(group, buildID))
	return err
}

// RangeBuilds streams every build row across groups (placement headroom accounting
// + the dead-node build→error sweep).
func (s *Stores) RangeBuilds(ctx context.Context, fn func(*BuildRecord) error) error {
	return s.kv.Range(ctx, buildPrefix, func(kv clusterstore.KV) error {
		var b BuildRecord
		if err := json.Unmarshal(kv.Value, &b); err != nil {
			return err
		}
		return fn(&b)
	})
}
