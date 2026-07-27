package registry

import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// BuildStore is the registry-facing view of build execution state. Builds are
// stored as route_link build records and tracked through registered → building
// → ready/error. A registered/building build's resources occupy its node's
// build pool (§7.5 "RESERVED 即占用"); a terminal (ready/error) build no longer
// occupies.

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
	Group                string                    `json:"group"`
	BuildID              string                    `json:"build_id"`
	NodeID               string                    `json:"node_id"`
	Profile              types.Profile             `json:"profile"`
	APISecretFingerprint string                    `json:"api_secret_fingerprint"`
	Resources            *routesync.BuildResources `json:"resources,omitempty"`
	State                BuildState                `json:"state"`
	TemplateID           string                    `json:"template_id,omitempty"` // assigned template id, refreshed from terminal node events
	Reason               string                    `json:"reason,omitempty"`
	CreatedU             int64                     `json:"created_unix,omitempty"`
}

// occupies reports whether the build still holds its reserved build resources
// (registered/building do; ready/error released).
func (b *BuildRecord) occupies() bool { return b.State == BuildRegistered || b.State == BuildBuilding }

func (s *Stores) PutBuild(ctx context.Context, b *BuildRecord) error {
	_, err := s.putRouteBuildShard(ctx, b)
	return err
}

func (s *Stores) GetBuildInGroup(ctx context.Context, group, buildID string) (*BuildRecord, bool, error) {
	rec, _, found, err := s.getRouteBuildShard(ctx, group, buildID)
	return rec, found, err
}

func (s *Stores) DeleteBuild(ctx context.Context, group, buildID string) error {
	return s.deleteRouteBuildShard(ctx, group, buildID)
}
