package registry

import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

// BuildStore is the registry-facing routing view of build execution state. Builds
// are stored as route_link build records and tracked through registered →
// waiting → building → ready/error. The node's SQLite state and heartbeat are
// the authoritative source for admission usage.

// BuildState mirrors the node's build lifecycle for placement accounting.
type BuildState = metadata.BuildState

const (
	BuildStarting   BuildState = "starting"
	BuildRegistered BuildState = "registered"
	BuildWaiting    BuildState = "waiting"
	BuildBuilding   BuildState = "building"
	BuildReady      BuildState = "ready"
	BuildError      BuildState = "error"
)

// BuildRecord is the Registry's retention-bounded projection of a node Build.
// It is query/routing state, never the authority for a canonical TemplateID.
type BuildRecord metadata.BuildRecord

// occupies reports whether the registry still considers the build live for node
// ownership and disconnect reconciliation. It does not calculate admission usage.
func (b *BuildRecord) occupies() bool {
	return b.State == BuildStarting || b.State == BuildRegistered || b.State == BuildWaiting || b.State == BuildBuilding
}

func (s *Stores) PutBuild(ctx context.Context, b *BuildRecord) error {
	_, err := s.putBuild(ctx, b)
	return err
}

func (s *Stores) GetBuildInGroup(ctx context.Context, group, buildID string) (*BuildRecord, bool, error) {
	rec, _, found, err := s.getBuild(ctx, group, buildID)
	return rec, found, err
}

func (s *Stores) DeleteBuild(ctx context.Context, group, buildID string) error {
	return s.deleteBuild(ctx, group, buildID)
}
