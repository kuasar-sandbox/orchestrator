package registry

import (
	"context"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// BuildStore is the registry-facing view of build execution state. Builds are
// stored as reserved route_link records under the build route-key namespace and
// tracked through registered → building → ready/error. A registered/building
// build's resources occupy its node's build pool (§7.5 "RESERVED 即占用"); a
// terminal (ready/error) build no longer occupies.

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

func (s *Stores) PutBuild(ctx context.Context, b *BuildRecord) error {
	rec := sandboxRecordFromBuild(b)
	_, err := s.PutSandbox(ctx, rec)
	return err
}

func (s *Stores) GetBuildInGroup(ctx context.Context, group, buildID string) (*BuildRecord, bool, error) {
	rec, _, found, err := s.GetSandbox(ctx, group, buildRouteKey(buildID))
	if err != nil || !found {
		return nil, found, err
	}
	b := buildRecordFromSandbox(rec)
	if b.BuildID == "" {
		return nil, false, nil
	}
	return b, true, nil
}

func (s *Stores) DeleteBuild(ctx context.Context, group, buildID string) error {
	return s.DeleteSandbox(ctx, group, buildRouteKey(buildID))
}

func buildRecordFromSandbox(rec *SandboxRecord) *BuildRecord {
	if rec == nil {
		return nil
	}
	return &BuildRecord{
		Group:      rec.Group,
		BuildID:    rec.BuildID,
		NodeID:     rec.NodeID,
		Resources:  rec.BuildResources,
		State:      rec.BuildState,
		TemplateID: rec.TemplateID,
		Reason:     rec.BuildReason,
		CreatedU:   rec.CreatedU,
	}
}

func sandboxRecordFromBuild(b *BuildRecord) *SandboxRecord {
	if b == nil {
		return nil
	}
	return &SandboxRecord{
		Group:          b.Group,
		RouteKey:       buildRouteKey(b.BuildID),
		NodeID:         b.NodeID,
		TemplateID:     b.TemplateID,
		BuildID:        b.BuildID,
		BuildState:     b.State,
		BuildResources: b.Resources,
		BuildReason:    b.Reason,
		CreatedU:       b.CreatedU,
	}
}
