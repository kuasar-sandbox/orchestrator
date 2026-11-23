package registry

import (
	"context"
	"errors"
	"fmt"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func (s *Stores) GetSandbox(ctx context.Context, group, key string) (*SandboxRecord, metadata.Version, bool, error) {
	v, err := s.routes.ReadRoute(ctx, metadata.RouteKey{Group: group, RouteKey: key})
	if err != nil || !v.Exists {
		return nil, v.Version, v.Exists, err
	}
	if err := validateSandboxRecord(&v.Value); err != nil {
		return nil, metadata.Version{}, false, err
	}
	return &v.Value, v.Version, true, nil
}
func (s *Stores) PutSandbox(ctx context.Context, r *SandboxRecord) (metadata.Version, error) {
	if err := validateSandboxRecord(r); err != nil {
		return metadata.Version{}, err
	}
	_, v, _, err := s.GetSandbox(ctx, r.Group, r.RouteKey)
	if err != nil {
		return metadata.Version{}, err
	}
	rev, ok, err := s.CASSandbox(ctx, r, v)
	if err == nil && !ok {
		err = metadata.ErrConflict
	}
	return rev, err
}
func (s *Stores) CASSandbox(ctx context.Context, r *SandboxRecord, expected metadata.Version) (metadata.Version, bool, error) {
	if err := validateSandboxRecord(r); err != nil {
		return metadata.Version{}, false, err
	}
	v, err := s.routes.CompareAndPutRoute(ctx, metadata.RouteKey{Group: r.Group, RouteKey: r.RouteKey}, expected, *r)
	if errors.Is(err, metadata.ErrConflict) {
		return metadata.Version{}, false, nil
	}
	return v, err == nil, err
}

// commitSandboxIntent persists the route and exact node ownership in one transaction.
func (s *Stores) commitSandboxIntent(ctx context.Context, r *SandboxRecord, expected metadata.Version) (metadata.Version, bool, error) {
	if err := validateSandboxRecord(r); err != nil {
		return metadata.Version{}, false, err
	}
	ref := metadata.SandboxRef(*r)
	v, err := s.routes.CommitIntentAndOwnership(ctx, expected, *r, ref)
	if errors.Is(err, metadata.ErrConflict) {
		return metadata.Version{}, false, nil
	}
	return v, err == nil, err
}
func (s *Stores) DeleteSandbox(ctx context.Context, group, key string) error {
	_, v, found, err := s.GetSandbox(ctx, group, key)
	if err != nil || !found {
		return err
	}
	return s.routes.DeleteRoute(ctx, metadata.RouteKey{Group: group, RouteKey: key}, v)
}
func (s *Stores) commitGroupSandboxIntent(ctx context.Context, r *SandboxRecord, expected metadata.Version, selected metadata.GroupAdmission) (metadata.Version, bool, error) {
	if err := validateSandboxRecord(r); err != nil {
		return metadata.Version{}, false, err
	}
	v, err := s.routes.CommitGroupIntent(ctx, expected, *r, metadata.SandboxRef(*r), selected)
	if errors.Is(err, metadata.ErrConflict) {
		return metadata.Version{}, false, nil
	}
	return v, err == nil, err
}
func (s *Stores) DeleteSandboxIfRevision(ctx context.Context, group, key string, expected metadata.Version) (bool, error) {
	err := s.routes.DeleteRoute(ctx, metadata.RouteKey{Group: group, RouteKey: key}, expected)
	if errors.Is(err, metadata.ErrConflict) {
		return false, nil
	}
	return err == nil, err
}
func (s *Stores) RangeSandboxes(ctx context.Context, group string, fn func(*SandboxRecord) error) error {
	if group == "" {
		return fmt.Errorf("registry: group is required for route list")
	}
	for cursor := metadata.Cursor(""); ; {
		page, err := s.routes.ListRoutes(ctx, group, cursor, 256)
		if err != nil {
			return err
		}
		for _, v := range page.Items {
			rec := v.Value
			if err := validateSandboxRecord(&rec); err != nil {
				return err
			}
			stripProtectedSandboxFields(&rec)
			if err := fn(&rec); err != nil {
				return err
			}
		}
		if page.Next == "" {
			return nil
		}
		cursor = page.Next
	}
}
func (s *Stores) putBuild(ctx context.Context, b *BuildRecord) (metadata.Version, error) {
	if b == nil || b.Group == "" || b.BuildID == "" {
		return metadata.Version{}, nil
	}
	_, rev, _, err := s.getBuild(ctx, b.Group, b.BuildID)
	if err != nil {
		return metadata.Version{}, err
	}
	v, ok, err := s.casBuild(ctx, b, rev)
	if err == nil && !ok {
		err = metadata.ErrConflict
	}
	return v, err
}
func (s *Stores) casBuild(ctx context.Context, b *BuildRecord, expected metadata.Version) (metadata.Version, bool, error) {
	if b == nil || b.Group == "" || b.BuildID == "" {
		return metadata.Version{}, false, nil
	}
	v, err := s.builds.CompareAndPutBuild(ctx, metadata.BuildKey{Group: b.Group, BuildID: b.BuildID}, expected, metadata.BuildRecord(*b))
	if errors.Is(err, metadata.ErrConflict) {
		return metadata.Version{}, false, nil
	}
	return v, err == nil, err
}
func (s *Stores) getBuild(ctx context.Context, group, id string) (*BuildRecord, metadata.Version, bool, error) {
	v, err := s.builds.ReadBuild(ctx, metadata.BuildKey{Group: group, BuildID: id})
	if err != nil || !v.Exists {
		return nil, v.Version, v.Exists, err
	}
	b := BuildRecord(v.Value)
	return &b, v.Version, true, nil
}
func (s *Stores) deleteBuild(ctx context.Context, group, id string) error {
	_, v, found, err := s.getBuild(ctx, group, id)
	if err != nil || !found {
		return err
	}
	return s.builds.DeleteBuild(ctx, metadata.BuildKey{Group: group, BuildID: id}, v)
}
func (s *Stores) deleteBuildIfRevision(ctx context.Context, group, id string, expected metadata.Version) (bool, error) {
	err := s.builds.DeleteBuild(ctx, metadata.BuildKey{Group: group, BuildID: id}, expected)
	if errors.Is(err, metadata.ErrConflict) {
		return false, nil
	}
	return err == nil, err
}
func (s *Stores) RangeBuildsInGroup(ctx context.Context, group string, fn func(*BuildRecord) error) error {
	if group == "" {
		return fmt.Errorf("registry: group is required for build list")
	}
	for cursor := metadata.Cursor(""); ; {
		page, err := s.builds.ListBuilds(ctx, group, cursor, 256)
		if err != nil {
			return err
		}
		for _, v := range page.Items {
			b := BuildRecord(v.Value)
			if err := fn(&b); err != nil {
				return err
			}
		}
		if page.Next == "" {
			return nil
		}
		cursor = page.Next
	}
}

var (
	errNodeSandboxIDConflict = metadata.ErrOwnershipConflict
)

// Assert the reference DTO remains identical to the persisted domain value.
var _ = metadata.NodeSandboxRef(clusterstate.NodeSandboxRef{})

func stripProtectedSandboxFields(route *SandboxRecord) {
	if route == nil {
		return
	}
	route.APISecretFingerprint = ""
	route.ManifestKeyFingerprint = ""
	route.CreateCredentials = nil
	route.WorkspacesDigest = ""
	route.StableID = ""
	route.APISecret = ""
	route.ServiceSecret = ""
	route.EnvdAccessToken = ""
	route.TrafficAccessToken = ""
	route.ForwardAccessToken = ""
}

func validateSandboxRecord(r *SandboxRecord) error {
	if r == nil {
		return errors.New("registry: sandbox route is required")
	}
	if r.Group == "" || r.RouteKey == "" {
		return errors.New("registry: sandbox route group and route key are required")
	}
	if !types.Profile(r.Profile).Valid() {
		return errors.New("registry: sandbox route profile is invalid")
	}
	if !validNodeSandboxIdentity(r.SandboxID, r.NodeSandboxID, r.SandboxGeneration) {
		return errors.New("registry: sandbox route identity is invalid")
	}
	if r.NextSandboxGeneration <= r.SandboxGeneration {
		return errors.New("registry: sandbox route next generation is invalid")
	}
	return nil
}
