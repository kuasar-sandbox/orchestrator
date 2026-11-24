package metastore

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func admissionFixture(t *testing.T, s *testStore) (metadata.NodeSnapshot, nodegroup.Support, metadata.GroupAdmission, metadata.SandboxRecord) {
	t.Helper()
	node := registerNode(t, s, "node")
	group := supportFixture("/g", 1)
	template := types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String()
	group.DefaultContext = &nodegroup.DefaultContext{TemplateID: template}
	publishGroups(t, s, node.Registration, "initial", 1, group)
	selection, err := s.ReadGroupSupport(t.Context(), group.Group, "node")
	check(t, err)
	admission := metadata.GroupAdmission{Group: group.Group, GroupVersion: group.Version, AuthRef: group.Entries[0].AuthRef, Registration: node.Registration, SupportVersion: selection.Support.Version, PublicationVersion: selection.Publication.Version}
	value := routeFixture("admitted")
	value.GroupVersion, value.TemplateSource, value.CreateTemplateID = group.Version, "default", template
	value.TemplateID = template
	return node, group, admission, value
}

func TestGroupAdmissionConformance(t *testing.T) {
	for name, factory := range backendCases {
		t.Run(name, func(t *testing.T) {
			newStore := func(t *testing.T) *testStore { return newBackendStore(t, factory(t)) }
			t.Run("selected_support_registration_and_publication_are_atomic", func(t *testing.T) {
				for _, change := range []string{"support", "publication", "registration", "labels", "draining", "unrelated_group_and_heartbeat"} {
					t.Run(change, func(t *testing.T) {
						s := newStore(t)
						node, group, admission, value := admissionFixture(t, s)
						admission.Placement = placement.Request{NodeSelectors: []map[string]string{{"pool": "eligible"}}}
						node.Value.Labels = map[string]string{"pool": "eligible"}
						var profileErr error
						node.Version, profileErr = s.UpdateProfile(t.Context(), node.Registration, node.Version, node.Value)
						check(t, profileErr)
						switch change {
						case "labels", "draining":
							if change == "labels" {
								node.Value.Labels["pool"] = "ineligible"
							} else {
								node.Value.Draining = true
							}
							_, err := s.UpdateProfile(t.Context(), node.Registration, node.Version, node.Value)
							check(t, err)
						case "support":
							group.Version = 2
							check(t, s.ApplyGroupSupport(t.Context(), node.Registration, 2, &group, ""))
						case "publication":
							check(t, s.BeginGroupSupport(t.Context(), node.Registration, "reconnect", 1, 1))
						case "registration":
							_, err := s.Register(t.Context(), node.Version, node.Value)
							check(t, err)
						case "unrelated_group_and_heartbeat":
							h := supportFixture("unrelated", 2)
							check(t, s.ApplyGroupSupport(t.Context(), node.Registration, 2, &h, ""))
							node.Value.Counts++
							_, err := s.UpdateProfile(t.Context(), node.Registration, node.Version, node.Value)
							check(t, err)
						}
						_, err := s.CommitGroupIntent(t.Context(), metadata.Version{}, value, metadata.SandboxRef(value), admission)
						if change == "unrelated_group_and_heartbeat" {
							check(t, err)
							return
						}
						if !errors.Is(err, metadata.ErrConflict) {
							t.Fatalf("stale %s selection committed: %v", change, err)
						}
						owner, err := s.ReadSandboxOwner(t.Context(), value.NodeID, value.NodeSandboxID)
						check(t, err)
						if owner.Exists {
							t.Fatal("failed Group intent left ownership")
						}
					})
				}
			})
			t.Run("support_withdrawal_keeps_each_object_and_ambiguous_owner", func(t *testing.T) {
				s := newStore(t)
				node, group, admission, value := admissionFixture(t, s)
				ctx := t.Context()
				version, err := s.CommitGroupIntent(ctx, metadata.Version{}, value, metadata.SandboxRef(value), admission)
				check(t, err)
				build := buildFixture("admitted-build")
				build.GroupVersion = group.Version
				build.RegistrationImageRepo, build.RegistrationRegistryAuth = "", ""
				buildVersion, err := s.CommitGroupRegistrationIntent(ctx, build, metadata.BuildRef(build), admission)
				check(t, err)
				check(t, s.ApplyGroupSupport(ctx, node.Registration, 2, nil, group.Group))
				assertAuth := func(want bool) {
					t.Helper()
					auth, err := s.ReadGroupAuth(ctx, group.Group, admission.AuthRef)
					check(t, err)
					if auth.Exists != want {
						t.Fatalf("retained material exists=%v want=%v", auth.Exists, want)
					}
				}
				assertAuth(true)
				// A timeout may remove a route while keeping the ambiguous owner.
				_, err = s.RollbackIntent(ctx, metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey}, version, nil, false)
				check(t, err)
				assertAuth(true)
				check(t, s.DeleteBuild(ctx, metadata.BuildKey{Group: build.Group, BuildID: build.BuildID}, buildVersion))
				assertAuth(true)
				owner, err := s.ReadSandboxOwner(ctx, value.NodeID, value.NodeSandboxID)
				check(t, err)
				check(t, s.RemoveSandboxOwner(ctx, value.NodeID, value.NodeSandboxID, owner.Version))
				assertAuth(true)
				buildOwner, err := s.ReadBuildOwner(ctx, build.NodeID, build.BuildID)
				check(t, err)
				check(t, s.RemoveBuildOwner(ctx, build.NodeID, build.BuildID, buildOwner.Version))
				assertAuth(false)
			})
			t.Run("precise_rejection_does_not_erase_facts_or_older_responsibility", func(t *testing.T) {
				for _, changed := range []string{"none", "registration", "materialized", "new_intent"} {
					t.Run(changed, func(t *testing.T) {
						s := newStore(t)
						node, _, admission, value := admissionFixture(t, s)
						old := value
						_, err := s.CommitGroupIntent(t.Context(), metadata.Version{}, old, metadata.SandboxRef(old), admission)
						check(t, err)
						prior, err := s.ReadRoute(t.Context(), metadata.RouteKey{Group: old.Group, RouteKey: old.RouteKey})
						check(t, err)
						value.SandboxGeneration++
						value.NextSandboxGeneration = value.SandboxGeneration + 1
						value.NodeSandboxID = value.SandboxID + "-g" + strconv.FormatUint(value.SandboxGeneration, 10)
						version, err := s.CommitGroupIntent(t.Context(), prior.Version, value, metadata.SandboxRef(value), admission)
						check(t, err)
						switch changed {
						case "registration":
							_, err = s.Register(t.Context(), node.Version, node.Value)
						case "materialized":
							value.InstanceID = "materialized"
							_, err = s.CompareAndPutRoute(t.Context(), metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey}, version, value)
						case "new_intent":
							value.NextSandboxGeneration++
							_, err = s.CompareAndPutRoute(t.Context(), metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey}, version, value)
						}
						check(t, err)
						err = s.RetireRejectedGroupIntent(t.Context(), node.Registration, version, metadata.SandboxRef(value), &old)
						if changed == "none" {
							check(t, err)
						} else if !errors.Is(err, metadata.ErrConflict) {
							t.Fatalf("stale proof accepted: %v", err)
						}
						owner, err := s.ReadSandboxOwner(t.Context(), old.NodeID, old.NodeSandboxID)
						check(t, err)
						if !owner.Exists {
							t.Fatal("older ambiguous owner removed")
						}
						owner, err = s.ReadSandboxOwner(t.Context(), value.NodeID, value.NodeSandboxID)
						check(t, err)
						if owner.Exists != (changed != "none") {
							t.Fatal("wrong rejected owner retirement")
						}
					})
				}
			})
			t.Run("unknown_commit_keeps_bound_intent_and_auth", func(t *testing.T) {
				s := newStore(t)
				node, group, admission, value := admissionFixture(t, s)
				calls := 0
				s.afterNativeCommit = func(int) error { calls++; return &metadata.Unknown{Cause: errors.New("lost reply")} }
				_, err := s.CommitGroupIntent(t.Context(), metadata.Version{}, value, metadata.SandboxRef(value), admission)
				s.afterNativeCommit = nil
				if !errors.Is(err, metadata.ErrUnknown) || calls != 1 {
					t.Fatalf("unknown replayed: %d %v", calls, err)
				}
				stored, err := s.ReadRoute(t.Context(), metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey})
				check(t, err)
				if !stored.Exists || stored.Value.CreateTemplateID != value.TemplateID {
					t.Fatal("unknown lost committed template binding")
				}
				check(t, s.ApplyGroupSupport(t.Context(), node.Registration, 2, nil, group.Group))
				auth, err := s.ReadGroupAuth(t.Context(), group.Group, admission.AuthRef)
				check(t, err)
				if !auth.Exists {
					t.Fatal("unknown lost retained authentication")
				}
			})
			t.Run("Build_replay_invalidates_first_delivery_rejection", func(t *testing.T) {
				for _, changed := range []string{"none", "replay", "report", "registration"} {
					t.Run(changed, func(t *testing.T) {
						s := newStore(t)
						node, group, admission, _ := admissionFixture(t, s)
						build := buildFixture("bld-fenced")
						build.GroupVersion = group.Version
						build.RegistrationImageRepo, build.RegistrationRegistryAuth = "", ""
						ref := metadata.BuildRef(build)
						version, err := s.CommitGroupRegistrationIntent(t.Context(), build, ref, admission)
						check(t, err)
						switch changed {
						case "replay":
							err = s.EnsureRegistrationOwnership(t.Context(), metadata.BuildKey{Group: build.Group, BuildID: build.BuildID}, build.NodeID, build.TemplateID)
						case "report":
							err = s.ApplyBuildReport(t.Context(), node.Registration, ref, routesync.BuildEvent{BuildID: build.BuildID, TemplateID: build.TemplateID, State: string(metadata.BuildRegistered)})
						case "registration":
							_, err = s.Register(t.Context(), node.Version, node.Value)
						}
						check(t, err)
						err = s.RetireRejectedGroupBuild(t.Context(), node.Registration, version, ref)
						if changed == "none" {
							check(t, err)
						} else if !errors.Is(err, metadata.ErrConflict) {
							t.Fatalf("rejection erased a newer fact: %v", err)
						}
						current, err := s.ReadBuild(t.Context(), metadata.BuildKey{Group: build.Group, BuildID: build.BuildID})
						check(t, err)
						if current.Exists != (changed != "none") {
							t.Fatal("wrong Build retirement")
						}
					})
				}
			})
		})
	}
}
