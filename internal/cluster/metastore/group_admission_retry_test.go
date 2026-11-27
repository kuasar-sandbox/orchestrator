package metastore

import (
	"errors"
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

// The backend hook aborts the first physical transaction before changing state.
// This distinguishes retrying an aborted transaction from replaying business
// work after an unknown outcome.
func TestGroupAdmissionRetriesCurrentEligibility(t *testing.T) {
	for name, factory := range backendCases {
		t.Run(name, func(t *testing.T) {
			for _, operation := range []string{"build", "sandbox"} {
				t.Run(operation, func(t *testing.T) {
					for _, change := range []string{"heartbeat", "counts", "labels", "draining", "zone", "runtime", "headroom", "support", "publication", "registration", "reaping", "competing_object"} {
						t.Run(change, func(t *testing.T) {
							s := newBackendStore(t, factory(t))
							node, group, admission, sandbox := admissionFixture(t, s)
							other := companion(t, s)
							admission.Placement = placement.Request{NodeSelectors: []map[string]string{{"pool": "eligible"}}}
							admission.TargetRuntimeDigest = "selected-runtime"
							admission.BuildResources = &routesync.BuildResources{CPU: 1000, Memory: 1024}
							node.Value.Labels = map[string]string{"pool": "eligible"}
							node.Value.RuntimeDigest = admission.TargetRuntimeDigest
							node.Value.BuildRegistrationCapacity = &routesync.BuildAdmissionLimit{MaxBuilds: 1}
							var err error
							node.Version, err = s.UpdateProfile(t.Context(), node.Registration, node.Version, node.Value)
							check(t, err)
							build := buildFixture("retry-admission")
							build.GroupVersion = group.Version
							build.RegistrationImageRepo, build.RegistrationRegistryAuth = "", ""
							competingBuild, competingSandbox := build, sandbox
							competingBuild.TemplateID += "-competing"
							competingSandbox.InstanceID = "competing"
							interventions := 0
							s.beforeRetry = func() error {
								interventions++
								switch change {
								case "support":
									group.Version++
									return other.ApplyGroupSupport(t.Context(), node.Registration, 2, &group, "")
								case "publication":
									return other.BeginGroupSupport(t.Context(), node.Registration, "reconnect", 1, 1)
								case "registration":
									_, err := other.Register(t.Context(), node.Version, node.Value)
									return err
								case "reaping":
									_, err := other.BeginReap(t.Context(), node.Value.NodeID, node.Version)
									return err
								case "competing_object":
									if operation == "build" {
										_, err := other.CommitGroupRegistrationIntent(t.Context(), competingBuild, metadata.BuildRef(competingBuild), admission)
										return err
									}
									_, err := other.CommitGroupIntent(t.Context(), metadata.Version{}, competingSandbox, metadata.SandboxRef(competingSandbox), admission)
									return err
								case "heartbeat":
									node.Value.LastHeartbeatUnix++
								case "counts":
									node.Value.Counts++
								case "labels":
									node.Value.Labels = map[string]string{"pool": "ineligible"}
								case "draining":
									node.Value.Draining = true
								case "zone":
									node.Value.Zone = "red"
								case "runtime":
									node.Value.RuntimeDigest = "different-runtime"
								case "headroom":
									node.Value.BuildRegistrationUsage = &routesync.BuildAdmissionUsage{Builds: 1}
								}
								_, err := other.UpdateProfile(t.Context(), node.Registration, node.Version, node.Value)
								return err
							}
							var version metadata.Version
							if operation == "build" {
								version, err = s.CommitGroupRegistrationIntent(t.Context(), build, metadata.BuildRef(build), admission)
							} else {
								version, err = s.CommitGroupIntent(t.Context(), metadata.Version{}, sandbox, metadata.SandboxRef(sandbox), admission)
							}
							if interventions != 1 {
								t.Fatalf("physical abort intervention count = %d, error = %v", interventions, err)
							}
							allowed := change == "heartbeat" || change == "counts"
							if allowed {
								check(t, err)
								if !version.IsCommitted() {
									t.Fatal("eligible retry returned no committed version")
								}
							} else if !errors.Is(err, metadata.ErrConflict) {
								t.Fatalf("changed %s admitted: %v", change, err)
							}
							wantExists := allowed || change == "competing_object"
							if operation == "build" {
								row, err := s.ReadBuild(t.Context(), metadata.BuildKey{Group: build.Group, BuildID: build.BuildID})
								check(t, err)
								owner, err := s.ReadBuildOwner(t.Context(), build.NodeID, build.BuildID)
								check(t, err)
								want := build
								if change == "competing_object" {
									want = competingBuild
								}
								if row.Exists != wantExists || owner.Exists != wantExists || (wantExists && (!reflect.DeepEqual(row.Value, want) || owner.Value != metadata.BuildRef(want))) {
									t.Fatalf("retry changed Build identity or partial ownership: row=%+v owner=%+v", row, owner)
								}
							} else {
								row, err := s.ReadRoute(t.Context(), metadata.RouteKey{Group: sandbox.Group, RouteKey: sandbox.RouteKey})
								check(t, err)
								owner, err := s.ReadSandboxOwner(t.Context(), sandbox.NodeID, sandbox.NodeSandboxID)
								check(t, err)
								want := sandbox
								if change == "competing_object" {
									want = competingSandbox
								}
								if row.Exists != wantExists || owner.Exists != wantExists || (wantExists && (!reflect.DeepEqual(row.Value, want) || owner.Value != metadata.SandboxRef(want))) {
									t.Fatalf("retry changed sandbox identity or partial ownership: row=%+v owner=%+v", row, owner)
								}
							}
						})
					}
				})
			}
			t.Run("unknown_Build_commit_is_not_replayed", func(t *testing.T) {
				s := newBackendStore(t, factory(t))
				_, group, admission, _ := admissionFixture(t, s)
				build := buildFixture("unknown-admission")
				build.GroupVersion = group.Version
				build.RegistrationImageRepo, build.RegistrationRegistryAuth = "", ""
				commits := 0
				s.afterNativeCommit = func(int) error { commits++; return storage.ErrUnknown }
				_, err := s.CommitGroupRegistrationIntent(t.Context(), build, metadata.BuildRef(build), admission)
				s.afterNativeCommit = nil
				if !errors.Is(err, storage.ErrUnknown) || commits != 1 {
					t.Fatalf("unknown intent commit was replayed: commits=%d error=%v", commits, err)
				}
				row, err := s.ReadBuild(t.Context(), metadata.BuildKey{Group: build.Group, BuildID: build.BuildID})
				check(t, err)
				owner, err := s.ReadBuildOwner(t.Context(), build.NodeID, build.BuildID)
				check(t, err)
				if !row.Exists || !owner.Exists || !reflect.DeepEqual(row.Value, build) || owner.Value != metadata.BuildRef(build) {
					t.Fatal("unknown outcome lost the committed intent identity or ownership")
				}
			})
		})
	}
}
