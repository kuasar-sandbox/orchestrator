package metastore

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type buildMergeFixture struct {
	node   metadata.NodeSnapshot
	build  metadata.BuildRecord
	target types.BuildTarget
	event  routesync.BuildEvent
}

func newBuildMergeFixture(t *testing.T, s *testStore) buildMergeFixture {
	t.Helper()
	node, group, admission, _ := admissionFixture(t, s)
	build := buildFixture("merge")
	build.GroupVersion = group.Version
	build.RegistrationImageRepo, build.RegistrationRegistryAuth = "", ""
	_, err := s.CommitGroupRegistrationIntent(t.Context(), build, metadata.BuildRef(build), admission)
	check(t, err)
	return buildMergeFixture{
		node: node, build: build,
		target: types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true},
		event: routesync.BuildEvent{
			BuildID: build.BuildID, TemplateID: build.TemplateID,
			State: string(metadata.BuildRegistered), Reason: "registered report",
		},
	}
}

func (f buildMergeFixture) key() metadata.BuildKey {
	return metadata.BuildKey{Group: f.build.Group, BuildID: f.build.BuildID}
}

func (f buildMergeFixture) apply(ctx context.Context, s *testStore, operation string) error {
	switch operation {
	case "accept":
		_, err := s.AcceptGroupBuildRegistration(ctx, f.node.Registration, f.key(), f.build.TemplateID, &f.target)
		return err
	case "report":
		return s.ApplyBuildReport(ctx, f.node.Registration, metadata.BuildRef(f.build), f.event)
	default:
		panic("unknown Build merge test operation")
	}
}

type buildMergeSnapshot struct {
	build   metadata.Versioned[metadata.BuildRecord]
	owner   metadata.Versioned[metadata.NodeBuildRef]
	node    metadata.NodeSnapshot
	support metadata.GroupSupportSelection
}

func readBuildMergeSnapshot(t *testing.T, s *testStore, f buildMergeFixture) buildMergeSnapshot {
	t.Helper()
	build, err := s.ReadBuild(t.Context(), f.key())
	check(t, err)
	owner, err := s.ReadBuildOwner(t.Context(), f.build.NodeID, f.build.BuildID)
	check(t, err)
	node, err := s.ReadNodeProfile(t.Context(), f.build.NodeID)
	check(t, err)
	support, err := s.ReadGroupSupport(t.Context(), f.build.Group, f.build.NodeID)
	check(t, err)
	return buildMergeSnapshot{build: build, owner: owner, node: node, support: support}
}

func assertMergedBuild(t *testing.T, f buildMergeFixture, before, after buildMergeSnapshot) {
	t.Helper()
	want := f.build
	want.State, want.Reason = metadata.BuildRegistered, f.event.Reason
	want.RegistrationTargetSet, want.RegistrationTarget = true, &f.target
	if !after.build.Exists || !after.build.Version.IsCommitted() || !reflect.DeepEqual(after.build.Value, want) {
		t.Fatal("merge lost the accepted target or report, or changed the Build identity")
	}
	if !after.owner.Exists || after.owner.Value != metadata.BuildRef(f.build) || !reflect.DeepEqual(after.owner, before.owner) {
		t.Fatal("merge changed Build ownership or its revision")
	}
	if !after.node.Exists || after.node.Registration != f.node.Registration || !reflect.DeepEqual(after.node, before.node) {
		t.Fatal("merge changed the Node profile or registration fence")
	}
	if !after.support.Support.Exists || !after.support.Publication.Exists || !after.support.Publication.Value.Published || !reflect.DeepEqual(after.support, before.support) {
		t.Fatal("merge changed Group support or publication")
	}
}

func TestBuildMergeConformance(t *testing.T) {
	for name, factory := range backendCases {
		t.Run(name, func(t *testing.T) {
			newStore := func(t *testing.T) *testStore { return newBackendStore(t, factory(t)) }
			for _, first := range []string{"report", "accept"} {
				second := "accept"
				if first == "accept" {
					second = "report"
				}
				t.Run(first+"_commits_before_"+second, func(t *testing.T) {
					s := newStore(t)
					f := newBuildMergeFixture(t, s)
					before := readBuildMergeSnapshot(t, s, f)
					other := companion(t, s)
					interventions, commits := 0, 0
					var firstVersion metadata.Version
					// The existing hook definitely aborts the first physical transaction,
					// commits the companion operation, then retries with the original pins.
					// This exercises logical conflicts, not an observed PostgreSQL 40001.
					s.beforeRetry = func() error {
						interventions++
						if err := f.apply(t.Context(), other, first); err != nil {
							return err
						}
						current, err := other.ReadBuild(t.Context(), f.key())
						firstVersion = current.Version
						return err
					}
					s.afterNativeCommit = func(int) error { commits++; return nil }
					err := f.apply(t.Context(), s, second)
					s.afterNativeCommit = nil
					check(t, err)
					if interventions != 1 || commits != 1 {
						t.Fatalf("interventions=%d commits=%d, want one of each", interventions, commits)
					}
					after := readBuildMergeSnapshot(t, s, f)
					if firstVersion.Compare(before.build.Version) <= 0 || after.build.Version.Compare(firstVersion) <= 0 {
						t.Fatal("both merge operations did not commit in the forced order")
					}
					assertMergedBuild(t, f, before, after)
				})
			}
			for _, operation := range []string{"accept", "report"} {
				t.Run(operation+"_unknown_commit_is_not_replayed", func(t *testing.T) {
					s := newStore(t)
					f := newBuildMergeFixture(t, s)
					before := readBuildMergeSnapshot(t, s, f)
					first := "report"
					if operation == "report" {
						first = "accept"
					}
					check(t, f.apply(t.Context(), s, first))
					intermediate := readBuildMergeSnapshot(t, s, f)
					unknown := &metadata.Unknown{Cause: &metadata.Conflict{
						Expected: before.build.Version, Actual: intermediate.build.Version,
					}}
					commits := 0
					s.afterNativeCommit = func(int) error { commits++; return unknown }
					err := f.apply(t.Context(), s, operation)
					s.afterNativeCommit = nil
					if err != unknown || !errors.Is(err, metadata.ErrUnknown) || commits != 1 {
						t.Fatalf("unknown commit replayed or relabeled: commits=%d err=%v", commits, err)
					}
					after := readBuildMergeSnapshot(t, s, f)
					if after.build.Version.Compare(intermediate.build.Version) <= 0 {
						t.Fatal("unknown result did not retain the native commit")
					}
					assertMergedBuild(t, f, before, after)
				})
			}
			for _, operation := range []string{"accept", "report"} {
				t.Run(operation+"_retry_retains_original_registration", func(t *testing.T) {
					s := newStore(t)
					f := newBuildMergeFixture(t, s)
					other := companion(t, s)
					interventions, commits := 0, 0
					var committed buildMergeSnapshot
					s.beforeRetry = func() error {
						interventions++
						if _, err := other.Register(t.Context(), f.node.Version, f.node.Value); err != nil {
							return err
						}
						committed = readBuildMergeSnapshot(t, other, f)
						return nil
					}
					s.afterNativeCommit = func(int) error { commits++; return nil }
					err := f.apply(t.Context(), s, operation)
					s.afterNativeCommit = nil
					if !errors.Is(err, metadata.ErrConflict) || interventions != 1 || commits != 0 {
						t.Fatalf("stale registration committed: interventions=%d commits=%d err=%v", interventions, commits, err)
					}
					if committed.node.Registration == f.node.Registration {
						t.Fatal("companion did not replace the registration fence")
					}
					if after := readBuildMergeSnapshot(t, s, f); !reflect.DeepEqual(after, committed) {
						t.Fatal("stale merge changed the Build, ownership, or newer registration/Group facts")
					}
				})
			}
			t.Run("accept_retry_rejects_changed_target", func(t *testing.T) {
				s := newStore(t)
				f := newBuildMergeFixture(t, s)
				other := companion(t, s)
				interventions, commits := 0, 0
				var committed buildMergeSnapshot
				s.beforeRetry = func() error {
					interventions++
					changed := f
					changed.target.Memory = false
					if err := changed.apply(t.Context(), other, "accept"); err != nil {
						return err
					}
					committed = readBuildMergeSnapshot(t, other, f)
					return nil
				}
				s.afterNativeCommit = func(int) error { commits++; return nil }
				err := f.apply(t.Context(), s, "accept")
				s.afterNativeCommit = nil
				if !errors.Is(err, metadata.ErrConflict) || interventions != 1 || commits != 0 {
					t.Fatalf("target mismatch committed: interventions=%d commits=%d err=%v", interventions, commits, err)
				}
				if !committed.build.Value.RegistrationTargetSet || metadata.SameBuildTarget(committed.build.Value.RegistrationTarget, &f.target) {
					t.Fatal("companion did not commit a different target")
				}
				if after := readBuildMergeSnapshot(t, s, f); !reflect.DeepEqual(after, committed) {
					t.Fatal("conflicting ACK changed the accepted target or ownership/registration/Group facts")
				}
			})
		})
	}
}
