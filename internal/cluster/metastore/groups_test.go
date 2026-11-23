package metastore

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	records "github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

func supportFixture(group string, version int64) nodegroup.Support {
	secret := strings.Repeat("1", 64)
	return nodegroup.Support{Group: group, Version: version, Entries: []nodegroup.Entry{{AuthRef: testFingerprint(secret), AuthMaterial: nodegroup.AuthMaterial{APISecret: secret}, Routing: nodegroup.Routing{TargetPort: 49983}}}}
}

func finishGroupSnapshot(t *testing.T, s *testStore, reg metadata.Registration, id string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		complete, err := s.FinishGroupSupport(t.Context(), reg, id)
		check(t, err)
		if complete {
			return
		}
	}
	t.Fatal("bounded full snapshot cleanup did not finish")
}

func publishGroups(t *testing.T, s *testStore, reg metadata.Registration, id string, sequence int64, groups ...nodegroup.Support) {
	t.Helper()
	check(t, s.BeginGroupSupport(t.Context(), reg, id, sequence, len(groups)))
	for i, group := range groups {
		check(t, s.WriteGroupSupportBatch(t.Context(), reg, id, i, []nodegroup.Support{group}))
	}
	finishGroupSnapshot(t, s, reg, id)
}

func TestGroupSupportConformance(t *testing.T) {
	for name, factory := range backendCases {
		t.Run(name, func(t *testing.T) {
			newStore := func(t *testing.T) *testStore { return newBackendStore(t, factory(t)) }
			t.Run("auth_reference_index_avoids_shared_write_and_reclaims_safely", func(t *testing.T) {
				s := newStore(t)
				ctx := t.Context()
				secret := strings.Repeat("1", 64)
				first := metadata.GroupAuthReference{Group: "g", AuthRef: testFingerprint(secret), Kind: "sandbox", ID: "first"}
				next := first
				next.ID = "second"
				_, err := s.mutate(ctx, func(m *mutation) (bool, error) { return true, s.retainGroupAuth(m, first, secret) })
				check(t, err)
				before, err := s.ReadGroupAuth(ctx, "g", first.AuthRef)
				check(t, err)
				_, err = s.mutate(ctx, func(m *mutation) (bool, error) { return true, s.retainGroupAuth(m, next, secret) })
				check(t, err)
				after, err := s.ReadGroupAuth(ctx, "g", first.AuthRef)
				check(t, err)
				if !after.Exists || after.Version != before.Version {
					t.Fatal("new object reference wrote shared authentication material")
				}
				_, err = s.mutate(ctx, func(m *mutation) (bool, error) { return true, s.releaseGroupAuth(m, first) })
				check(t, err)
				other := companion(t, s)
				s.beforeRetry = func() error {
					_, err := other.mutate(ctx, func(m *mutation) (bool, error) { return true, other.releaseGroupAuth(m, next) })
					return err
				}
				_, err = s.mutate(ctx, func(m *mutation) (bool, error) { return true, s.retainGroupAuth(m, first, secret) })
				if !errors.Is(err, metadata.ErrConflict) {
					t.Fatalf("last-reference reclamation raced through stale material: %v", err)
				}
				_, err = s.mutate(ctx, func(m *mutation) (bool, error) { return true, s.retainGroupAuth(m, first, secret) })
				check(t, err)
				after, err = s.ReadGroupAuth(ctx, "g", first.AuthRef)
				check(t, err)
				if !after.Exists || after.Version == before.Version {
					t.Fatal("recreated auth lost delete/recreate fence")
				}
				check(t, s.read(ctx, func(tr *records.Tx) error {
					page, err := tr.Scan(s.groupAuthReferenceScope("g", first.AuthRef), nil, pageLimit(2))
					if err == nil && len(page.Items) != 1 {
						t.Fatalf("reference retry left %d rows", len(page.Items))
					}
					return err
				}))
			})
			t.Run("complete_boundary_ordered_deltas_and_stable_publication", func(t *testing.T) {
				s := newStore(t)
				ctx := t.Context()
				reg := registerNode(t, s, "node").Registration
				check(t, s.BeginGroupSupport(ctx, reg, "first", 2, 2))
				g, h := supportFixture("g", 1), supportFixture("h", 2)
				check(t, s.WriteGroupSupportBatch(ctx, reg, "first", 0, []nodegroup.Support{g}))
				candidate, err := s.ReadGroupSupport(ctx, "g", "node")
				check(t, err)
				if candidate.Support.Exists {
					t.Fatal("partial snapshot published")
				}
				if _, err := s.FinishGroupSupport(ctx, reg, "first"); !errors.Is(err, metadata.ErrGroupResync) {
					t.Fatalf("missing batch accepted: %v", err)
				}
				check(t, s.WriteGroupSupportBatch(ctx, reg, "first", 0, []nodegroup.Support{g}))
				check(t, s.WriteGroupSupportBatch(ctx, reg, "first", 1, []nodegroup.Support{h}))
				finishGroupSnapshot(t, s, reg, "first")
				before, err := s.ReadGroupSupport(ctx, "h", "node")
				check(t, err)
				if !before.Support.Exists || before.Support.Value.DefaultContext != nil {
					t.Fatal("defaultless support missing")
				}
				g.Version = 3
				g.Entries[0].Routing.TargetPort = 80
				check(t, s.ApplyGroupSupport(ctx, reg, 3, &g, ""))
				after, err := s.ReadGroupSupport(ctx, "h", "node")
				check(t, err)
				if after.Publication.Version != before.Publication.Version || after.Support.Version != before.Support.Version {
					t.Fatal("unrelated delta invalidated stable candidate fence")
				}
				g.Version = 5
				if err := s.ApplyGroupSupport(ctx, reg, 5, &g, ""); !errors.Is(err, metadata.ErrGroupResync) {
					t.Fatalf("sequence gap accepted: %v", err)
				}
				check(t, s.ApplyGroupSupport(ctx, reg, 4, nil, "g"))
				g.Version = 3
				check(t, s.ApplyGroupSupport(ctx, reg, 3, &g, ""))
				candidate, err = s.ReadGroupSupport(ctx, "g", "node")
				check(t, err)
				if candidate.Support.Exists {
					t.Fatal("late upsert resurrected deleted support")
				}
			})
			t.Run("old_registration_cannot_report_or_remove_retained_object_auth", func(t *testing.T) {
				s := newStore(t)
				ctx := t.Context()
				first := registerNode(t, s, "node")
				g := supportFixture("g", 1)
				publishGroups(t, s, first.Registration, "first", 1, g)
				object := metadata.GroupAuthReference{Group: "g", AuthRef: g.Entries[0].AuthRef, Kind: "sandbox", ID: "logical-instance"}
				_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
					return true, s.retainGroupAuth(m, object, g.Entries[0].AuthMaterial.APISecret)
				})
				check(t, err)
				claim, err := s.BeginReap(ctx, "node", first.Version)
				check(t, err)
				if done, err := s.FinalizeReap(ctx, claim); err != nil || done {
					t.Fatalf("reaped with support references: %v %v", done, err)
				}
				reaping, err := s.ReadNodeProfile(ctx, "node")
				check(t, err)
				next, err := s.Register(ctx, reaping.Version, first.Value)
				check(t, err)
				candidate, err := s.ReadGroupSupport(ctx, "g", "node")
				check(t, err)
				if candidate.Support.Exists {
					t.Fatal("old registration supplied new registration's candidate")
				}
				if _, err := s.CleanupGroupSupport(ctx, claim); !errors.Is(err, metadata.ErrConflict) {
					t.Fatalf("old reaper accepted: %v", err)
				}
				g.Version = 2
				if err := s.ApplyGroupSupport(ctx, first.Registration, 2, &g, ""); !errors.Is(err, metadata.ErrConflict) {
					t.Fatalf("old stream accepted: %v", err)
				}
				publishGroups(t, s, next.Registration, "empty", 0)
				auth, err := s.ReadGroupAuth(ctx, "g", object.AuthRef)
				check(t, err)
				if !auth.Exists {
					t.Fatalf("support removal lost object authentication: %+v", auth)
				}
				_, err = s.mutate(ctx, func(m *mutation) (bool, error) { return true, s.releaseGroupAuth(m, object) })
				check(t, err)
				auth, err = s.ReadGroupAuth(ctx, "g", object.AuthRef)
				check(t, err)
				if auth.Exists {
					t.Fatal("last reference did not release authentication material")
				}
			})
			t.Run("retry_retains_original_registration_and_unknown_is_not_replayed", func(t *testing.T) {
				s := newStore(t)
				ctx := t.Context()
				first := registerNode(t, s, "node")
				g := supportFixture("g", 1)
				publishGroups(t, s, first.Registration, "first", 1, g)
				other := companion(t, s)
				s.beforeRetry = func() error { _, err := other.Register(ctx, first.Version, first.Value); return err }
				g.Version = 2
				if err := s.ApplyGroupSupport(ctx, first.Registration, 2, &g, ""); !errors.Is(err, metadata.ErrConflict) {
					t.Fatalf("retry borrowed newer registration: %v", err)
				}
				current, err := s.ReadNodeProfile(ctx, "node")
				check(t, err)
				check(t, s.BeginGroupSupport(ctx, current.Registration, "second", 2, 1))
				calls := 0
				s.afterNativeCommit = func(int) error { calls++; return storage.ErrUnknown }
				err = s.WriteGroupSupportBatch(ctx, current.Registration, "second", 0, []nodegroup.Support{g})
				if !errors.Is(err, storage.ErrUnknown) || calls != 1 {
					t.Fatalf("unknown automatically replayed: %v calls=%d", err, calls)
				}
				s.afterNativeCommit = nil
				// A caller retry is constrained by the same registration, sync and
				// offset; it cannot count the committed batch twice.
				check(t, s.WriteGroupSupportBatch(ctx, current.Registration, "second", 0, []nodegroup.Support{g}))
				finishGroupSnapshot(t, s, current.Registration, "second")
				auth, err := s.ReadGroupAuth(ctx, "g", g.Entries[0].AuthRef)
				check(t, err)
				if !auth.Exists {
					t.Fatalf("replayed batch lost authentication: %+v", auth)
				}
			})
			t.Run("bounded_cleanup_and_validation", func(t *testing.T) {
				s := newStore(t)
				ctx := t.Context()
				reg := registerNode(t, s, "node").Registration
				g, h := supportFixture("g", 1), supportFixture("h", 2)
				g.Entries = nil
				h.Entries = nil
				for i := 1; i <= 80; i++ {
					secret := fmt.Sprintf("%064x", i)
					e := nodegroup.Entry{AuthRef: testFingerprint(secret), AuthMaterial: nodegroup.AuthMaterial{APISecret: secret}}
					g.Entries = append(g.Entries, e)
					h.Entries = append(h.Entries, e)
				}
				publishGroups(t, s, reg, "large", 2, g, h)
				check(t, s.BeginGroupSupport(ctx, reg, "clear", 2, 0))
				done, err := s.FinishGroupSupport(ctx, reg, "clear")
				check(t, err)
				if done {
					t.Fatal("cleanup exceeded per-transaction auth work budget")
				}
				finishGroupSnapshot(t, s, reg, "clear")
				for _, group := range []string{"g", "h"} {
					a, err := s.ReadGroupAuth(ctx, group, g.Entries[0].AuthRef)
					check(t, err)
					if a.Exists {
						t.Fatal("cleanup retained removed support auth")
					}
				}
				bad := supportFixture("bad", 3)
				bad.DefaultContext = &nodegroup.DefaultContext{}
				if err := s.ApplyGroupSupport(ctx, reg, 3, &bad, ""); err == nil {
					t.Fatal("empty default object accepted")
				}
				bad.DefaultContext = nil
				bad.Entries[0].AuthMaterial.APISecret = strings.Repeat("2", 64)
				if err := s.ApplyGroupSupport(ctx, reg, 3, &bad, ""); err == nil {
					t.Fatal("self-asserted auth fingerprint accepted")
				}
			})
		})
	}
}
