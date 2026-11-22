package metastore

import (
	"context"
	"errors"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"reflect"
	"testing"
)

func TestExpiredNodeRecovery(t *testing.T) {
	for name, factory := range backendCases {
		t.Run(name, func(t *testing.T) {
			create := func(t *testing.T) *testStore { return newBackendStore(t, factory(t)) }
			t.Run("bounded_pages", func(t *testing.T) { runExpiredNodeProfilesBoundedPaging(t, create) })
			t.Run("original_claim", func(t *testing.T) { runExpiredNodeClaimConflicts(t, create) })
		})
	}
}
func runExpiredNodeProfilesBoundedPaging(t *testing.T, newTestStore func(*testing.T) *testStore) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, value := range []metadata.NodeProfileRecord{
		{NodeID: "a-fresh", LastHeartbeatUnix: 11},
		{NodeID: "b-expired", LastHeartbeatUnix: 9},
		{NodeID: "c-missing", LastHeartbeatUnix: 0},
		{NodeID: "d-reaping", LastHeartbeatUnix: 9},
		{NodeID: "e-equality", LastHeartbeatUnix: 10},
	} {
		value.LinkOwner = "registry"
		profile, err := s.Register(ctx, metadata.Version{}, value)
		check(t, err)
		if value.NodeID == "d-reaping" {
			_, err = s.BeginReap(ctx, value.NodeID, profile.Version)
			check(t, err)
		}
	}
	var ids []string
	pages, emptyWithNext := 0, 0
	for cursor := metadata.Cursor(""); ; {
		page, err := s.ListExpiredNodeProfiles(ctx, 10, cursor, 1)
		check(t, err)
		pages++
		if len(page.Items) > 1 {
			t.Fatal("expired listing exceeded the bounded page")
		}
		if len(page.Items) == 0 && page.Next != "" {
			emptyWithNext++
		}
		for _, profile := range page.Items {
			ids = append(ids, profile.Value.NodeID)
			// Removing the current page must not invalidate the continuation.
			claim, err := s.BeginReap(ctx, profile.Value.NodeID, profile.Version)
			check(t, err)
			removed, err := s.FinalizeReap(ctx, claim)
			check(t, err)
			if !removed {
				t.Fatal("empty expired node was not removed")
			}
		}
		if page.Next == "" {
			break
		}
		if page.Next == cursor {
			t.Fatal("filtered page did not advance")
		}
		cursor = page.Next
	}
	if pages != 5 || emptyWithNext != 3 || !reflect.DeepEqual(ids, []string{"b-expired", "e-equality"}) {
		t.Fatalf("pages=%d emptyWithNext=%d expired=%v", pages, emptyWithNext, ids)
	}
	if _, err := s.ListExpiredNodeProfiles(ctx, 10, "", 513); !errors.Is(err, metadata.ErrInvalidRecord) {
		t.Fatalf("unbounded profile listing accepted: %v", err)
	}
}

func runExpiredNodeClaimConflicts(t *testing.T, newTestStore func(*testing.T) *testStore) {
	for _, mode := range []string{"heartbeat", "reconnect"} {
		t.Run(mode, func(t *testing.T) {
			s := newTestStore(t)
			peer := companion(t, s)
			ctx := context.Background()
			old, err := s.Register(ctx, metadata.Version{}, metadata.NodeProfileRecord{NodeID: "node", LinkOwner: "registry", LastHeartbeatUnix: 1})
			check(t, err)
			page, err := s.ListExpiredNodeProfiles(ctx, 10, "", 128)
			check(t, err)
			if len(page.Items) != 1 {
				t.Fatal("expected one stale candidate")
			}
			updated := false
			s.beforeRetry = func() error {
				if updated {
					return nil
				}
				updated = true
				next := old.Value
				next.LastHeartbeatUnix = 11
				if mode == "heartbeat" {
					_, err := peer.UpdateProfile(ctx, old.Registration, old.Version, next)
					return err
				}
				_, err := peer.Register(ctx, old.Version, next)
				return err
			}
			_, err = s.BeginReap(ctx, "node", page.Items[0].Version)
			s.beforeRetry = nil
			if !updated || !errors.Is(err, metadata.ErrConflict) {
				t.Fatalf("concurrent %s did not fence claim: updated=%v err=%v", mode, updated, err)
			}
			got, err := s.ReadNodeProfile(ctx, "node")
			check(t, err)
			if !got.Exists || got.Reaping || got.Value.LastHeartbeatUnix != 11 || got.Version == old.Version {
				t.Fatalf("claim altered fresh profile: %+v", got)
			}
			if (got.Registration == old.Registration) != (mode == "heartbeat") {
				t.Fatalf("unexpected registration fence for %s: old=%+v got=%+v", mode, old.Registration, got.Registration)
			}
			claims, err := s.ListReaping(ctx, "", 128)
			check(t, err)
			if len(claims.Items) != 0 {
				t.Fatal("conflicted claim left a reaping marker")
			}
		})
	}
}
