package raftstore

import (
	"errors"
	"testing"
	"time"
)

func TestServePermitUsesMonotonicBoundAndOperationGates(t *testing.T) {
	identity := PermitIdentity{
		ClusterID: "cluster-1", StorageGeneration: "generation-1", SystemEpoch: 7,
		ManifestDigest: digestFor("manifest"),
	}
	grant := PermitGrant{
		PermitIdentity: identity, CommitIndex: 42, MaxLifetimeMillis: 5000,
		ServeGate: true, WriteGate: false, CutoverGate: true, RecoveryClosed: true,
	}
	started := time.Now()
	permit, err := NewServePermit(grant, started)
	if err != nil {
		t.Fatal(err)
	}
	if err := permit.Authorize(started.Add(time.Second), identity, PermitRegistryRead); err != nil {
		t.Fatal(err)
	}
	if err := permit.Authorize(started.Add(time.Second), identity, PermitRegistryWrite); !errors.Is(err, ErrPermitDenied) {
		t.Fatalf("closed write gate error = %v", err)
	}
	if err := permit.Authorize(started.Add(5*time.Second), identity, PermitRegistryRead); !errors.Is(err, ErrPermitExpired) {
		t.Fatalf("expired permit error = %v", err)
	}
	other := identity
	other.SystemEpoch++
	if err := permit.Authorize(started.Add(time.Second), other, PermitRegistryRead); !errors.Is(err, ErrPermitMismatch) {
		t.Fatalf("identity mismatch error = %v", err)
	}
}

func TestPermitCacheRejectsOlderRefresh(t *testing.T) {
	now := time.Now()
	clock := now
	cache := NewPermitCache(func() time.Time { return clock })
	identity := PermitIdentity{
		ClusterID: "cluster-1", StorageGeneration: "generation-1", SystemEpoch: 1,
		ManifestDigest: digestFor("manifest"),
	}
	grant := PermitGrant{
		PermitIdentity: identity, CommitIndex: 10, MaxLifetimeMillis: 1000,
		ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}
	if err := cache.Install(grant, now); err != nil {
		t.Fatal(err)
	}
	older := grant
	older.CommitIndex = 9
	if err := cache.Install(older, now); err == nil {
		t.Fatal("older permit replaced a newer permit")
	}
	if err := cache.Install(grant, now.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	equivocation := grant
	equivocation.WriteGate = false
	if err := cache.Install(equivocation, now); err == nil {
		t.Fatal("same-index permit equivocation was accepted")
	}
	clock = now.Add(time.Second)
	if err := cache.Authorize(identity, PermitRouterCacheForward); !errors.Is(err, ErrPermitExpired) {
		t.Fatalf("cache expiry error = %v", err)
	}
}

func TestPermitCacheKeepsPreviousAndActiveManifestPermits(t *testing.T) {
	now := time.Now()
	clock := now
	cache := NewPermitCache(func() time.Time { return clock })
	previous := PermitIdentity{
		ClusterID: "cluster-1", StorageGeneration: "generation-1", SystemEpoch: 1,
		ManifestDigest: digestFor("manifest-1"),
	}
	active := previous
	active.SystemEpoch = 2
	active.ManifestDigest = digestFor("manifest-2")
	for index, identity := range []PermitIdentity{previous, active} {
		if err := cache.Install(PermitGrant{
			PermitIdentity: identity, CommitIndex: uint64(index + 10), MaxLifetimeMillis: 1000,
			ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
		}, now); err != nil {
			t.Fatal(err)
		}
	}
	for _, identity := range []PermitIdentity{previous, active} {
		if err := cache.Authorize(identity, PermitRouterCacheForward); err != nil {
			t.Fatalf("identity %+v was not authorized: %v", identity, err)
		}
	}

	third := active
	third.SystemEpoch++
	third.ManifestDigest = digestFor("manifest-3")
	if err := cache.Install(PermitGrant{
		PermitIdentity: third, CommitIndex: 12, MaxLifetimeMillis: 1000,
		ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}, now); !errors.Is(err, ErrPermitCapacity) {
		t.Fatalf("third live identity error = %v", err)
	}
}

func TestPermitCacheRetiresExpiredIdentityWithoutReplayExtension(t *testing.T) {
	now := time.Now()
	clock := now
	cache := NewPermitCache(func() time.Time { return clock })
	grantFor := func(epoch, index uint64) PermitGrant {
		return PermitGrant{
			PermitIdentity: PermitIdentity{
				ClusterID: "cluster-1", StorageGeneration: "generation-1", SystemEpoch: epoch,
				ManifestDigest: digestFor(string(rune('0' + epoch))),
			},
			CommitIndex: index, MaxLifetimeMillis: 1000,
			ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
		}
	}
	first, second, third := grantFor(1, 10), grantFor(2, 11), grantFor(3, 12)
	if err := cache.Install(first, now); err != nil {
		t.Fatal(err)
	}
	if err := cache.Install(second, now); err != nil {
		t.Fatal(err)
	}
	clock = now.Add(time.Second)
	if err := cache.Install(third, clock); err != nil {
		t.Fatal(err)
	}
	if err := cache.Install(first, clock); err == nil {
		t.Fatal("retired permit replay was reinstalled with a fresh expiry")
	}
	if err := cache.Authorize(first.PermitIdentity, PermitRegistryRead); !errors.Is(err, ErrPermitMismatch) {
		t.Fatalf("retired identity authorization error = %v", err)
	}
}
