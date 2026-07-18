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
	clock = now.Add(time.Second)
	if err := cache.Authorize(identity, PermitRouterCacheForward); !errors.Is(err, ErrPermitExpired) {
		t.Fatalf("cache expiry error = %v", err)
	}
}
