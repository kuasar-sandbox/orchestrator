package store

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
)

func groupToken(t *testing.T, pair KeyPair) string {
	t.Helper()
	raw, err := hex.DecodeString(pair.APISecret)
	if err != nil {
		t.Fatal(err)
	}
	token, err := apikey.Mint(raw)
	if err != nil {
		t.Fatal(err)
	}
	return token
}
func groupNotFoundVersion(t *testing.T, err error, want int64) {
	t.Helper()
	var v *nodegroup.VersionError
	if !errors.Is(err, nodegroup.ErrNotFound) || !errors.As(err, &v) || v.Version != want {
		t.Fatalf("want authenticated not-found revision %d, got %v", want, err)
	}
}

func TestGroupOwnershipOutlivesAllowlistButAdmissionDoesNot(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	pair := testKeyPair("1", "2")
	token := groupToken(t, pair)
	if _, err := s.AddKeyPair(ctx, pair, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	_, err := s.GroupGet(ctx, token, "/cell/group")
	groupNotFoundVersion(t, err, 0)
	result, created, err := s.GroupPut(ctx, token, "/cell/group", 0, nodegroup.Context{}, nil)
	if err != nil || !created || result.Version <= 0 {
		t.Fatalf("create: %+v %v %v", result, created, err)
	}
	if _, err := s.RemoveKeyPair(ctx, pair); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GroupGet(ctx, token, "/cell/group"); err != nil {
		t.Fatalf("revocation erased management: %v", err)
	}
	if _, _, err := s.GroupAdmit(ctx, token, "/cell/group", result.Version); !errors.Is(err, nodegroup.ErrForbidden) {
		t.Fatalf("revoked create was admitted: %v", err)
	}
	page, err := s.GroupList(ctx, token, "", 1)
	if err != nil || len(page.Groups) != 1 {
		t.Fatalf("revoked owner list: %+v %v", page, err)
	}
	updated, created, err := s.GroupPut(ctx, token, "/cell/group", result.Version, nodegroup.Context{TargetPort: 77}, nil)
	if err != nil || created || updated.Version <= result.Version {
		t.Fatalf("owner update: %+v %v %v", updated, created, err)
	}
	changedPair := testKeyPair("1", "3")
	_, _, err = s.GroupPut(ctx, token, "/cell/group", updated.Version, nodegroup.Context{}, &nodegroup.KeyPut{APISecret: changedPair.APISecret, ManifestKey: changedPair.ManifestKey})
	if !errors.Is(err, ErrKeyPairConflict) {
		t.Fatalf("fixed binding changed after allowlist GC: %v", err)
	}
	if ok, err := s.HasKeyPair(ctx, changedPair); err != nil || ok {
		t.Fatalf("rejected binding installed: %v %v", ok, err)
	}
	other := testKeyPair("4", "5")
	if _, err := s.AddKeyPair(ctx, other, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	_, err = s.GroupGet(ctx, groupToken(t, other), "/cell/group")
	var leaked *nodegroup.VersionError
	if !errors.Is(err, nodegroup.ErrNotFound) || errors.As(err, &leaked) {
		t.Fatalf("foreign owner learned revision: %v", err)
	}
}

func TestGroupCombinedTransactionRollbackAndLease(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	pair := testKeyPair("1", "2")
	token := groupToken(t, pair)
	expires := time.Now().Unix() + 3600
	put := &nodegroup.KeyPut{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey, ExpiresUnix: expires}
	if _, err := s.db.Exec(`CREATE TRIGGER fail_group_insert BEFORE INSERT ON node_groups BEGIN SELECT RAISE(ABORT,'injected group failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.GroupPut(ctx, token, "g", 0, nodegroup.Context{}, put); err == nil {
		t.Fatal("injected failure was ignored")
	}
	if allowed, err := s.HasKeyPair(ctx, pair); err != nil || allowed {
		t.Fatalf("pair escaped failed transaction: %v %v", allowed, err)
	}
	seq, supports, err := s.GroupSnapshot(ctx)
	if err != nil || seq != 0 || len(supports) != 0 {
		t.Fatalf("rollback changed report boundary: %d %v %v", seq, supports, err)
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_group_insert`); err != nil {
		t.Fatal(err)
	}
	first, created, err := s.GroupPut(ctx, token, "g", 0, nodegroup.Context{}, put)
	if err != nil || !created {
		t.Fatalf("bootstrap: %+v %v", first, err)
	}
	lease := func() int64 {
		t.Helper()
		var expiry int64
		if err := s.db.QueryRow(`SELECT expires_unix FROM manifest_keys`).Scan(&expiry); err != nil {
			t.Fatal(err)
		}
		return expiry
	}
	put.ExpiresUnix = expires + 3600
	if _, _, err := s.GroupPut(ctx, token, "g", 0, nodegroup.Context{}, put); !errors.Is(err, nodegroup.ErrConflict) {
		t.Fatalf("stale CAS accepted: %v", err)
	}
	if got := lease(); got != expires {
		t.Fatalf("rejected CAS renewed lease: %d", got)
	}
	noop, created, err := s.GroupPut(ctx, token, "g", first.Version, nodegroup.Context{}, put)
	if err != nil || created || noop.Version != first.Version || lease() != put.ExpiresUnix {
		t.Fatalf("renewal no-op: %+v %v", noop, err)
	}
	seq, _, err = s.GroupSnapshot(ctx)
	if err != nil || seq != first.Version {
		t.Fatalf("key-only renewal fabricated Group delta: %d %v", seq, err)
	}
	put.ExpiresUnix = expires
	if _, _, err := s.GroupPut(ctx, token, "g", first.Version, nodegroup.Context{}, put); err != nil || lease() != expires+3600 {
		t.Fatalf("late renewal shortened lease: %v", err)
	}
	put.ExpiresUnix = 0
	if _, _, err := s.GroupPut(ctx, token, "g", first.Version, nodegroup.Context{}, put); err != nil || lease() != 0 {
		t.Fatalf("permanent renewal: %v", err)
	}
	put.ExpiresUnix = expires
	if _, _, err := s.GroupPut(ctx, token, "g", first.Version, nodegroup.Context{}, put); err != nil || lease() != 0 {
		t.Fatalf("finite renewal downgraded permanent pair: %v", err)
	}
	put.ExpiresUnix = time.Now().Unix() - 1
	if _, _, err := s.GroupPut(ctx, token, "g", first.Version, nodegroup.Context{}, put); !errors.Is(err, nodegroup.ErrInvalid) {
		t.Fatalf("expired absolute renewal accepted: %v", err)
	}
}

func TestGroupTombstoneRequiresOwnerMACAndExplicitRecreation(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	pair := testKeyPair("1", "2")
	token := groupToken(t, pair)
	first, _, err := s.GroupPut(ctx, token, "g", 0, nodegroup.Context{ImageRepo: "private"}, &nodegroup.KeyPut{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey})
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := s.GroupDelete(ctx, token, "g", first.Version)
	if err != nil || deleted.Version <= first.Version {
		t.Fatalf("delete: %+v %v", deleted, err)
	}
	var material string
	if err := s.db.QueryRow(`SELECT api_secret_enc||manifest_key_hash||manifest_key_enc||context_enc FROM node_groups WHERE name='g'`).Scan(&material); err != nil || material != "" {
		t.Fatalf("tombstone retained secrets: %v", err)
	}
	if allowed, err := s.HasKeyPair(ctx, pair); err != nil || !allowed {
		t.Fatalf("Group delete removed shared pair: %v %v", allowed, err)
	}
	_, err = s.GroupGet(ctx, token, "g")
	groupNotFoundVersion(t, err, deleted.Version)
	again, err := s.GroupDelete(ctx, token, "g", deleted.Version)
	if err != nil || again.Version != deleted.Version {
		t.Fatalf("authorized repeated delete: %+v %v", again, err)
	}
	// Preserve the fingerprint while corrupting the MAC, including the repeat
	// DELETE path where a tombstone has no secret material left to verify.
	forged := token[:len(token)-2] + "00"
	if forged == token {
		forged = token[:len(token)-2] + "01"
	}
	if _, err := s.GroupDelete(ctx, forged, "g", deleted.Version); !errors.Is(err, nodegroup.ErrNotFound) {
		t.Fatalf("short fingerprint authorized tombstone: %v", err)
	}
	if _, err := s.RemoveKeyPair(ctx, pair); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GroupDelete(ctx, token, "g", deleted.Version); !errors.Is(err, nodegroup.ErrNotFound) {
		t.Fatalf("revoked pair repeated delete: %v", err)
	}
	other := testKeyPair("3", "4")
	_, _, err = s.GroupPut(ctx, groupToken(t, other), "g", deleted.Version, nodegroup.Context{}, &nodegroup.KeyPut{APISecret: other.APISecret, ManifestKey: other.ManifestKey})
	if !errors.Is(err, nodegroup.ErrNotFound) {
		t.Fatalf("tombstone takeover: %v", err)
	}
	if allowed, _ := s.HasKeyPair(ctx, other); allowed {
		t.Fatal("rejected takeover installed pair")
	}
	put := &nodegroup.KeyPut{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey}
	if _, _, err := s.GroupPut(ctx, token, "g", 0, nodegroup.Context{}, put); !errors.Is(err, nodegroup.ErrConflict) {
		t.Fatalf("background replay revived deleted Group: %v", err)
	}
	recreated, created, err := s.GroupPut(ctx, token, "g", deleted.Version, nodegroup.Context{}, put)
	if err != nil || !created || recreated.Version <= deleted.Version {
		t.Fatalf("explicit recreate: %+v %v", recreated, err)
	}
}

func TestGroupConcurrentCASAndOwnerScopedPages(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	pair := testKeyPair("1", "2")
	token := groupToken(t, pair)
	first, _, err := s.GroupPut(ctx, token, "g", 0, nodegroup.Context{}, &nodegroup.KeyPut{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, port := range []int{41, 42} {
		wg.Add(1)
		go func(port int) {
			defer wg.Done()
			<-start
			_, _, err := s.GroupPut(ctx, token, "g", first.Version, nodegroup.Context{TargetPort: port}, nil)
			errs <- err
		}(port)
	}
	close(start)
	wg.Wait()
	close(errs)
	succeeded, conflicted := 0, 0
	for err := range errs {
		if err == nil {
			succeeded++
		} else if errors.Is(err, nodegroup.ErrConflict) {
			conflicted++
		} else {
			t.Fatal(err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("CAS results: %d success, %d conflict", succeeded, conflicted)
	}
	if _, _, err := s.GroupPut(ctx, token, "h", 0, nodegroup.Context{}, nil); err != nil {
		t.Fatal(err)
	}
	page, err := s.GroupList(ctx, token, "", 1)
	if err != nil || len(page.Groups) != 1 || page.NextCursor == "" {
		t.Fatalf("first page: %+v %v", page, err)
	}
	last, err := s.GroupList(ctx, token, page.NextCursor, 1)
	if err != nil || len(last.Groups) != 1 || last.Groups[0].Group == page.Groups[0].Group || last.NextCursor != "" {
		t.Fatalf("last page: %+v %v", last, err)
	}
	other := testKeyPair("3", "4")
	if _, err := s.AddKeyPair(ctx, other, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GroupList(ctx, groupToken(t, other), page.NextCursor, 1); !errors.Is(err, nodegroup.ErrInvalid) {
		t.Fatalf("cursor crossed owners: %v", err)
	}
	seq, supports, err := s.GroupSnapshot(ctx)
	if err != nil || len(supports) != 2 || seq != 3 {
		t.Fatalf("consistent catalog: %d %+v %v", seq, supports, err)
	}
	if supports[0].DefaultContext != nil || supports[0].Entries[0].Routing.TargetPort == 0 {
		t.Fatalf("empty-default active support lost: %+v", supports[0])
	}
}

func TestGroupBudgetsRejectBeforePairInstallation(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	pair := testKeyPair("1", "2")
	token := groupToken(t, pair)
	_, _, err := s.GroupPut(ctx, token, "g", 0, nodegroup.Context{Metadata: map[string]string{"huge": strings.Repeat("x", nodegroup.MaxContextBytes)}}, &nodegroup.KeyPut{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey})
	if !errors.Is(err, nodegroup.ErrTooLarge) {
		t.Fatalf("budget error: %v", err)
	}
	if allowed, _ := s.HasKeyPair(ctx, pair); allowed {
		t.Fatal("oversized Group installed credentials")
	}
}

func TestGroupCommitWithoutReplySurvivesReopen(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	pair := testKeyPair("1", "2")
	token := groupToken(t, pair)
	// The database commits, but the caller does not consume the result. The
	// subsequent read proves state, not which retry caused it or key renewal.
	_, _, err := s.GroupPut(ctx, token, "g", 0, nodegroup.Context{ImageRepo: "private-repository"}, &nodegroup.KeyPut{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey})
	if err != nil {
		t.Fatal(err)
	}
	var stored, path, database string
	var seq int
	if err := s.db.QueryRow(`SELECT api_secret_enc||manifest_key_enc||context_enc FROM node_groups`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	for _, plain := range []string{pair.APISecret, pair.ManifestKey, "private-repository"} {
		if strings.Contains(stored, plain) {
			t.Fatal("Group root/default material was stored unencrypted")
		}
	}
	if err := s.db.QueryRow(`PRAGMA database_list`).Scan(&seq, &database, &path); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, s.box)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	detail, err := reopened.GroupGet(ctx, token, "g")
	if err != nil || detail.Version != 1 || detail.Context.ImageRepo != "private-repository" {
		t.Fatalf("committed Group lost after restart: %+v %v", detail, err)
	}
	if _, _, err := reopened.GroupPut(ctx, token, "g", 0, nodegroup.Context{ImageRepo: "private-repository"}, nil); !errors.Is(err, nodegroup.ErrConflict) {
		t.Fatalf("unknown-result retry bypassed original CAS: %v", err)
	}
}
