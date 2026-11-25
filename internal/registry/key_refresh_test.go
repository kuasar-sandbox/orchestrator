package registry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// Ownership must never be consulted for a key refresh: even a failed ownership
// page must not suppress renewal of an otherwise healthy node's key leases.
type rejectKeyRefreshOwnership struct {
	metadata.OwnershipRepository
	reads int
}

func (o *rejectKeyRefreshOwnership) ListSandboxOwners(context.Context, string, metadata.Cursor, int) (metadata.Page[metadata.Versioned[metadata.NodeSandboxRef]], error) {
	o.reads++
	return metadata.Page[metadata.Versioned[metadata.NodeSandboxRef]]{}, errors.New("unexpected sandbox ownership scan")
}

func (o *rejectKeyRefreshOwnership) ListBuildOwners(context.Context, string, metadata.Cursor, int) (metadata.Page[metadata.Versioned[metadata.NodeBuildRef]], error) {
	o.reads++
	return metadata.Page[metadata.Versioned[metadata.NodeBuildRef]]{}, errors.New("unexpected build ownership scan")
}

type countKeyRefreshPages struct {
	metadata.KeyPairRepository
	reads, failAt int
	pageErr       error
}

func (k *countKeyRefreshPages) ListKeyPairs(ctx context.Context, node string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.NodeKeyPair]], error) {
	k.reads++
	if limit <= 0 || limit > 256 {
		return metadata.Page[metadata.Versioned[metadata.NodeKeyPair]]{}, fmt.Errorf("unbounded key page: %d", limit)
	}
	if k.reads == k.failAt {
		return metadata.Page[metadata.Versioned[metadata.NodeKeyPair]]{}, k.pageErr
	}
	return k.KeyPairRepository.ListKeyPairs(ctx, node, cursor, limit)
}

func TestHeartbeatKeyRefreshDoesNotReadOwnership(t *testing.T) {
	reg := testRegWithBox(t)
	ctx := t.Context()
	pair := testNodeKeyPair(testAPISecret, testMK, time.Now().Add(keyLeaseTTL).Unix())
	if err := putTestNode(reg.stores, ctx, &NodeRecord{NodeID: "n1", KeyPairs: []clusterstate.NodeKeyPair{pair}}); err != nil {
		t.Fatal(err)
	}
	ownership := &rejectKeyRefreshOwnership{OwnershipRepository: reg.stores.ownership}
	reg.stores.ownership = ownership
	var sends int
	attachTestNode(t, reg, &fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind != routesync.CmdKeyPut || cmd.APISecretFingerprint != pair.APISecretFingerprint {
			t.Errorf("unexpected refresh command kind or fingerprint")
		}
		sends++
		reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
	}})
	refresh := make(chan struct{}, 1)
	refresh <- struct{}{}
	close(refresh)
	reg.runNodeKeyPairRefreshes(testNodeSession(ctx, reg.stores, "n1"), "n1", refresh)
	got, _, found, err := testReadNodeKeyPair(reg.stores, ctx, "n1", pair.APISecretFingerprint)
	if err != nil || !found || sends != 1 || got.AckedExpiresUnix != pair.ExpiresUnix {
		t.Fatalf("renewal: found=%v sends=%d acked=%d want=%d err=%v", found, sends, got.AckedExpiresUnix, pair.ExpiresUnix, err)
	}
	if ownership.reads != 0 {
		t.Fatalf("refresh scanned ownership %d times", ownership.reads)
	}
}

func TestGetNodeWithKeyPairsBoundedPagesAndFailures(t *testing.T) {
	s := newTestStores(t)
	ctx := t.Context()
	if err := s.PutNode(ctx, &NodeRecord{NodeID: "n1", APIEndpoint: "node-api"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 257; i++ {
		pair := testNodeKeyPair(fmt.Sprintf("%064x", i+1), testMK, time.Now().Add(keyLeaseTTL).Unix())
		if err := s.UpsertNodeKeyPair(ctx, "n1", pair); err != nil {
			t.Fatal(err)
		}
	}
	ownership := &rejectKeyRefreshOwnership{OwnershipRepository: s.ownership}
	s.ownership = ownership
	keys := &countKeyRefreshPages{KeyPairRepository: s.keyPairs}
	s.keyPairs = keys
	got, found, err := s.GetNodeWithKeyPairs(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("node: found=%v err=%v", found, err)
	}
	if got.APIEndpoint != "node-api" || len(got.KeyPairs) != 257 || keys.reads != 2 || ownership.reads != 0 || len(got.Sandboxes) != 0 || len(got.Builds) != 0 {
		t.Fatalf("profile=%q keys=%d pages=%d owner reads=%d sandboxes=%d builds=%d", got.APIEndpoint, len(got.KeyPairs), keys.reads, ownership.reads, len(got.Sandboxes), len(got.Builds))
	}
	seen := make(map[string]bool, len(got.KeyPairs))
	for _, pair := range got.KeyPairs {
		if seen[pair.APISecretFingerprint] {
			t.Fatal("duplicate key across pages")
		}
		seen[pair.APISecretFingerprint] = true
	}

	keys.reads = 0
	if got, found, err := s.GetNodeWithKeyPairs(ctx, "missing"); got != nil || found || err != nil || keys.reads != 0 {
		t.Fatalf("missing node: got=%v found=%v err=%v key pages=%d", got != nil, found, err, keys.reads)
	}
	keys.reads, keys.failAt, keys.pageErr = 0, 2, errors.New("second key page unavailable")
	if got, found, err := s.GetNodeWithKeyPairs(ctx, "n1"); got != nil || found || !errors.Is(err, keys.pageErr) {
		t.Fatalf("partial key snapshot escaped: got=%v found=%v err=%v", got != nil, found, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if got, found, err := s.GetNodeWithKeyPairs(cancelled, "n1"); got != nil || found || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read: got=%v found=%v err=%v", got != nil, found, err)
	}
}
