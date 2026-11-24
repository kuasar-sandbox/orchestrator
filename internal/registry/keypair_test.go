package registry

import (
	"context"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

func testNodeKeyPair(apiSecret, manifestKey string, expiresUnix int64) clusterstate.NodeKeyPair {
	return clusterstate.NodeKeyPair{
		APISecretFingerprint:   fullFingerprint(apiSecret),
		APISecretType:          clusterstate.SecretInline,
		APISecret:              apiSecret,
		ManifestKeyFingerprint: fullFingerprint(manifestKey),
		ManifestKeyType:        clusterstate.SecretInline,
		ManifestKey:            manifestKey,
		ExpiresUnix:            expiresUnix,
	}
}

func TestUpsertNodeKeyPairRejectsFingerprintRebinding(t *testing.T) {
	ctx := context.Background()
	stores := newTestStores(t)
	seedTestNode(t, stores, "n1")
	original := testNodeKeyPair(
		"ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100",
		"00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
		123,
	)
	if err := stores.UpsertNodeKeyPair(ctx, "n1", original); err != nil {
		t.Fatal(err)
	}
	conflict := testNodeKeyPair(
		original.APISecret,
		strings.Repeat("2", 64),
		456,
	)
	if err := stores.UpsertNodeKeyPair(ctx, "n1", conflict); err == nil {
		t.Fatal("same API secret fingerprint was rebound to a different manifest key")
	}
	got, _, found, err := testReadNodeKeyPair(stores, ctx, "n1", original.APISecretFingerprint)
	if err != nil || !found || got.ManifestKeyFingerprint != original.ManifestKeyFingerprint || got.ExpiresUnix != original.ExpiresUnix {
		t.Fatalf("original pair changed after conflict: found=%v err=%v pair=%+v", found, err, got)
	}
}

func TestConcurrentUpsertNodeKeyPairCannotRebindFingerprint(t *testing.T) {
	ctx := context.Background()
	for iteration := 0; iteration < 25; iteration++ {
		stores := newTestStores(t)
		seedTestNode(t, stores, "n1")
		left := testNodeKeyPair(
			"ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100",
			strings.Repeat("2", 64), 123,
		)
		right := testNodeKeyPair(left.APISecret, strings.Repeat("3", 64), 456)
		start := make(chan struct{})
		type outcome struct {
			pair clusterstate.NodeKeyPair
			err  error
		}
		results := make(chan outcome, 2)
		for _, pair := range []clusterstate.NodeKeyPair{left, right} {
			pair := pair
			go func() {
				<-start
				results <- outcome{pair: pair, err: stores.UpsertNodeKeyPair(ctx, "n1", pair)}
			}()
		}
		close(start)
		first, second := <-results, <-results
		if (first.err == nil) == (second.err == nil) {
			t.Fatalf("iteration %d outcomes = %v, %v; want exactly one winner", iteration, first.err, second.err)
		}
		winner := first.pair
		if first.err != nil {
			winner = second.pair
		}
		got, _, found, err := testReadNodeKeyPair(stores, ctx, "n1", left.APISecretFingerprint)
		if err != nil || !found || !sameNodeKeyPairMaterial(got, winner) {
			t.Fatalf("iteration %d stored pair=%+v found=%v err=%v; winner=%+v", iteration, got, found, err, winner)
		}
	}
}

func TestNormalizeNodeKeyPairRejectsHalfPair(t *testing.T) {
	pair := testNodeKeyPair(
		"ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100",
		"00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
		123,
	)
	pair.ManifestKeyFingerprint = ""
	if _, err := normalizeNodeKeyPair(pair); err == nil {
		t.Fatal("half key pair accepted")
	}
}

func TestNormalizeNodeKeyPairRejectsNonCanonicalFingerprint(t *testing.T) {
	pair := testNodeKeyPair(
		"ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100",
		"00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
		123,
	)
	pair.APISecretFingerprint = "ABCDEF" + pair.APISecretFingerprint[6:]
	if _, err := normalizeNodeKeyPair(pair); err == nil {
		t.Fatal("uppercase API secret fingerprint accepted")
	}
}
