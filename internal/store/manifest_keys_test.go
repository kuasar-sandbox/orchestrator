package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(filepath.Join(t.TempDir(), "test.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func testKeyPair(apiDigit, manifestDigit string) KeyPair {
	return KeyPair{
		APISecret:   strings.Repeat(apiDigit, 64),
		ManifestKey: strings.Repeat(manifestDigit, 64),
	}
}

func setTestSandboxServiceCredentials(sb *types.Sandbox) {
	sb.ServiceSecret = strings.Repeat("7", 64)
	forwardToken, err := keys.MintForwardAccessToken(sb.ServiceSecret, sb.AuthSandboxID())
	if err != nil {
		panic(err)
	}
	sb.ForwardAccessToken = forwardToken
	if sb.Profile == types.ProfileE2B {
		sb.EnvdAccessToken = "test-envd-access-token"
		sb.TrafficAccessToken = "test-traffic-access-token"
	}
}

// TestKeyPairTTL exercises add, exact refresh, candidate selection, expiry and
// pruning. Expired rows are excluded from authorization before physical pruning.
func TestKeyPairTTL(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("1", "2")
	apiHash, err := APISecretHash(pair.APISecret)
	if err != nil {
		t.Fatal(err)
	}
	manifestHash, err := ManifestKeyHash(pair.ManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(apiHash) != 64 || len(manifestHash) != 64 {
		t.Fatalf("full hashes = %d/%d chars, want 64/64", len(apiHash), len(manifestHash))
	}

	added, err := st.AddKeyPair(ctx, pair, "tenant", 0, "")
	if err != nil || !added {
		t.Fatalf("add: added=%v err=%v", added, err)
	}
	if ok, err := st.HasKeyPair(ctx, pair); err != nil || !ok {
		t.Fatalf("pair should be allowed after add: ok=%v err=%v", ok, err)
	}
	candidates, err := st.AllowedKeyPairsByAPISecretHashPrefix(ctx, apiHash[:24])
	if err != nil || len(candidates) != 1 || candidates[0] != pair {
		t.Fatalf("candidate lookup = %#v, err=%v", candidates, err)
	}
	exact, ok, err := st.AllowedKeyPairByAPISecretFingerprint(ctx, apiHash)
	if err != nil || !ok || exact != pair {
		t.Fatalf("exact lookup = %#v, ok=%v err=%v", exact, ok, err)
	}

	added, err = st.AddKeyPair(ctx, pair, "", 3600, "")
	if err != nil || added {
		t.Fatalf("exact re-add should refresh: added=%v err=%v", added, err)
	}
	infos, err := st.ListKeyPairs(ctx)
	if err != nil || len(infos) != 1 {
		t.Fatalf("list after refresh = %#v, err=%v", infos, err)
	}
	if infos[0].APISecretHash != apiHash || infos[0].ManifestKeyHash != manifestHash {
		t.Fatalf("listed hashes = %#v", infos[0])
	}
	if infos[0].Label != "tenant" || infos[0].ExpiresUnix == 0 {
		t.Fatalf("refresh should retain label and set expiry: %#v", infos[0])
	}

	if _, err := st.db.ExecContext(ctx,
		`UPDATE manifest_keys SET expires_unix=? WHERE api_secret_hash=?`,
		time.Now().Unix()-10, apiHash); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.HasKeyPair(ctx, pair); err != nil || ok {
		t.Fatalf("expired pair must not be allowed: ok=%v err=%v", ok, err)
	}
	if candidates, err := st.AllowedKeyPairsByAPISecretHashPrefix(ctx, apiHash[:24]); err != nil || len(candidates) != 0 {
		t.Fatalf("expired candidate lookup = %#v, err=%v", candidates, err)
	}
	if exact, ok, err := st.AllowedKeyPairByAPISecretFingerprint(ctx, apiHash); err != nil || ok || exact != (KeyPair{}) {
		t.Fatalf("expired exact lookup = %#v, ok=%v err=%v", exact, ok, err)
	}
	if infos, _ := st.ListKeyPairs(ctx); len(infos) != 1 {
		t.Fatal("expired row should remain listed until pruning")
	}

	removed, err := st.PruneExpiredKeyPairs(ctx)
	if err != nil || removed != 1 {
		t.Fatalf("prune: removed=%d err=%v", removed, err)
	}
	if infos, _ := st.ListKeyPairs(ctx); len(infos) != 0 {
		t.Fatal("pruned row should be gone")
	}
}

func TestAddKeyPairRejectsConflictingManifestKey(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("3", "4")
	auth := `{"auths":{"*":{"username":"original"}}}`
	if added, err := st.AddKeyPair(ctx, pair, "original", 0, auth); err != nil || !added {
		t.Fatalf("initial add: added=%v err=%v", added, err)
	}

	conflict := pair
	conflict.ManifestKey = strings.Repeat("5", 64)
	if added, err := st.AddKeyPair(ctx, conflict, "replacement", 10, `{"replacement":true}`); added || !errors.Is(err, ErrKeyPairConflict) {
		t.Fatalf("conflicting add: added=%v err=%v", added, err)
	}

	infos, err := st.ListKeyPairs(ctx)
	if err != nil || len(infos) != 1 {
		t.Fatalf("list after conflict = %#v, err=%v", infos, err)
	}
	if infos[0].Label != "original" || infos[0].ExpiresUnix != 0 {
		t.Fatalf("conflict changed existing row: %#v", infos[0])
	}
	if got, err := st.RegistryAuthForKeyPair(ctx, pair); err != nil || got != auth {
		t.Fatalf("original registry auth = %q, err=%v", got, err)
	}
	if got, err := st.RegistryAuthForKeyPair(ctx, conflict); err != nil || got != "" {
		t.Fatalf("conflicting pair retrieved registry auth = %q, err=%v", got, err)
	}
}

func TestAllowedKeyPairVerifiesBothStoredFingerprints(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("4", "5")
	if _, err := st.AddKeyPair(ctx, pair, "tenant", 0, ""); err != nil {
		t.Fatal(err)
	}
	apiHash, err := APISecretHash(pair.APISecret)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx,
		`UPDATE manifest_keys SET manifest_key_hash=? WHERE api_secret_hash=?`,
		strings.Repeat("0", 64), apiHash); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.AllowedKeyPairByAPISecretFingerprint(ctx, apiHash); err == nil || ok {
		t.Fatalf("corrupt pair lookup: ok=%v err=%v", ok, err)
	}
}

func TestRemoveKeyPairByAPISecretFingerprintDeletesExpiredKeyTableOnly(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("5", "6")
	if _, err := st.AddKeyPair(ctx, pair, "tenant", 0, ""); err != nil {
		t.Fatal(err)
	}
	sb := &types.Sandbox{
		ID: "sandbox-key-drop", TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("7", 64)}.String(),
		Profile: types.ProfileBare, State: types.StateRunning, APISecret: pair.APISecret, ManifestKey: pair.ManifestKey,
		CreatedUnix: 1,
	}
	setTestSandboxServiceCredentials(sb)
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	apiHash, err := APISecretHash(pair.APISecret)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx,
		`UPDATE manifest_keys SET expires_unix=? WHERE api_secret_hash=?`,
		time.Now().Unix()-10, apiHash); err != nil {
		t.Fatal(err)
	}

	if removed, err := st.RemoveKeyPairByAPISecretFingerprint(ctx, strings.Repeat("A", 64)); err == nil || removed != 0 {
		t.Fatalf("non-canonical key drop: removed=%d err=%v", removed, err)
	}
	removed, err := st.RemoveKeyPairByAPISecretFingerprint(ctx, apiHash)
	if err != nil || removed != 1 {
		t.Fatalf("expired key drop: removed=%d err=%v", removed, err)
	}
	if removed, err := st.RemoveKeyPairByAPISecretFingerprint(ctx, apiHash); err != nil || removed != 0 {
		t.Fatalf("repeated key drop: removed=%d err=%v", removed, err)
	}
	if infos, err := st.ListKeyPairs(ctx); err != nil || len(infos) != 0 {
		t.Fatalf("key table after drop = %#v, err=%v", infos, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil || got == nil {
		t.Fatalf("sandbox after key drop = %#v, err=%v", got, err)
	}
	if got.APISecret != pair.APISecret || got.ManifestKey != pair.ManifestKey {
		t.Fatalf("key drop changed sandbox credentials: %#v", got)
	}
}

func TestKeyPairRegistryAuthRefresh(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	pair := testKeyPair("6", "7")
	auth1 := `{"auths":{"*":{"username":"u","password":"p"}}}`
	if _, err := st.AddKeyPair(ctx, pair, "tenant", 0, auth1); err != nil {
		t.Fatal(err)
	}
	if got, err := st.RegistryAuthForKeyPair(ctx, pair); err != nil || got != auth1 {
		t.Fatalf("registry auth = %q, err=%v", got, err)
	}
	if _, err := st.AddKeyPair(ctx, pair, "", 3600, ""); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.RegistryAuthForKeyPair(ctx, pair); got != auth1 {
		t.Fatalf("empty refresh replaced registry auth: %q", got)
	}
	auth2 := `{"auths":{"*":{"identitytoken":"bearer"}}}`
	if _, err := st.AddKeyPair(ctx, pair, "", 0, auth2); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.RegistryAuthForKeyPair(ctx, pair); got != auth2 {
		t.Fatalf("new registry auth not stored: %q", got)
	}
}

func TestBusinessCredentialsAreEncryptedImmutableAndIndependentOfAllowlist(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	initial := testKeyPair("8", "9")
	replacement := testKeyPair("a", "b")
	if _, err := st.AddKeyPair(ctx, initial, "tenant", 0, ""); err != nil {
		t.Fatal(err)
	}

	sb := &types.Sandbox{
		ID: "sandbox-1", TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("c", 64)}.String(),
		Profile: types.ProfileBare, State: types.StateRunning, APISecret: initial.APISecret, ManifestKey: initial.ManifestKey,
		CreatedUnix: 1,
	}
	setTestSandboxServiceCredentials(sb)
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	b := &types.Build{
		BuildID: "build-1", TemplateID: "transient-1",
		APISecret: initial.APISecret, ManifestKey: initial.ManifestKey,
		Profile: types.ProfileE2B, Kind: types.KindImg,
		Status: types.BuildRegistered, CreatedUnix: 1,
	}
	if err := st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}

	var sbAPIEnc, sbManifestEnc, buildAPIEnc, buildManifestEnc string
	if err := st.db.QueryRowContext(ctx,
		`SELECT api_secret_enc,manifest_key_enc FROM sandboxes WHERE id=?`, sb.ID,
	).Scan(&sbAPIEnc, &sbManifestEnc); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRowContext(ctx,
		`SELECT api_secret_enc,manifest_key_enc FROM builds WHERE build_id=?`, b.BuildID,
	).Scan(&buildAPIEnc, &buildManifestEnc); err != nil {
		t.Fatal(err)
	}
	for name, ciphertext := range map[string]string{
		"sandbox API secret": sbAPIEnc, "sandbox manifest key": sbManifestEnc,
		"build API secret": buildAPIEnc, "build manifest key": buildManifestEnc,
	} {
		if ciphertext == initial.APISecret || ciphertext == initial.ManifestKey {
			t.Fatalf("%s stored as plaintext", name)
		}
	}

	sb.APISecret, sb.ManifestKey, sb.State = replacement.APISecret, replacement.ManifestKey, types.StatePaused
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	b.APISecret, b.ManifestKey, b.Status = replacement.APISecret, replacement.ManifestKey, types.BuildReady
	if err := st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	if removed, err := st.RemoveKeyPair(ctx, initial); err != nil || removed != 1 {
		t.Fatalf("remove allowlist pair: removed=%d err=%v", removed, err)
	}

	gotSB, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotSB.APISecret != initial.APISecret || gotSB.ManifestKey != initial.ManifestKey || gotSB.State != types.StatePaused {
		t.Fatalf("sandbox after upsert/drop = %#v", gotSB)
	}
	gotBuild, err := st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if gotBuild.APISecret != initial.APISecret || gotBuild.ManifestKey != initial.ManifestKey || gotBuild.Status != types.BuildReady {
		t.Fatalf("build after upsert/drop = %#v", gotBuild)
	}
}

func TestSandboxListUsesAPISecretCandidateHash(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	first := testKeyPair("c", "d")
	second := testKeyPair("e", "f")
	for id, pair := range map[string]KeyPair{"sandbox-1": first, "sandbox-2": second} {
		sb := &types.Sandbox{
			ID: id, Profile: types.ProfileBare, TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("1", 64)}.String(), State: types.StateRunning,
			APISecret: pair.APISecret, ManifestKey: pair.ManifestKey, CreatedUnix: 1,
		}
		setTestSandboxServiceCredentials(sb)
		if err := st.Put(ctx, sb); err != nil {
			t.Fatal(err)
		}
	}
	hash, err := APISecretHash(first.APISecret)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := st.List(ctx, "", hash[:24], 100, "")
	if err != nil || len(got) != 1 || got[0].ID != "sandbox-1" {
		t.Fatalf("candidate-filtered list = %#v, err=%v", got, err)
	}
}

func TestSecretValidation(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	for name, pair := range map[string]KeyPair{
		"short API secret":     {APISecret: "aa", ManifestKey: strings.Repeat("1", 64)},
		"uppercase API secret": {APISecret: strings.Repeat("A", 64), ManifestKey: strings.Repeat("1", 64)},
		"short manifest key":   {APISecret: strings.Repeat("1", 64), ManifestKey: "bb"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := st.AddKeyPair(ctx, pair, "", 0, ""); err == nil {
				t.Fatal("AddKeyPair accepted invalid secret")
			}
		})
	}
	if _, err := st.AllowedKeyPairsByAPISecretHashPrefix(ctx, strings.Repeat("1", 23)); err == nil {
		t.Fatal("candidate lookup accepted an invalid prefix")
	}
	if _, _, err := st.AllowedKeyPairByAPISecretFingerprint(ctx, strings.Repeat("A", 64)); err == nil {
		t.Fatal("exact lookup accepted a non-canonical fingerprint")
	}
}

func TestBuildOptionsRoundTrip(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	enabled, writeback := false, false
	b := &types.Build{
		BuildID:     "b1",
		TemplateID:  "transient-t1",
		APISecret:   strings.Repeat("2", 64),
		ManifestKey: strings.Repeat("3", 64),
		Profile:     types.ProfileE2B,
		Kind:        types.KindImg,
		Status:      types.BuildRegistered,
		Builder: types.BuildOptions{
			Referer: &types.BuildRefererOptions{
				Enabled:   &enabled,
				Writeback: &writeback,
			},
			Registry: &types.BuildRegistryOptions{
				TLS: &types.BuildRegistryTLSOptions{CABundlePEM: "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n"},
			},
		},
		CreatedUnix: time.Now().Unix(),
	}
	if err := st.PutBuild(ctx, b); err != nil {
		t.Fatalf("PutBuild: %v", err)
	}
	got, err := st.GetBuild(ctx, "b1")
	if err != nil {
		t.Fatalf("GetBuild: %v", err)
	}
	if got.Builder.Referer == nil || got.Builder.Referer.Enabled == nil || *got.Builder.Referer.Enabled {
		t.Fatalf("referer.enabled did not round-trip false: %+v", got.Builder)
	}
	if got.Builder.Referer.Writeback == nil || *got.Builder.Referer.Writeback {
		t.Fatalf("referer.writeback did not round-trip false: %+v", got.Builder)
	}
	if got.Builder.Registry == nil || got.Builder.Registry.TLS == nil ||
		got.Builder.Registry.TLS.CABundlePEM != "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n" {
		t.Fatalf("registry.tls.ca_bundle_pem did not round-trip: %+v", got.Builder)
	}
}
