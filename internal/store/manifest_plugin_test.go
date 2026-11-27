package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/manifestkey"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestManifestPluginStoreBoundaries(t *testing.T) {
	if !testsecret.InSubprocess(t) {
		return
	}
	material := bytes.Repeat([]byte{0xab}, 32)
	var key [32]byte
	copy(key[:], material)
	fp := manifestkey.Fingerprint(key)
	broker := testsecret.Start(t, map[string][]byte{
		"Tenant/Manifest_v1": material, "alias": material,
		"other": bytes.Repeat([]byte{0x33}, 32), "short": []byte("short"),
	})
	ctx := t.Context()

	t.Run("identity and stored carrier", func(t *testing.T) {
		s := testStore(t)
		plain := KeyPair{APISecret: strings.Repeat("1", 64), ManifestKey: hex.EncodeToString(material)}
		if added, err := s.AddKeyPair(ctx, plain, "", 0, ""); err != nil || !added {
			t.Fatalf("plain add: %v %v", added, err)
		}
		ref := KeyPair{APISecret: plain.APISecret, ManifestKey: "@test/Tenant/Manifest_v1"}
		if added, err := s.AddKeyPair(ctx, ref, "", 0, ""); err != nil || added {
			t.Fatalf("carrier refresh: %v %v", added, err)
		}
		apiFP, _ := APISecretHash(plain.APISecret)
		stored, found, err := s.AllowedKeyPairByAPISecretFingerprint(ctx, apiFP)
		if err != nil || !found || stored.ManifestKey != ref.ManifestKey || stored.ManifestKeyFingerprint != fp {
			t.Fatalf("stored carrier or fingerprint mismatch: %v", err)
		}
		var encrypted, hash string
		if err := s.db.QueryRow(`SELECT manifest_key_enc,manifest_key_hash FROM manifest_keys WHERE api_secret_hash=?`, apiFP).Scan(&encrypted, &hash); err != nil {
			t.Fatal(err)
		}
		text, err := s.box.DecryptString(encrypted)
		if err != nil || text != ref.ManifestKey || hash != fp {
			t.Fatalf("stored material was rewritten: %v", err)
		}
		for _, value := range []string{plain.ManifestKey, ref.ManifestKey, "@test/alias"} {
			if found, err := s.HasKeyPair(ctx, KeyPair{APISecret: plain.APISecret, ManifestKey: value}); err != nil || !found {
				t.Fatalf("same material identity: %v %v", found, err)
			}
		}
		if _, err := s.AddKeyPair(ctx, KeyPair{APISecret: plain.APISecret, ManifestKey: "@test/other"}, "", 0, ""); !errors.Is(err, ErrKeyPairConflict) {
			t.Fatalf("changed material accepted: %v", err)
		}
		if _, err := s.AddKeyPair(ctx, KeyPair{APISecret: strings.Repeat("4", 64), ManifestKey: ref.ManifestKey, ManifestKeyFingerprint: strings.Repeat("0", 64)}, "", 0, ""); err == nil {
			t.Fatal("untrusted fingerprint accepted")
		}
	})

	t.Run("invalid material is not persisted", func(t *testing.T) {
		s := testStore(t)
		for _, value := range []string{"@test/missing", "@test/short", "@absent/id", "@test/"} {
			if _, err := s.AddKeyPair(ctx, KeyPair{APISecret: strings.Repeat("5", 64), ManifestKey: value}, "", 0, ""); err == nil {
				t.Fatalf("accepted invalid reference %q", value)
			}
		}
		rows, err := s.ListKeyPairs(ctx)
		if err != nil || len(rows) != 0 {
			t.Fatalf("failed resolution persisted rows: %v", err)
		}
	})

	t.Run("resolve precedes transaction", func(t *testing.T) {
		s := testStore(t)
		s.db.SetMaxOpenConns(1)
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		before := broker.Count("alias")
		limited, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		_, err = s.AddKeyPair(limited, KeyPair{APISecret: strings.Repeat("6", 64), ManifestKey: "@test/alias"}, "", 0, "")
		if err == nil || broker.Count("alias") != before+1 {
			t.Fatalf("resolver ran behind the held DB transaction: %v", err)
		}
	})

	t.Run("durable projections need no broker", func(t *testing.T) {
		s := testStore(t)
		sb := testSandboxWithServiceCredentials("plugin-sandbox", types.ProfileBare)
		sb.ManifestKey = "@test/Tenant/Manifest_v1"
		if err := s.InsertSandbox(ctx, sb); err != nil {
			t.Fatal(err)
		}
		before := broker.Count("Tenant/Manifest_v1")
		broker.Set("Tenant/Manifest_v1", nil)
		defer broker.Set("Tenant/Manifest_v1", material)
		got, err := s.Get(ctx, sb.ID)
		if err != nil || got.ManifestKey != sb.ManifestKey || got.ManifestKeyFingerprint != fp {
			t.Fatalf("read bound identity: %v", err)
		}
		got.DeadlineUnix++
		if err := s.Put(ctx, got); err != nil {
			t.Fatal(err)
		}
		if broker.Count("Tenant/Manifest_v1") != before {
			t.Fatal("lifecycle projection resolved a durable carrier")
		}
	})

	t.Run("group transaction retains references", func(t *testing.T) {
		s := testStore(t)
		pair := KeyPair{APISecret: strings.Repeat("7", 64), ManifestKey: "@test/alias"}
		token := groupToken(t, pair)
		put := &nodegroup.KeyPut{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey}
		result, created, err := s.GroupPut(ctx, token, "plugin-group", 0, nodegroup.Context{}, put)
		if err != nil || !created {
			t.Fatalf("group put: %v %v", created, err)
		}
		if put.ManifestKeyFingerprint != fp {
			t.Fatal("accepted Group command lost its verified material identity")
		}
		_, admitted, err := s.GroupAdmit(ctx, token, "plugin-group", result.Version)
		if err != nil || admitted.ManifestKey != pair.ManifestKey || admitted.ManifestKeyFingerprint != fp {
			t.Fatalf("group binding: %v", err)
		}
	})

	t.Run("Build registration reuses its bound root", func(t *testing.T) {
		s := testStore(t)
		build := admissionBuild("plugin-build", types.BuildResources{CPU: 1000, Memory: 1 << 30})
		build.ManifestKey = "@test/alias"
		pair, err := ResolveKeyPair(ctx, KeyPair{APISecret: build.APISecret, ManifestKey: build.ManifestKey})
		if err != nil {
			t.Fatal(err)
		}
		before := broker.Count("alias")
		broker.Set("alias", nil)
		defer broker.Set("alias", material)
		persisted, added, err := s.RegisterBuildWithMMDSRouteSecretValues(ctx, build, types.BuildAdmissionLimit{MaxBuilds: 1}, "", nil, pair)
		if err != nil || !added || persisted.ManifestKey != build.ManifestKey || persisted.ManifestKeyFingerprint != fp {
			t.Fatalf("registration with already-bound root: added=%v err=%v", added, err)
		}
		if broker.Count("alias") != before {
			t.Fatal("registration re-resolved an already-bound ManifestKey")
		}
	})
}
