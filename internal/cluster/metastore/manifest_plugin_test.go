package metastore

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/manifestkey"
	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
)

func TestNativeManifestMetadataRejectsUppercase(t *testing.T) {
	s := newBackendStore(t, backendCases["sqlite"](t))
	registerNode(t, s, "node")
	apiSecret := strings.Repeat("1", 64)
	pair := metadata.NodeKeyPair{APISecret: apiSecret, APISecretFingerprint: testFingerprint(apiSecret), ManifestKey: strings.Repeat("AB", 32), ManifestKeyFingerprint: testFingerprint(strings.Repeat("ab", 32))}
	if _, err := s.PutKeyPair(t.Context(), "node", pair); !errors.Is(err, metadata.ErrInvalidRecord) {
		t.Fatalf("metadata registration accepted non-canonical native ManifestKey: %v", err)
	}
	rows, err := s.ListKeyPairs(t.Context(), "node", "", 10)
	if err != nil || len(rows.Items) != 0 {
		t.Fatalf("invalid native root was persisted: %v", err)
	}
}

func TestManifestPluginMetadataBoundary(t *testing.T) {
	if !testsecret.InSubprocess(t) {
		return
	}
	material := bytes.Repeat([]byte{0xab}, 32)
	var raw [32]byte
	copy(raw[:], material)
	broker := testsecret.Start(t, map[string][]byte{"Tenant/Manifest_v1": material})
	s := newBackendStore(t, backendCases["sqlite"](t))
	registerNode(t, s, "node")
	apiSecret := strings.Repeat("1", 64)
	pair := metadata.NodeKeyPair{APISecret: apiSecret, APISecretFingerprint: testFingerprint(apiSecret), ManifestKey: "@test/Tenant/Manifest_v1", ManifestKeyFingerprint: manifestkey.Fingerprint(raw), ExpiresUnix: time.Now().Add(time.Hour).Unix()}
	before := broker.Count("Tenant/Manifest_v1")
	normalized, err := metadata.NormalizeKeyPair(pair)
	if err != nil || normalized.ManifestKey != pair.ManifestKey || broker.Count("Tenant/Manifest_v1") != before {
		t.Fatalf("pure metadata normalization changed the carrier: %v", err)
	}
	bad := pair
	bad.ManifestKeyFingerprint = strings.Repeat("0", 64)
	if _, err := s.PutKeyPair(t.Context(), "node", bad); err == nil {
		t.Fatal("untrusted fingerprint accepted")
	}
	rows, err := s.ListKeyPairs(t.Context(), "node", "", 10)
	if err != nil || len(rows.Items) != 0 {
		t.Fatalf("failed validation persisted a key: %v", err)
	}
	// Existing hex and a reference to the same root share identity and can
	// refresh the carrier without a conflicting key-pair assignment.
	literal := pair
	literal.ManifestKey = hex.EncodeToString(material)
	if _, err := s.PutKeyPair(t.Context(), "node", literal); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.AckKeyPair(t.Context(), "node", literal); err != nil || !ok {
		t.Fatalf("ack initial literal: %v %v", ok, err)
	}
	if _, err := s.PutKeyPair(t.Context(), "node", pair); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.AckKeyPair(t.Context(), "node", literal); err != nil || ok {
		t.Fatalf("old carrier ACK confirmed new reference: %v %v", ok, err)
	}
	before = broker.Count("Tenant/Manifest_v1")
	broker.Set("Tenant/Manifest_v1", nil)
	rows, err = s.ListKeyPairs(t.Context(), "node", "", 10)
	if err != nil || len(rows.Items) != 1 || rows.Items[0].Value.ManifestKey != pair.ManifestKey {
		t.Fatalf("durable metadata read changed the carrier: %v", err)
	}
	if rows.Items[0].Value.AckedExpiresUnix != 0 {
		t.Fatal("carrier refresh reused the old delivery acknowledgement")
	}
	if ok, err := s.AckKeyPair(t.Context(), "node", pair); err != nil || !ok {
		t.Fatalf("ack validated delivery: %v %v", ok, err)
	}
	if broker.Count("Tenant/Manifest_v1") != before {
		t.Fatal("metadata read/ACK resolved material inside a transaction")
	}
}
