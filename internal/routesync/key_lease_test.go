package routesync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func inlineKeyMaterial(value string) NodeKeyMaterialV1 {
	raw, _ := hex.DecodeString(value)
	digest := sha256.Sum256(raw)
	return NodeKeyMaterialV1{
		Type: KeyMaterialInline, Value: value, Fingerprint: hex.EncodeToString(digest[:12]),
	}
}

func TestNodeKeyLeaseRequiresSeparateExactKeyDomains(t *testing.T) {
	lease := NodeKeyLeaseV1{
		Version:      NodeKeyLeaseVersionV1,
		Group:        "group-a",
		KeyRevision:  7,
		AuthKey:      inlineKeyMaterial(strings.Repeat("a", 64)),
		ManifestKey:  inlineKeyMaterial(strings.Repeat("b", 64)),
		RegistryAuth: NodeRegistryAuthV1{Type: KeyMaterialRef, Ref: "provider://registry/group-a"},
		ExpiresUnix:  1234,
	}
	if err := lease.Validate(); err != nil {
		t.Fatal(err)
	}
	lease.ManifestKey.Fingerprint = lease.AuthKey.Fingerprint
	if err := lease.Validate(); err == nil {
		t.Fatal("shared AuthKey/ManifestKey fingerprint accepted")
	}
	lease.ManifestKey = inlineKeyMaterial(strings.Repeat("b", 64))
	lease.ManifestKey.Fingerprint = strings.Repeat("c", 24)
	if err := lease.Validate(); err == nil {
		t.Fatal("mismatched inline fingerprint accepted")
	}
}

func TestNodeKeyLeaseRefFencesRotatedDrop(t *testing.T) {
	lease := NodeKeyLeaseV1{
		Version: NodeKeyLeaseVersionV1, Group: "group-a", KeyRevision: 7,
		AuthKey: inlineKeyMaterial(strings.Repeat("a", 64)), ManifestKey: inlineKeyMaterial(strings.Repeat("b", 64)),
		RegistryAuth: NodeRegistryAuthV1{Type: KeyMaterialInline, Value: "registry-a"}, ExpiresUnix: 1234,
	}
	ref, err := lease.Ref()
	if err != nil {
		t.Fatal(err)
	}
	if err := ref.Validate(); err != nil {
		t.Fatal(err)
	}
	ref.AuthKeyFingerprint = ref.ManifestKeyFingerprint
	if err := ref.Validate(); err == nil {
		t.Fatal("ambiguous key lease reference accepted")
	}
}

func TestNodeKeyLeaseRefBindsRegistryAuthAndRevision(t *testing.T) {
	lease := NodeKeyLeaseV1{
		Version: NodeKeyLeaseVersionV1, Group: "group-a", KeyRevision: 7,
		AuthKey: inlineKeyMaterial(strings.Repeat("a", 64)), ManifestKey: inlineKeyMaterial(strings.Repeat("b", 64)),
		RegistryAuth: NodeRegistryAuthV1{Type: KeyMaterialInline, Value: "registry-a"}, ExpiresUnix: 1234,
	}
	first, err := lease.Ref()
	if err != nil {
		t.Fatal(err)
	}
	lease.RegistryAuth.Value = "registry-b"
	second, err := lease.Ref()
	if err != nil {
		t.Fatal(err)
	}
	if first.RegistryAuthDigest == second.RegistryAuthDigest {
		t.Fatal("registry auth rotation did not change the exact lease reference")
	}
	lease.KeyRevision++
	third, err := lease.Ref()
	if err != nil {
		t.Fatal(err)
	}
	if second.KeyRevision == third.KeyRevision {
		t.Fatal("key revision did not change the exact lease reference")
	}
}

func TestKeyPutAckCarriesExactLeaseReference(t *testing.T) {
	lease := NodeKeyLeaseV1{
		Version: NodeKeyLeaseVersionV1, Group: "group-a", KeyRevision: 7,
		AuthKey: inlineKeyMaterial(strings.Repeat("a", 64)), ManifestKey: inlineKeyMaterial(strings.Repeat("b", 64)),
		ExpiresUnix: 1234,
	}
	value, err := lease.Ref()
	if err != nil {
		t.Fatal(err)
	}
	ref := &value
	encoded, err := json.Marshal(CmdAck{CmdID: "cmd-1", Status: AckAccepted, KeyLeaseRef: ref})
	if err != nil {
		t.Fatal(err)
	}
	var ack CmdAck
	if err := json.Unmarshal(encoded, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.KeyLeaseRef == nil || *ack.KeyLeaseRef != *ref {
		t.Fatalf("key lease ACK = %+v", ack)
	}
}
