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
	ref := NodeKeyLeaseRefV1{
		Version: NodeKeyLeaseVersionV1, Group: "group-a",
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
	}
	if err := ref.Validate(); err != nil {
		t.Fatal(err)
	}
	ref.AuthKeyFingerprint = ref.ManifestKeyFingerprint
	if err := ref.Validate(); err == nil {
		t.Fatal("ambiguous key lease reference accepted")
	}
}

func TestKeyPutAckCarriesExactLeaseReference(t *testing.T) {
	ref := &NodeKeyLeaseRefV1{
		Version: NodeKeyLeaseVersionV1, Group: "group-a",
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
	}
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
