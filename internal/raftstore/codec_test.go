package raftstore

import (
	"strings"
	"testing"
)

func TestCommandCodecRejectsInvalidUTF8WithoutNormalization(t *testing.T) {
	invalid := string([]byte{'b', 0xff})
	registryLayout := testRegistryLayout(1, "generation-codec-utf8")
	identity := registryLayoutShardIdentity(t, registryLayout, 0)
	record := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, false)
	record.Group = invalid
	if _, err := EncodeDataCommand(DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &record,
	}); err == nil || !strings.Contains(err.Error(), "invalid UTF-8") {
		t.Fatalf("EncodeDataCommand invalid UTF-8 error = %v", err)
	}

	registryLayout.ClusterID = invalid
	if _, err := EncodeSystemCommand(SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digestFor("layout"),
	}); err == nil || !strings.Contains(err.Error(), "invalid UTF-8") {
		t.Fatalf("EncodeSystemCommand invalid UTF-8 error = %v", err)
	}

	if _, err := DecodeSystemCommand([]byte("{\"type\":\"\xff\"}")); err == nil ||
		!strings.Contains(err.Error(), "invalid UTF-8") {
		t.Fatalf("DecodeSystemCommand invalid UTF-8 error = %v", err)
	}
}
