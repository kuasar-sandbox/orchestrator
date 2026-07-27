package routesync

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
)

func TestNodeLinkConnectCommandRoundTripPreservesMigrationToken(t *testing.T) {
	want := &Command{
		CmdID: "connect-1", Kind: CmdConnect, SID: "stable-g1", Profile: "e2b",
		APISecretFingerprint: "api-secret-fingerprint",
		Cluster: &ClusterSandboxContext{
			Group: "/tenant/workloads", RouteKey: "route-stable", AuthSandboxID: "stable",
		},
		MigrationToken: "kmt1.opaque-migration-token",
	}
	got := roundTrip(t, &Msg{Type: TypeCommand, Rev: 9, Cmd: want})
	if got.Cmd == nil || got.Cmd.Kind != CmdConnect || got.Cmd.SID != want.SID ||
		got.Cmd.MigrationToken != want.MigrationToken || got.Cmd.Cluster == nil ||
		got.Cmd.Cluster.AuthSandboxID != want.Cluster.AuthSandboxID || got.Rev != 9 {
		t.Fatalf("connect command round-trip: %+v rev=%d", got.Cmd, got.Rev)
	}
}

func TestNodeLinkMigrationTokenWireSizeLimit(t *testing.T) {
	exact := &Msg{
		Type: TypeCommand,
		Cmd: &Command{
			CmdID: "connect-exact", Kind: CmdConnect,
			MigrationToken: strings.Repeat("x", migrationtoken.MaxWireSize),
		},
	}
	got := roundTrip(t, exact)
	if got.Cmd == nil || len(got.Cmd.MigrationToken) != migrationtoken.MaxWireSize {
		t.Fatalf("exact-limit migration token length = %d", len(got.Cmd.MigrationToken))
	}

	oversized := &Msg{
		Type: TypeCommand,
		Cmd: &Command{
			CmdID: "connect-oversized", Kind: CmdConnect,
			MigrationToken: strings.Repeat("x", migrationtoken.MaxWireSize+1),
		},
	}
	var output bytes.Buffer
	if err := WriteMsg(&output, oversized); !errors.Is(err, migrationtoken.ErrTokenTooLarge) {
		t.Fatalf("WriteMsg oversized migration token error = %v, want token too large", err)
	}
	if output.Len() != 0 {
		t.Fatal("WriteMsg emitted an oversized migration token frame")
	}

	payload, err := json.Marshal(oversized)
	if err != nil {
		t.Fatal(err)
	}
	var frame bytes.Buffer
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(payload)))
	frame.Write(header[:])
	frame.Write(payload)
	if _, err := ReadMsg(&frame); !errors.Is(err, migrationtoken.ErrTokenTooLarge) {
		t.Fatalf("ReadMsg oversized migration token error = %v, want token too large", err)
	}
}
