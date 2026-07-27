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
		TimeoutSeconds: 37,
	}
	got := roundTrip(t, &Msg{Type: TypeCommand, Rev: 9, Cmd: want})
	if got.Cmd == nil || got.Cmd.Kind != CmdConnect || got.Cmd.SID != want.SID ||
		got.Cmd.MigrationToken != want.MigrationToken || got.Cmd.TimeoutSeconds != want.TimeoutSeconds || got.Cmd.Cluster == nil ||
		got.Cmd.Cluster.AuthSandboxID != want.Cluster.AuthSandboxID || got.Rev != 9 {
		t.Fatalf("connect command round-trip: %+v rev=%d", got.Cmd, got.Rev)
	}
}

func TestNodeLinkConnectAckRoundTripPreservesTypedResult(t *testing.T) {
	want := &CmdAck{
		CmdID: "connect-1", Status: AckAccepted,
		Connect: &ConnectResult{
			NodeSandboxID: "stable-g1", TemplateID: "e2b-img-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Profile: "e2b", EnvdAccessToken: "envd", TrafficAccessToken: "traffic", ForwardAccessToken: "kat1.forward",
		},
	}
	got := roundTrip(t, &Msg{Type: TypeCmdAck, Ack: want})
	if got.Ack == nil || got.Ack.Connect == nil || *got.Ack.Connect != *want.Connect ||
		got.Ack.CmdID != want.CmdID || got.Ack.Status != want.Status {
		t.Fatalf("connect ack round-trip: %+v", got.Ack)
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
