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
			NodeSandboxID: "stable-g1", TemplateID: "e2b-img-bWFuaWZlc3Q6Ly9hYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFh",
			Profile: "e2b", EnvdAccessToken: "envd", TrafficAccessToken: "traffic", ForwardAccessToken: "kat1.forward",
		},
	}
	got := roundTrip(t, &Msg{Type: TypeCmdAck, Ack: want})
	if got.Ack == nil || got.Ack.Connect == nil || *got.Ack.Connect != *want.Connect ||
		got.Ack.CmdID != want.CmdID || got.Ack.Status != want.Status {
		t.Fatalf("connect ack round-trip: %+v", got.Ack)
	}
}

func TestNodeLinkRejectedAckRoundTripPreservesHTTPStatus(t *testing.T) {
	want := &CmdAck{
		CmdID: "connect-rejected", Status: AckRejected,
		Reason: "migration credential not allowed", HTTPStatus: 403,
	}
	got := roundTrip(t, &Msg{Type: TypeCmdAck, Ack: want})
	if got.Ack == nil || got.Ack.CmdID != want.CmdID || got.Ack.Status != want.Status ||
		got.Ack.Reason != want.Reason || got.Ack.HTTPStatus != want.HTTPStatus {
		t.Fatalf("rejected ack round-trip: %+v", got.Ack)
	}
}

func TestNodeLinkExecSessionCommandAndAckRoundTrip(t *testing.T) {
	command := &Command{
		CmdID: "exec-1", Kind: CmdExecSession, SID: "stable-g2", Profile: "bare",
		APISecretFingerprint: "api-secret-fingerprint",
		Cluster: &ClusterSandboxContext{
			Group: "/tenant/workloads", RouteKey: "route-stable", AuthSandboxID: "stable",
		},
		MigrationToken: "kmt1.opaque-migration-token",
		TTLSeconds:     37,
		ExecConditions: []string{"request.cwd == '/'", "!request.stdio.tty"},
	}
	got := roundTrip(t, &Msg{Type: TypeCommand, Rev: 11, Cmd: command})
	if got.Cmd == nil || got.Cmd.Kind != CmdExecSession || got.Cmd.SID != command.SID ||
		got.Cmd.Profile != command.Profile || got.Cmd.APISecretFingerprint != command.APISecretFingerprint ||
		got.Cmd.MigrationToken != command.MigrationToken || got.Cmd.TTLSeconds != command.TTLSeconds ||
		len(got.Cmd.ExecConditions) != 2 || got.Cmd.ExecConditions[0] != command.ExecConditions[0] ||
		got.Cmd.ExecConditions[1] != command.ExecConditions[1] ||
		got.Cmd.Cluster == nil || *got.Cmd.Cluster != *command.Cluster || got.Rev != 11 {
		t.Fatalf("exec-session command round-trip: %+v rev=%d", got.Cmd, got.Rev)
	}

	ack := &CmdAck{
		CmdID: "exec-1", Status: AckAccepted,
		ExecSession: &ExecSessionResult{ExecAccessToken: "kat1.exec"},
	}
	got = roundTrip(t, &Msg{Type: TypeCmdAck, Ack: ack})
	if got.Ack == nil || got.Ack.ExecSession == nil || *got.Ack.ExecSession != *ack.ExecSession ||
		got.Ack.CmdID != ack.CmdID || got.Ack.Status != ack.Status || got.Ack.Connect != nil {
		t.Fatalf("exec-session ack round-trip: %+v", got.Ack)
	}
}

func TestCommandTracksExecConditionsWirePresence(t *testing.T) {
	for _, raw := range []string{
		`{"cmd_id":"other-empty","kind":"connect","exec_conditions":[]}`,
		`{"cmd_id":"exec-null","kind":"exec_session","exec_conditions":null}`,
	} {
		var command Command
		if err := json.Unmarshal([]byte(raw), &command); err != nil {
			t.Fatal(err)
		}
		if !command.ExecConditionsSpecified() {
			t.Fatalf("wire presence lost for %s", raw)
		}
	}

	var omitted Command
	if err := json.Unmarshal([]byte(`{"cmd_id":"exec-omitted","kind":"exec_session"}`), &omitted); err != nil {
		t.Fatal(err)
	}
	if omitted.ExecConditionsSpecified() {
		t.Fatal("omitted exec_conditions reported as specified")
	}
	payload, err := json.Marshal(&Command{CmdID: "exec-empty", Kind: CmdExecSession, ExecConditions: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte("exec_conditions")) {
		t.Fatalf("empty exec conditions were not canonicalized to omission: %s", payload)
	}
}

func TestNodeLinkExecSessionWireOmitsCredentialRoots(t *testing.T) {
	payload, err := json.Marshal(&Msg{
		Type: TypeCmdAck,
		Ack: &CmdAck{
			CmdID: "exec-1", Status: AckAccepted,
			ExecSession: &ExecSessionResult{ExecAccessToken: "kat1.exec"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"api_secret", "service_secret", "session_id"} {
		if bytes.Contains(payload, []byte(forbidden)) {
			t.Fatalf("exec-session ack leaks %q: %s", forbidden, payload)
		}
	}
	if !bytes.Contains(payload, []byte(`"exec_session":{"exec_access_token":"kat1.exec"}`)) {
		t.Fatalf("exec-session ack wire = %s", payload)
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
