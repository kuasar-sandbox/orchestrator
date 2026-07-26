package routesync

import (
	"bytes"
	"strings"
	"testing"
)

func roundTrip(t *testing.T, m *Msg) *Msg {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteMsg(&buf, m); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	got, err := ReadMsg(&buf)
	if err != nil {
		t.Fatalf("ReadMsg: %v", err)
	}
	return got
}

func TestNodeLinkCodecRoundTrip(t *testing.T) {
	nr := roundTrip(t, &Msg{Type: TypeNodeRegister, NodeReg: &NodeRegister{
		NodeID: "n1", Labels: map[string]string{"zone": "z1", "slot": "c01-s03"},
		BuildCapacity: &BuildResources{CPU: 4000, Mem: 8 << 30, Storage: 64 << 30},
		DataEndpoint:  "10.0.0.1:8443", AcceptRedirect: true,
	}})
	if nr.NodeReg == nil || nr.NodeReg.NodeID != "n1" || nr.NodeReg.Labels["slot"] != "c01-s03" || nr.NodeReg.BuildCapacity.Mem != 8<<30 || !nr.NodeReg.AcceptRedirect {
		t.Fatalf("node_register round-trip: %+v", nr.NodeReg)
	}

	hello := roundTrip(t, &Msg{Type: TypeHello, Hello: &Hello{Version: Version, Redirect: &NodeLinkRedirect{Targets: []NodeLinkTarget{
		{MemberID: "registry-2", Endpoint: "127.0.0.1:7702"},
	}}}})
	if hello.Hello == nil || hello.Hello.Redirect == nil || len(hello.Hello.Redirect.Targets) != 1 || hello.Hello.Redirect.Targets[0].MemberID != "registry-2" {
		t.Fatalf("hello redirect round-trip: %+v", hello.Hello)
	}

	c := roundTrip(t, &Msg{Type: TypeCommand, Rev: 42, Cmd: &Command{
		CmdID: "x1", Kind: CmdCreate, SID: "s1", Config: map[string]string{"kuasar-sandbox.cluster": `{"group":"/c/p/a/g1","route_key":"u1:sess1"}`},
		TemplateRef: "manifest://abc", APISecretFingerprint: strings.Repeat("a", 64),
	}})
	if c.Cmd == nil || c.Cmd.Kind != CmdCreate || c.Cmd.Config["kuasar-sandbox.cluster"] == "" || c.Rev != 42 {
		t.Fatalf("command round-trip: %+v rev=%d", c.Cmd, c.Rev)
	}
	k := roundTrip(t, &Msg{Type: TypeCommand, Cmd: &Command{
		CmdID: "k1", Kind: CmdKeyPut,
		APISecretFingerprint: strings.Repeat("a", 64), APISecretType: "ref", APISecretRef: "vault://tenant/api",
		ManifestKeyFingerprint: strings.Repeat("b", 64), ManifestKeyType: "ref", ManifestKeyRef: "vault://tenant/manifest",
		ExpiresUnix: 123,
	}})
	if k.Cmd == nil || k.Cmd.APISecretType != "ref" || k.Cmd.APISecretRef != "vault://tenant/api" ||
		k.Cmd.ManifestKeyType != "ref" || k.Cmd.ManifestKeyRef != "vault://tenant/manifest" {
		t.Fatalf("key_put ref round-trip: %+v", k.Cmd)
	}
	d := roundTrip(t, &Msg{Type: TypeCommand, Cmd: &Command{
		CmdID: "k2", Kind: CmdKeyDrop, APISecretFingerprint: strings.Repeat("a", 64),
	}})
	if d.Cmd == nil || d.Cmd.Kind != CmdKeyDrop || d.Cmd.APISecretFingerprint != strings.Repeat("a", 64) {
		t.Fatalf("key_drop round-trip: %+v", d.Cmd)
	}
	b := roundTrip(t, &Msg{Type: TypeCommand, Cmd: &Command{
		CmdID: "b1", Kind: CmdBuildRegister, BuildID: "build-1", TemplateRef: "transient-1", Profile: "bare",
	}})
	if b.Cmd == nil || b.Cmd.BuildID != "build-1" || b.Cmd.Profile != "bare" {
		t.Fatalf("build_register round-trip: %+v", b.Cmd)
	}

	// Sandbox routes carry runtime state only; the node-link owner supplies cluster identity.
	r := roundTrip(t, &Msg{Type: TypeUpsert, Route: &RouteEntry{
		SandboxID: "s1", State: StateRunning,
		FloatingIP: "100.100.96.5", AccessToken: "tok",
	}})
	if r.Route == nil || r.Route.SandboxID != "s1" || r.Route.State != StateRunning {
		t.Fatalf("sandbox route round-trip: %+v", r.Route)
	}

	a := roundTrip(t, &Msg{Type: TypeCmdAck, Ack: &CmdAck{CmdID: "x1", Status: AckAccepted}})
	if a.Ack == nil || a.Ack.Status != AckAccepted {
		t.Fatalf("cmd_ack round-trip: %+v", a.Ack)
	}

	h := roundTrip(t, &Msg{Type: TypeHeartbeat, Beat: &Heartbeat{Zone: "z1", Allocated: 1 << 30, Pool: 8 << 30, Draining: true}})
	if h.Beat == nil || !h.Beat.Draining || h.Beat.Pool != 8<<30 {
		t.Fatalf("heartbeat round-trip: %+v", h.Beat)
	}

	reg := roundTrip(t, &Msg{Type: TypeRegister, Register: &Register{
		Subscribe: &Subscribe{Kind: KindRegistry}, ResumeFrom: MakeRevToken("node-fp", 100),
	}})
	if reg.Register == nil || reg.Register.Subscribe.Kind != KindRegistry || reg.Register.ResumeFrom != "node-fp:100" {
		t.Fatalf("registry register round-trip: %+v", reg.Register)
	}
}
