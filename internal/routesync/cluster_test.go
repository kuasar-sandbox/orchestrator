package routesync

import (
	"bytes"
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
		DataEndpoint:  "10.0.0.1:8443", NodeEpoch: 7, SessionSeq: 11,
		LoadModelVersion: 1, AcceptRedirect: true,
	}})
	if nr.NodeReg == nil || nr.NodeReg.NodeID != "n1" || nr.NodeReg.Labels["slot"] != "c01-s03" || nr.NodeReg.BuildCapacity.Mem != 8<<30 ||
		nr.NodeReg.NodeEpoch != 7 || nr.NodeReg.SessionSeq != 11 || nr.NodeReg.LoadModelVersion != 1 || !nr.NodeReg.AcceptRedirect {
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
		TemplateRef: "manifest://abc", KeyFingerprint: "e2b_deadbeef", NodeEpoch: 7, SessionSeq: 11,
		StorageGeneration: "generation-1", Binding: "keb1.opaque", BindingDigest: "binding-digest",
	}})
	if c.Cmd == nil || c.Cmd.Kind != CmdCreate || c.Cmd.Config["kuasar-sandbox.cluster"] == "" || c.Rev != 42 ||
		c.Cmd.NodeEpoch != 7 || c.Cmd.SessionSeq != 11 || c.Cmd.StorageGeneration != "generation-1" || c.Cmd.BindingDigest != "binding-digest" {
		t.Fatalf("command round-trip: %+v rev=%d", c.Cmd, c.Rev)
	}
	k := roundTrip(t, &Msg{Type: TypeCommand, Cmd: &Command{
		CmdID: "k1", Kind: CmdKeyPut, ManifestKeyType: "ref", ManifestKeyRef: "vault://tenant/key", ExpiresUnix: 123,
	}})
	if k.Cmd == nil || k.Cmd.ManifestKeyType != "ref" || k.Cmd.ManifestKeyRef != "vault://tenant/key" {
		t.Fatalf("key_put ref round-trip: %+v", k.Cmd)
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
		FloatingIP: "100.100.96.5", AccessToken: "tok", NodeID: "n1", NodeEpoch: 7,
		StorageGeneration: "generation-1", BindingDigest: "binding-digest", EventSeq: 3,
	}})
	if r.Route == nil || r.Route.SandboxID != "s1" || r.Route.State != StateRunning ||
		r.Route.NodeEpoch != 7 || r.Route.BindingDigest != "binding-digest" || r.Route.EventSeq != 3 {
		t.Fatalf("sandbox route round-trip: %+v", r.Route)
	}

	a := roundTrip(t, &Msg{Type: TypeCmdAck, Ack: &CmdAck{CmdID: "x1", Status: AckAccepted}})
	if a.Ack == nil || a.Ack.Status != AckAccepted {
		t.Fatalf("cmd_ack round-trip: %+v", a.Ack)
	}

	event := roundTrip(t, &Msg{Type: TypeBuildEvent, Build: &BuildEvent{
		BuildID: "build-1", NodeEpoch: 7, EventSeq: 4, StorageGeneration: "generation-1",
		BindingDigest: "binding-digest", State: "ready",
	}})
	if event.Build == nil || event.Build.EventSeq != 4 || event.Build.BindingDigest != "binding-digest" {
		t.Fatalf("build event round-trip: %+v", event.Build)
	}
	eventAck := roundTrip(t, &Msg{Type: TypeEventAck, EventAck: &EventAck{
		ObjectKind: "build", ObjectID: "build-1", EventSeq: 4,
	}})
	if eventAck.EventAck == nil || eventAck.EventAck.ObjectID != "build-1" || eventAck.EventAck.EventSeq != 4 {
		t.Fatalf("event ack round-trip: %+v", eventAck.EventAck)
	}

	h := roundTrip(t, &Msg{Type: TypeHeartbeat, Beat: &Heartbeat{Zone: "z1", Allocated: 1 << 30, Pool: 8 << 30, Draining: true}})
	if h.Beat == nil || !h.Beat.Draining || h.Beat.Pool != 8<<30 {
		t.Fatalf("heartbeat round-trip: %+v", h.Beat)
	}

	reg := roundTrip(t, &Msg{Type: TypeRegister, Register: &Register{
		Version: Version, Subscribe: &Subscribe{Kind: KindRegistry}, ResumeFrom: MakeRevToken("node-fp", 100),
	}})
	if reg.Register == nil || reg.Register.Version != Version || reg.Register.Subscribe.Kind != KindRegistry || reg.Register.ResumeFrom != "node-fp:100" {
		t.Fatalf("registry register round-trip: %+v", reg.Register)
	}
}

func TestReadRegisterRejectsProtocolVersionMismatch(t *testing.T) {
	for _, version := range []int{0, Version - 1, Version + 1} {
		var buffer bytes.Buffer
		if err := WriteMsg(&buffer, &Msg{Type: TypeRegister, Register: &Register{Version: version}}); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadRegister(&buffer); err == nil {
			t.Fatalf("protocol version %d was accepted", version)
		}
	}
}
