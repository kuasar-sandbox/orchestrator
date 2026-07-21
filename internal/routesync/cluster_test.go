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
		NodeID: "n1", EnrollmentID: "enrollment-1", Labels: map[string]string{"zone": "z1", "slot": "c01-s03"},
		BuildCapacity: &BuildResources{CPU: 4000, Mem: 8 << 30, Storage: 64 << 30},
		DataEndpoint:  "10.0.0.1:8443", NodeEpoch: 7, SessionSeq: 11,
		LoadModelVersion: 1,
	}})
	if nr.NodeReg == nil || nr.NodeReg.NodeID != "n1" || nr.NodeReg.Labels["slot"] != "c01-s03" || nr.NodeReg.BuildCapacity.Mem != 8<<30 ||
		nr.NodeReg.EnrollmentID != "enrollment-1" || nr.NodeReg.NodeEpoch != 7 || nr.NodeReg.SessionSeq != 11 ||
		nr.NodeReg.LoadModelVersion != 1 {
		t.Fatalf("node_register round-trip: %+v", nr.NodeReg)
	}

	hello := roundTrip(t, &Msg{Type: TypeHello, Hello: &Hello{Version: Version, Redirect: &NodeLinkRedirect{Targets: []NodeLinkTarget{
		{MemberID: "registry-2", Endpoint: "127.0.0.1:7702"},
	}}}})
	if hello.Hello == nil || hello.Hello.Redirect == nil || len(hello.Hello.Redirect.Targets) != 1 || hello.Hello.Redirect.Targets[0].MemberID != "registry-2" {
		t.Fatalf("hello redirect round-trip: %+v", hello.Hello)
	}

	c := roundTrip(t, &Msg{Type: TypeCommand, Cmd: &Command{
		CmdID: "x1", Kind: CmdSandboxAdmitDispatch, SID: "s1", Group: "/c/p/a/g1", RouteKey: "u1:sess1",
		NormalizedDemand: []byte(`{"kind":"sandbox"}`), DispatchSpec: []byte(`{"template":"t1"}`),
		DemandDigest: "demand-digest", DispatchSpecDigest: "spec-digest", ProviderPolicy: "provider-v1/policy-v1",
		NodeEpoch: 7, SessionSeq: 11,
		RegistryGeneration: "generation-1", Binding: "keb1.opaque", BindingDigest: "binding-digest",
	}})
	if c.Cmd == nil || c.Cmd.Kind != CmdSandboxAdmitDispatch || c.Cmd.Group != "/c/p/a/g1" || c.Cmd.RouteKey != "u1:sess1" ||
		!bytes.Equal(c.Cmd.NormalizedDemand, []byte(`{"kind":"sandbox"}`)) || !bytes.Equal(c.Cmd.DispatchSpec, []byte(`{"template":"t1"}`)) ||
		c.Cmd.ProviderPolicy != "provider-v1/policy-v1" || c.Cmd.NodeEpoch != 7 || c.Cmd.SessionSeq != 11 ||
		c.Cmd.RegistryGeneration != "generation-1" || c.Cmd.BindingDigest != "binding-digest" {
		t.Fatalf("command round-trip: %+v", c.Cmd)
	}
	b := roundTrip(t, &Msg{Type: TypeCommand, Cmd: &Command{
		CmdID: "b1", Kind: CmdBuildAdmitDispatch, BuildID: "build-1", NodeEpoch: 7, SessionSeq: 11,
	}})
	if b.Cmd == nil || b.Cmd.BuildID != "build-1" || b.Cmd.Kind != CmdBuildAdmitDispatch {
		t.Fatalf("build dispatch round-trip: %+v", b.Cmd)
	}

	// Sandbox routes carry runtime state only; the node-link owner supplies cluster identity.
	r := roundTrip(t, &Msg{Type: TypeUpsert, Route: &RouteEntry{
		SandboxID: "s1", State: StateRunning,
		FloatingIP: "100.100.96.5", AccessToken: "tok", NodeID: "n1", NodeEpoch: 7,
		RegistryGeneration: "generation-1", BindingDigest: "binding-digest", EventSeq: 3,
	}})
	if r.Route == nil || r.Route.SandboxID != "s1" || r.Route.State != StateRunning ||
		r.Route.NodeEpoch != 7 || r.Route.BindingDigest != "binding-digest" || r.Route.EventSeq != 3 {
		t.Fatalf("sandbox route round-trip: %+v", r.Route)
	}

	a := roundTrip(t, &Msg{Type: TypeCmdAck, Ack: &CmdAck{CmdID: "x1", Status: AckAccepted}})
	if a.Ack == nil || a.Ack.Status != AckAccepted {
		t.Fatalf("cmd_ack round-trip: %+v", a.Ack)
	}

	eventAck := roundTrip(t, &Msg{Type: TypeEventAck, EventAck: &EventAck{
		ObjectKind: "sandbox", ObjectID: "sandbox-1", RegistryGeneration: "generation-1",
		BindingDigest: strings.Repeat("a", 64), EventSeq: 4,
	}})
	if eventAck.EventAck == nil || eventAck.EventAck.ObjectID != "sandbox-1" ||
		eventAck.EventAck.RegistryGeneration != "generation-1" || eventAck.EventAck.EventSeq != 4 {
		t.Fatalf("event ack round-trip: %+v", eventAck.EventAck)
	}

	load := &PlacementLoadSnapshot{
		NodeID: "n1", NodeEpoch: 7, SessionSeq: 11, DataEndpoint: "10.0.0.1:8443",
		SampleSeq: 3, LoadModelVersion: 1, SandboxSlotCapacity: 10,
		EmergencyReservedMemory: 512 << 20,
		SandboxQueueLimit:       20, SandboxRateTokenAvailable: true,
	}
	l := roundTrip(t, &Msg{Type: TypePlacementLoad, Load: load})
	if l.Load == nil || *l.Load != *load {
		t.Fatalf("placement snapshot round-trip: %+v", l.Load)
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
