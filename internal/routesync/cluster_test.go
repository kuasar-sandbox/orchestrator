package routesync

import (
	"bytes"
	"testing"
)

func roundTrip(t *testing.T, m *Msg) *Msg {
	t.Helper()
	var buf bytes.Buffer
	if err := writeMsg(&buf, m); err != nil {
		t.Fatalf("writeMsg: %v", err)
	}
	got, err := readMsg(&buf)
	if err != nil {
		t.Fatalf("readMsg: %v", err)
	}
	return got
}

func TestNodeLinkCodecRoundTrip(t *testing.T) {
	nr := roundTrip(t, &Msg{Type: TypeNodeRegister, NodeReg: &NodeRegister{
		NodeID: "n1", Labels: map[string]string{"zone": "z1", "slot": "c01-s03"},
		BuildCapacity: &BuildResources{CPU: 4000, Mem: 8 << 30, Storage: 64 << 30},
		DataEndpoint:  "10.0.0.1:8443",
	}})
	if nr.NodeReg == nil || nr.NodeReg.NodeID != "n1" || nr.NodeReg.Labels["slot"] != "c01-s03" || nr.NodeReg.BuildCapacity.Mem != 8<<30 {
		t.Fatalf("node_register round-trip: %+v", nr.NodeReg)
	}

	c := roundTrip(t, &Msg{Type: TypeCommand, Rev: 42, Cmd: &Command{
		CmdID: "x1", Kind: CmdCreate, SID: "s1", Group: "/c/p/a/g1", RouteKey: "u1:sess1",
		TemplateRef: "manifest://abc", KeyFingerprint: "e2b_deadbeef",
	}})
	if c.Cmd == nil || c.Cmd.Kind != CmdCreate || c.Cmd.Group != "/c/p/a/g1" || c.Rev != 42 {
		t.Fatalf("command round-trip: %+v rev=%d", c.Cmd, c.Rev)
	}

	// a sandbox route reuses RouteEntry with the cluster fields set
	r := roundTrip(t, &Msg{Type: TypeUpsert, Route: &RouteEntry{
		SandboxID: "s1", State: StateRunning, Group: "/c/p/a/g1", RouteKey: "u1:sess1",
		FloatingIP: "100.100.96.5", AccessToken: "tok",
	}})
	if r.Route == nil || r.Route.Group != "/c/p/a/g1" || r.Route.RouteKey != "u1:sess1" {
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
		Subscribe: &Subscribe{Kind: KindRegistry}, ResumeFrom: 100,
	}})
	if reg.Register == nil || reg.Register.Subscribe.Kind != KindRegistry || reg.Register.ResumeFrom != 100 {
		t.Fatalf("registry register round-trip: %+v", reg.Register)
	}
}
