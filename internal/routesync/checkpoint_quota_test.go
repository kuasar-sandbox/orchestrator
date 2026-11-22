package routesync

import (
	"bytes"
	"testing"
)

func TestCheckpointQuotaProtocolDoesNotDowngrade(t *testing.T) {
	if err := ValidateHello(&Msg{Type: TypeHello, Hello: &Hello{Version: 11}}); err == nil {
		t.Fatal("pre-quota peer was accepted")
	}
	quota := 3
	message := &Msg{Type: TypeCommand, Cmd: &Command{Kind: CmdCreate, MaxCheckpoints: &quota}}
	var wire bytes.Buffer
	if err := WriteMsg(&wire, message); err != nil {
		t.Fatal(err)
	}
	decoded, err := ReadMsg(&wire)
	if err != nil || decoded.Cmd.MaxCheckpoints == nil || *decoded.Cmd.MaxCheckpoints != 3 {
		t.Fatalf("quota wire: %+v %v", decoded, err)
	}
	message.Cmd.Kind = CmdConnect
	if err := ValidateMessage(message); err == nil {
		t.Fatal("quota leaked into another lifecycle operation")
	}
	message.Cmd.Kind = CmdCreate
	quota = 0
	if err := ValidateMessage(message); err == nil {
		t.Fatal("explicit zero quota accepted")
	}
}
