package nodectl

import (
	"bytes"
	"strings"
	"testing"
)

func TestMessageRoundTrip(t *testing.T) {
	in := &Message{
		Type:                TypeAdmit,
		SandboxID:           "sb-001",
		CapacityMemoryBytes: 8 << 30,
		CapacityCPU:         2,
		FloorMemoryBytes:    128 << 20,
		FloorCPU:            0.1,
		StartupBudgetMemory: 1 << 30,
		CgroupPath:          "/sys/fs/cgroup/sandboxes/sb-001",
	}
	var buf bytes.Buffer
	if err := WriteMessage(&buf, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadMessage(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if out.Type != in.Type ||
		out.SandboxID != in.SandboxID ||
		out.CapacityMemoryBytes != in.CapacityMemoryBytes ||
		out.CapacityCPU != in.CapacityCPU ||
		out.FloorCPU != in.FloorCPU ||
		out.StartupBudgetMemory != in.StartupBudgetMemory ||
		out.CgroupPath != in.CgroupPath {
		t.Errorf("round trip mismatch:\n in=%+v\nout=%+v", in, out)
	}
}

func TestReadMessage_TooLong(t *testing.T) {
	var buf bytes.Buffer
	// Write a length prefix that exceeds MaxMessageBytes.
	buf.Write([]byte{0xff, 0xff, 0xff, 0xff})
	_, err := ReadMessage(&buf)
	if err == nil {
		t.Fatal("expected error for oversized message")
	}
	if !strings.Contains(err.Error(), "max") {
		t.Errorf("error %q does not mention max limit", err.Error())
	}
}

func TestReadMessage_EOF(t *testing.T) {
	_, err := ReadMessage(bytes.NewReader(nil))
	if err == nil {
		t.Fatal("expected EOF")
	}
}
