package proxystats

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

func TestFrameRoundTripAndStrictBounds(t *testing.T) {
	frame := Frame{
		Type: TypeUpdate, Version: Version, Epoch: 7, Sequence: 2,
		Counters: map[string]uint64{"requests_total": 4},
		Traffic:  []SandboxSnapshot{{SandboxID: "s1", Services: map[string]ServiceSnapshot{"forward": {Connected: 1}}}},
	}
	var wire bytes.Buffer
	if err := WriteFrame(&wire, frame); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(&wire)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, frame) {
		t.Fatalf("round trip = %#v, want %#v", got, frame)
	}
	bad := frame
	bad.Traffic[0].SandboxID = string(bytes.Repeat([]byte{'x'}, MaxSandboxIDBytes+1))
	if err := WriteFrame(&bytes.Buffer{}, bad); err == nil {
		t.Fatal("oversized sandbox id was accepted")
	}
}

type shortFrameWriter struct {
	bytes.Buffer
	max int
}

func (w *shortFrameWriter) Write(payload []byte) (int, error) {
	if len(payload) > w.max {
		payload = payload[:w.max]
	}
	return w.Buffer.Write(payload)
}

func TestWriteFrameCompletesShortWrites(t *testing.T) {
	frame := Frame{Type: TypeReady, Version: Version, Epoch: 3, Sequence: 1}
	wire := &shortFrameWriter{max: 3}
	if err := WriteFrame(wire, frame); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadFrame(&wire.Buffer); err != nil || !reflect.DeepEqual(got, frame) {
		t.Fatalf("short-write round trip = %#v err=%v", got, err)
	}
	if err := writeFrameBytes(zeroWriter{}, []byte("x")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero-progress write error = %v, want io.ErrShortWrite", err)
	}
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

func TestFrameRejectsMalformedProtocolState(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name  string
		frame Frame
	}{
		{
			name:  "sequence zero",
			frame: Frame{Type: TypeUpdate, Version: Version, Epoch: 1, Counters: map[string]uint64{"requests_total": 1}},
		},
		{
			name: "duplicate sandbox",
			frame: Frame{Type: TypeUpdate, Version: Version, Epoch: 1, Sequence: 2, Traffic: []SandboxSnapshot{
				{SandboxID: "s1", Services: map[string]ServiceSnapshot{"forward": {Connected: 1}}},
				{SandboxID: "s1", Services: map[string]ServiceSnapshot{"exec": {Connected: 1}}},
			}},
		},
		{
			name: "busy idle timestamp",
			frame: Frame{Type: TypeUpdate, Version: Version, Epoch: 1, Sequence: 2, Traffic: []SandboxSnapshot{{
				SandboxID: "s1", Services: map[string]ServiceSnapshot{"forward": {Connected: 1, IdleSince: &now, IdleSinceBootNS: 1}},
			}}},
		},
		{
			name: "zero idle timestamp",
			frame: Frame{Type: TypeUpdate, Version: Version, Epoch: 1, Sequence: 2, Traffic: []SandboxSnapshot{{
				SandboxID: "s1", Services: map[string]ServiceSnapshot{"forward": {IdleSince: new(time.Time), IdleSinceBootNS: 1}},
			}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := WriteFrame(&bytes.Buffer{}, test.frame); err == nil {
				t.Fatal("malformed frame was accepted")
			}
		})
	}

	payload := []byte(`{"type":"hello","version":1,"workerID":"w0","epoch":1,"unexpected":true}`)
	var wire bytes.Buffer
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(payload)))
	wire.Write(header[:])
	wire.Write(payload)
	if _, err := ReadFrame(&wire); err == nil {
		t.Fatal("unknown frame field was accepted")
	}
}

func TestFrameRejectsOldEgressField(t *testing.T) {
	payload := []byte(`{"type":"update","version":1,"epoch":1,"seq":2,"traffic":[{"sandboxID":"s1","services":{"forward":{"parking":0,"egress":1}}}]}`)
	var wire bytes.Buffer
	if err := binary.Write(&wire, binary.LittleEndian, uint32(len(payload))); err != nil {
		t.Fatal(err)
	}
	wire.Write(payload)
	if _, err := ReadFrame(&wire); err == nil {
		t.Fatal("old service egress field silently decoded as connected zero")
	}
}
