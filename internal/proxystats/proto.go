// Package proxystats owns final-node proxy traffic accounting and the
// worker-to-master absolute snapshot protocol.
package proxystats

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	Version           = 1
	MaxFrameBytes     = 1 << 20
	MaxFrameSandboxes = 256
	MaxCounters       = 256
	MaxSandboxIDBytes = 256
	MaxWorkerIDBytes  = 128
)

const (
	TypeHello   = "hello"
	TypeUpdate  = "update"
	TypeRemove  = "remove"
	TypeReady   = "ready"
	TypeGoodbye = "goodbye"
)

type Frame struct {
	Type     string `json:"type"`
	Version  int    `json:"version"`
	WorkerID string `json:"workerID,omitempty"`
	Epoch    uint64 `json:"epoch"`
	Sequence uint64 `json:"seq,omitempty"`

	Counters   map[string]uint64 `json:"counters,omitempty"`
	Traffic    []SandboxSnapshot `json:"traffic,omitempty"`
	SandboxIDs []string          `json:"sandboxIDs,omitempty"`
}

type SandboxSnapshot struct {
	SandboxID string                     `json:"sandboxID"`
	Services  map[string]ServiceSnapshot `json:"services"`
}

type ServiceSnapshot struct {
	Parking         uint64     `json:"parking"`
	Egress          uint64     `json:"egress"`
	IdleSince       *time.Time `json:"idleSince,omitempty"`
	IdleSinceBootNS int64      `json:"idleSinceBootNS,omitempty"`
}

func WriteFrame(w io.Writer, frame Frame) error {
	if err := validateFrame(frame); err != nil {
		return err
	}
	payload, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > MaxFrameBytes {
		return fmt.Errorf("proxystats: frame size %d is invalid", len(payload))
	}
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeFrameBytes(w, header[:]); err != nil {
		return err
	}
	return writeFrameBytes(w, payload)
}

func writeFrameBytes(w io.Writer, payload []byte) error {
	for len(payload) > 0 {
		n, err := w.Write(payload)
		if n < 0 || n > len(payload) {
			return io.ErrShortWrite
		}
		payload = payload[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func ReadFrame(r io.Reader) (Frame, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}
	size := binary.LittleEndian.Uint32(header[:])
	if size == 0 || size > MaxFrameBytes {
		return Frame{}, fmt.Errorf("proxystats: frame size %d is invalid", size)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Frame{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var frame Frame
	if err := decoder.Decode(&frame); err != nil {
		return Frame{}, fmt.Errorf("proxystats: decode frame: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Frame{}, errors.New("proxystats: trailing frame data")
		}
		return Frame{}, fmt.Errorf("proxystats: trailing frame data: %w", err)
	}
	if err := validateFrame(frame); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func validateFrame(frame Frame) error {
	if frame.Version != Version {
		return fmt.Errorf("proxystats: unsupported version %d", frame.Version)
	}
	if frame.Epoch == 0 {
		return errors.New("proxystats: epoch is required")
	}
	if len(frame.WorkerID) > MaxWorkerIDBytes || strings.ContainsAny(frame.WorkerID, "\r\n\x00") {
		return errors.New("proxystats: invalid worker id")
	}
	switch frame.Type {
	case TypeHello:
		if frame.WorkerID == "" || frame.Sequence != 0 || len(frame.Counters) != 0 || len(frame.Traffic) != 0 || len(frame.SandboxIDs) != 0 {
			return errors.New("proxystats: invalid hello frame")
		}
	case TypeReady, TypeGoodbye:
		if frame.WorkerID != "" || frame.Sequence == 0 || len(frame.Counters) != 0 || len(frame.Traffic) != 0 || len(frame.SandboxIDs) != 0 {
			return fmt.Errorf("proxystats: invalid %s frame", frame.Type)
		}
	case TypeUpdate:
		if frame.WorkerID != "" || frame.Sequence == 0 || len(frame.SandboxIDs) != 0 ||
			(len(frame.Counters) == 0 && len(frame.Traffic) == 0) {
			return errors.New("proxystats: invalid update frame")
		}
		if frame.Counters != nil && len(frame.Counters) == 0 {
			return errors.New("proxystats: empty counter snapshot")
		}
		if len(frame.Counters) > MaxCounters || len(frame.Traffic) > MaxFrameSandboxes {
			return errors.New("proxystats: update exceeds item limit")
		}
		for name := range frame.Counters {
			if !validCounterName(name) {
				return fmt.Errorf("proxystats: invalid counter name")
			}
		}
		seen := make(map[string]struct{}, len(frame.Traffic))
		for _, snapshot := range frame.Traffic {
			if err := validateSandboxSnapshot(snapshot); err != nil {
				return err
			}
			if _, duplicate := seen[snapshot.SandboxID]; duplicate {
				return fmt.Errorf("proxystats: duplicate sandbox %q", snapshot.SandboxID)
			}
			seen[snapshot.SandboxID] = struct{}{}
		}
	case TypeRemove:
		if frame.WorkerID != "" || frame.Sequence == 0 || len(frame.Counters) != 0 || len(frame.Traffic) != 0 ||
			len(frame.SandboxIDs) == 0 || len(frame.SandboxIDs) > MaxFrameSandboxes {
			return errors.New("proxystats: invalid remove frame")
		}
		seen := make(map[string]struct{}, len(frame.SandboxIDs))
		for _, sid := range frame.SandboxIDs {
			if !validSandboxID(sid) {
				return errors.New("proxystats: invalid sandbox id")
			}
			if _, duplicate := seen[sid]; duplicate {
				return fmt.Errorf("proxystats: duplicate sandbox %q", sid)
			}
			seen[sid] = struct{}{}
		}
	default:
		return fmt.Errorf("proxystats: unknown frame type %q", frame.Type)
	}
	return nil
}

func validateSandboxSnapshot(snapshot SandboxSnapshot) error {
	if !validSandboxID(snapshot.SandboxID) || len(snapshot.Services) == 0 || len(snapshot.Services) > len(allServices) {
		return errors.New("proxystats: invalid sandbox traffic snapshot")
	}
	for service, state := range snapshot.Services {
		if !validService(service) {
			return fmt.Errorf("proxystats: invalid service %q", service)
		}
		if (state.Parking != 0 || state.Egress != 0) && (state.IdleSince != nil || state.IdleSinceBootNS != 0) {
			return fmt.Errorf("proxystats: busy service %q carries idle time", service)
		}
		if (state.IdleSince == nil) != (state.IdleSinceBootNS == 0) {
			return fmt.Errorf("proxystats: incomplete idle time for service %q", service)
		}
		if state.IdleSince != nil && (state.IdleSince.IsZero() || state.IdleSinceBootNS <= 0) {
			return fmt.Errorf("proxystats: invalid idle time for service %q", service)
		}
	}
	return nil
}

func validCounterName(name string) bool {
	return name != "" && len(name) <= 1024 && !strings.ContainsAny(name, "\r\n\x00")
}

func validSandboxID(sid string) bool {
	return sid != "" && len(sid) <= MaxSandboxIDBytes && !strings.ContainsAny(sid, "\r\n\x00")
}
