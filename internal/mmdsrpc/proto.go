// Package mmdsrpc implements the bounded, process-local proxy master/worker
// protocol used to resolve MMDS routes kept outside the fixed-layout SHM table.
package mmdsrpc

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

const maxFrame = 1 << 20

type EndpointRequest struct {
	RequestID uint64 `json:"id"`
	SandboxID string `json:"sid"`
	Path      string `json:"path"`
}

// EndpointResponse never carries a provider URL. ServiceSocket is an already
// parsed absolute Unix socket path selected from conductor policy by the master.
type EndpointResponse struct {
	RequestID     uint64 `json:"id"`
	Found         bool   `json:"found"`
	Unavailable   bool   `json:"unavailable,omitempty"`
	Type          string `json:"type,omitempty"`
	ContentType   string `json:"content_type,omitempty"`
	Body          []byte `json:"body,omitempty"`
	Present       bool   `json:"present,omitempty"`
	Service       string `json:"service,omitempty"`
	ServiceSocket string `json:"service_socket,omitempty"`
}

func writeFrame(w io.Writer, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(body) == 0 || len(body) > maxFrame {
		return errors.New("mmdsrpc: message too large")
	}
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(body)))
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, body)
}

func writeAll(w io.Writer, body []byte) error {
	for len(body) != 0 {
		n, err := w.Write(body)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		body = body[n:]
	}
	return nil
}

func readFrame(r io.Reader, value any) error {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	size := binary.LittleEndian.Uint32(header[:])
	if size == 0 || size > maxFrame {
		return errors.New("mmdsrpc: bad frame length")
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return err
	}
	return json.Unmarshal(body, value)
}
