// Package framing implements the small length-prefixed JSON wire format
// ([4B LE len][json]) shared by internal/routesync (registry<->node route
// sync) and internal/mmdsrpc (proxy master<->worker MMDS resolution) --
// unrelated peer sets that happen to need the identical framing.
package framing

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ErrTooLarge is returned when an encoded message would exceed the caller's
// maxFrame.
var ErrTooLarge = errors.New("framing: message too large")

// EncodeChecked marshals v to JSON and checks it fits within maxFrame,
// without writing anything -- for a caller that wants to validate before
// committing state or dispatching, or that needs the encoded bytes before
// framing them (see WriteFrame).
func EncodeChecked(v any, maxFrame int) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(b) > maxFrame {
		return nil, ErrTooLarge
	}
	return b, nil
}

// WriteFrame writes b (already checked by the caller, e.g. via EncodeChecked)
// as one length-prefixed frame.
func WriteFrame(w io.Writer, b []byte) error {
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

// ReadFrame reads one length-prefixed frame, bounded by maxFrame, and returns
// its undecoded body. The caller unmarshals it (routesync and mmdsrpc apply
// their own semantic validation around that step, so decoding isn't done
// here).
func ReadFrame(r io.Reader, maxFrame int) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[:])
	if n == 0 || int(n) > maxFrame {
		return nil, fmt.Errorf("framing: bad frame length")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
