package registry

import (
	"encoding/binary"
	"encoding/json"
	"io"
)

// writeFrame writes a length-prefixed JSON frame ([4B LE len][JSON]).
func writeFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}
