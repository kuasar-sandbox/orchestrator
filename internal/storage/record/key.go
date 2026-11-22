// Package record provides the shared record codec, revisions and conditional
// transactions. It contains no cluster business types or backend dependencies.
package record

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

// Key is a canonical sequence of length-delimited logical components. Equality
// components longer than 256 bytes get a bounded physical representation. Scope
// prefixes remain prefixes; original keys are persisted and checked for collisions.
// Component order is not a user-visible sort contract. Explicit ordered fields
// must use fixed-width order-preserving bytes, never a hashed long identifier.
type Key []byte

func NewKey(parts ...string) Key {
	var key Key
	for _, part := range parts {
		key = key.Child(part)
	}
	return key
}
func (k Key) Child(part string) Key {
	out := bytes.Clone(k)
	out = binary.BigEndian.AppendUint32(out, uint32(len(part)))
	return append(out, part...)
}
func physical(key Key) ([]byte, error) {
	if len(key) == 0 || len(key) > 9000 {
		return nil, fmt.Errorf("%w: logical key length", storage.ErrInvalid)
	}
	out := []byte{'r'}
	for rest := []byte(key); len(rest) > 0; {
		if len(rest) < 4 {
			return nil, fmt.Errorf("%w: logical key encoding", storage.ErrInvalid)
		}
		n := int(binary.BigEndian.Uint32(rest[:4]))
		rest = rest[4:]
		if n > len(rest) {
			return nil, fmt.Errorf("%w: logical key component", storage.ErrInvalid)
		}
		part := rest[:n]
		rest = rest[n:]
		if n <= 256 {
			out = append(out, 0)
			out = binary.BigEndian.AppendUint16(out, uint16(n))
			out = append(out, part...)
		} else {
			hash := sha256.Sum256(part)
			out = append(out, 1)
			out = append(out, hash[:]...)
		}
	}
	if err := storage.ValidateKey(out); err != nil {
		return nil, err
	}
	return out, nil
}
func revisionKey(key []byte) []byte { out := bytes.Clone(key); out[0] = 'v'; return out }
func chunkPrefix(key []byte) []byte {
	hash := sha256.Sum256(key)
	return append([]byte{'c'}, hash[:]...)
}
func chunkKey(prefix []byte, index uint32) []byte {
	return binary.BigEndian.AppendUint32(bytes.Clone(prefix), index)
}

func decodeCounter(raw storage.Value, key Key) (Version, error) {
	var v Version
	if !raw.Exists {
		return v, nil
	}
	if len(raw.Bytes) < 10 || !bytes.Equal(raw.Bytes[10:], key) {
		return v, fmt.Errorf("%w: revision identity", storage.ErrInvalid)
	}
	copy(v[:], raw.Bytes[:10])
	if !v.IsCommitted() {
		return Version{}, fmt.Errorf("%w: revision value", storage.ErrInvalid)
	}
	return v, nil
}
