package metadata

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
)

// Version is the complete committed transaction version, including the two-byte
// batch order. Zero denotes absence, never a reusable historical revision.
type Version [10]byte

func (v Version) IsZero() bool { return v == Version{} }

// IsCommitted excludes both absence and the native incomplete-stamp placeholder.
func (v Version) IsCommitted() bool {
	if v.IsZero() {
		return false
	}
	for _, b := range v {
		if b != 0xff {
			return true
		}
	}
	return false
}

// Compare preserves Router cache ordering for one record in the same database
// history. It does not order business generations or independent databases.
func (v Version) Compare(other Version) int    { return bytes.Compare(v[:], other[:]) }
func (v Version) String() string               { return hex.EncodeToString(v[:]) }
func (v Version) MarshalText() ([]byte, error) { return []byte(v.String()), nil }
func (v Version) MarshalJSON() ([]byte, error) { return json.Marshal(v.String()) }

func (v *Version) UnmarshalText(text []byte) error {
	if len(text) != 20 {
		return errors.New("metadata: version must contain 20 hexadecimal digits")
	}
	var next Version
	if _, err := hex.Decode(next[:], text); err != nil {
		return err
	}
	if !next.IsZero() && !next.IsCommitted() {
		return errors.New("metadata: incomplete version")
	}
	*v = next
	return nil
}
func (v *Version) UnmarshalJSON(raw []byte) error {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return err
	}
	return v.UnmarshalText([]byte(text))
}

type Versioned[T any] struct {
	Exists  bool
	Version Version
	Value   T
}
