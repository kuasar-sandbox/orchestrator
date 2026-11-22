package record

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/storage"
)

// Version orders revisions of one logical key within one database namespace,
// including delete/recreate. It is not a global commit sequence. The 20-hex wire
// representation is retained; its bytes no longer expose native FDB stamps.
type Version [10]byte

func (v Version) IsZero() bool { return v == Version{} }
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
func (v Version) Compare(other Version) int    { return bytes.Compare(v[:], other[:]) }
func (v Version) String() string               { return hex.EncodeToString(v[:]) }
func (v Version) MarshalText() ([]byte, error) { return []byte(v.String()), nil }
func (v Version) MarshalJSON() ([]byte, error) { return json.Marshal(v.String()) }
func (v *Version) UnmarshalText(text []byte) error {
	if len(text) != 20 {
		return errors.New("record: version must contain 20 hexadecimal digits")
	}
	var next Version
	if _, err := hex.Decode(next[:], text); err != nil {
		return err
	}
	if !next.IsZero() && !next.IsCommitted() {
		return errors.New("record: incomplete version")
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
func (v Version) next() (Version, error) {
	for i := len(v) - 1; i >= 0; i-- {
		v[i]++
		if v[i] != 0 {
			if v.IsCommitted() {
				return v, nil
			}
			break
		}
	}
	return Version{}, &storage.CapacityError{Resource: "record revision exhausted"}
}
