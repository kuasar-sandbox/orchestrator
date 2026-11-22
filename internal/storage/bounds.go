package storage

import (
	"bytes"
	"fmt"
)

const (
	MaxKeyBytes   = 2048
	MaxValueBytes = 100_000
	// Logical work budget; drivers also enforce native limits, including FDB's
	// namespace and conflict-range overhead, before attempting a commit.
	MaxTransactionBytes = 9_000_000
	MaxPageRows         = 1024
	MaxPageBytes        = 2 << 20
)

func ValidateKey(key []byte) error {
	if len(key) == 0 || len(key) > MaxKeyBytes {
		return fmt.Errorf("%w: key length", ErrInvalid)
	}
	return nil
}
func ValidateRange(r Range, limit Limit) error {
	if len(r.Begin) > MaxKeyBytes || len(r.End) > MaxKeyBytes || len(r.After) > MaxKeyBytes ||
		(r.End != nil && bytes.Compare(r.Begin, r.End) >= 0) ||
		(r.After != nil && (bytes.Compare(r.After, r.Begin) < 0 || (r.End != nil && bytes.Compare(r.After, r.End) >= 0))) ||
		limit.Rows < 1 || limit.Rows > MaxPageRows || limit.Bytes < 1 || limit.Bytes > MaxPageBytes {
		return fmt.Errorf("%w: range or page limit", ErrInvalid)
	}
	return nil
}

// PrefixEnd handles arbitrary binary prefixes, including trailing 0xff bytes.
// A nil result means the range extends to the end of the keyspace.
func PrefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
func Equal(a, b Value) bool { return a.Exists == b.Exists && bytes.Equal(a.Bytes, b.Bytes) }
func Clone(v Value) Value   { return Value{Exists: v.Exists, Bytes: bytes.Clone(v.Bytes)} }

type Meter struct {
	bytes int
	err   error
}

func (m *Meter) Add(n int) error {
	if m.err != nil {
		return m.err
	}
	m.bytes += n
	if m.bytes >= MaxTransactionBytes {
		m.err = &CapacityError{Resource: "transaction bytes"}
	}
	return m.err
}
func (m *Meter) Err() error { return m.err }
