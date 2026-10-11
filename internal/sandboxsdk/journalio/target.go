// Mirrored from sandboxer/internal/journalio at 509cf54032dc34157b2e54effa089fb5aff19763 (Apache-2.0), for runner diagnostics.
// Package journalio parses explicit journal targets and frames output streams.
// It has no knowledge of the caller's workload or identity model.
package journalio

import (
	"fmt"
	"net/url"
	"strings"
)

const (
	Prefix = "journald="
	// MaxTargetBytes bounds one encoded CLI target, including its identifier.
	MaxTargetBytes = 64 << 10
	MaxFields      = 64
)

// Target is the configuration of one output. Fields never inherit from another
// target or the process environment. New copies Fields before using them.
type Target struct {
	Tag    string
	Fields map[string]string
}

// Parse accepts journald=TAG[,FIELD=VALUE...]. It splits before unescaping values
// once, so encoded separators remain data and '+' is not converted to a space.
func Parse(raw string) (Target, error) {
	if len(raw) > MaxTargetBytes {
		return Target{}, fmt.Errorf("journal target exceeds %d bytes", MaxTargetBytes)
	}
	value, ok := strings.CutPrefix(raw, Prefix)
	if !ok {
		return Target{}, fmt.Errorf("journal target must start with %s", Prefix)
	}
	parts := strings.Split(value, ",")
	if len(parts)-1 > MaxFields {
		return Target{}, fmt.Errorf("journal target exceeds %d fields", MaxFields)
	}
	target := Target{Tag: parts[0]}
	if len(parts) > 1 {
		target.Fields = make(map[string]string, len(parts)-1)
	}
	for _, part := range parts[1:] {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return Target{}, fmt.Errorf("journal field requires FIELD=VALUE")
		}
		if err := validField(key); err != nil {
			return Target{}, err
		}
		if _, exists := target.Fields[key]; exists {
			return Target{}, fmt.Errorf("duplicate journal field %q", key)
		}
		decoded, err := url.PathUnescape(value)
		if err != nil {
			return Target{}, fmt.Errorf("journal field %q has invalid percent encoding", key)
		}
		target.Fields[key] = decoded
	}
	if err := target.validate(); err != nil {
		return Target{}, err
	}
	return target, nil
}

func (t Target) validate() error {
	if t.Tag == "" {
		return fmt.Errorf("journald= requires a tag")
	}
	for _, c := range t.Tag {
		if !(c == '-' || c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
			return fmt.Errorf("journal tag must contain only [A-Za-z0-9_-]")
		}
	}
	if len(t.Fields) > MaxFields {
		return fmt.Errorf("journal target exceeds %d fields", MaxFields)
	}
	size := len(Prefix) + len(t.Tag)
	for key, value := range t.Fields {
		if err := validField(key); err != nil {
			return err
		}
		// Also bound callers constructing a Target directly. The parser has
		// already checked the (possibly longer) encoded CLI representation.
		if len(value) > MaxTargetBytes || size > MaxTargetBytes-len(key)-len(value)-2 {
			return fmt.Errorf("journal target exceeds %d bytes", MaxTargetBytes)
		}
		size += len(key) + len(value) + 2
	}
	if size > MaxTargetBytes {
		return fmt.Errorf("journal target exceeds %d bytes", MaxTargetBytes)
	}
	return nil
}

func validField(key string) error {
	if len(key) == 0 || len(key) > 64 || key[0] < 'A' || key[0] > 'Z' {
		return fmt.Errorf("journal field name must start with A-Z and be 1..64 bytes")
	}
	for _, c := range key {
		if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
			return fmt.Errorf("journal field %q must contain only [A-Z0-9_]", key)
		}
	}
	switch key {
	case "MESSAGE", "PRIORITY", "SYSLOG_IDENTIFIER":
		return fmt.Errorf("journal field %q is owned by the output writer", key)
	}
	return nil
}
