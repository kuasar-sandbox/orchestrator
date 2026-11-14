package workspace

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/http"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
)

// PublicMetadata projects records written by older nodes before exposing them.
// An unreadable workspace namespace is omitted, so none of its raw input leaks.
func PublicMetadata(meta map[string]string) map[string]string {
	raw, ok := meta[Namespace]
	if !ok {
		return meta
	}
	clean := maps.Clone(meta)
	// Older nodes persisted arbitrary metadata values, including disk formats
	// unknown to this node. Decode only the topology, then validate it.
	var topology []struct {
		Name   string  `json:"name"`
		Mounts []Mount `json:"mounts,omitempty"`
	}
	var entries []Entry
	err := strictjson.DecodeAllowUnknown([]byte(raw), &topology)
	if err == nil && topology != nil {
		projected, _ := json.Marshal(topology)
		entries, err = Parse(string(projected))
	}
	if err != nil || topology == nil {
		delete(clean, Namespace)
	} else {
		clean[Namespace] = TopologyJSON(entries)
	}
	return clean
}

// WithHeader applies whole-value precedence before parsing either carrier.
func WithHeader(meta map[string]string, headers http.Header) (map[string]string, error) {
	values, present := headers[http.CanonicalHeaderKey(Header)]
	if !present {
		return meta, nil
	}
	if len(values) != 1 || values[0] == "" {
		return nil, errors.New("workspace header must contain one JSON array")
	}
	out := maps.Clone(meta)
	if out == nil {
		out = make(map[string]string)
	}
	out[Namespace] = values[0]
	return out, nil
}

// CheckEnvelope verifies that a trusted transport carries only topology in
// public config and binds its confidential selectors to that exact declaration.
func CheckEnvelope(meta map[string]string, entries []Entry) error {
	raw, ok := meta[Namespace]
	if !ok && len(entries) == 0 {
		return nil
	}
	if !ok {
		return errors.New("workspace sources require matching topology")
	}
	public, err := Parse(raw)
	if err != nil {
		return errors.New("invalid workspace topology")
	}
	for _, e := range public {
		if e.Slot != nil || len(e.Disk) != 0 {
			return errors.New("workspace sources must use the private envelope")
		}
	}
	if entries == nil {
		return nil
	}
	encoded, err := json.Marshal(entries)
	if err != nil {
		return errors.New("invalid workspace source envelope")
	}
	parsed, err := Parse(string(encoded))
	if err != nil || TopologyJSON(public) != TopologyJSON(parsed) {
		return errors.New("workspace source envelope does not match topology")
	}
	return nil
}

// InputDigest pins a reservation without replicating a disk reference or token.
func InputDigest(entries []Entry) (string, error) {
	if len(entries) == 0 {
		return "", nil
	}
	raw, err := json.Marshal(entries)
	if err != nil {
		return "", err
	}
	// RawMessage preserves the caller's object ordering. Normalize before
	// comparing retries, preserving integer sizes beyond float64 precision.
	var value any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&value); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}
