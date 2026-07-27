// Package execsession defines the shared control-plane request contract for
// creating an exec session. Node and cluster handlers use the same bounded,
// complete-body decoder so their direct and proxied APIs cannot drift.
package execsession

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

const MaxRequestBodyBytes int64 = 64 << 10

var (
	ErrRequestTooLarge = errors.New("exec session request body is too large")
	ErrInvalidRequest  = errors.New("invalid exec session request body")
)

type Request struct {
	TTLSeconds int64
}

// DecodeRequest reads and validates the complete raw request body. An actually
// empty body and an empty JSON object are equivalent. contentLength may be -1
// for chunked or HTTP/2 requests; the reader-side limit remains authoritative.
func DecodeRequest(body io.Reader, contentLength int64) (Request, error) {
	if contentLength > MaxRequestBodyBytes {
		return Request{}, ErrRequestTooLarge
	}
	if body == nil {
		return Request{}, nil
	}

	raw, err := io.ReadAll(io.LimitReader(body, MaxRequestBodyBytes+1))
	if err != nil {
		return Request{}, ErrInvalidRequest
	}
	if int64(len(raw)) > MaxRequestBodyBytes {
		return Request{}, ErrRequestTooLarge
	}
	if len(raw) == 0 {
		return Request{}, nil
	}

	var wire *struct {
		TTLSeconds json.RawMessage `json:"ttlSeconds"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil || wire == nil {
		return Request{}, ErrInvalidRequest
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Request{}, ErrInvalidRequest
	}

	var request Request
	if wire.TTLSeconds != nil {
		if bytes.Equal(bytes.TrimSpace(wire.TTLSeconds), []byte("null")) ||
			json.Unmarshal(wire.TTLSeconds, &request.TTLSeconds) != nil || request.TTLSeconds < 0 {
			return Request{}, ErrInvalidRequest
		}
	}
	return request, nil
}
