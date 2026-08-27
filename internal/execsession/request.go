// Package execsession defines the shared control-plane request contract for
// creating an exec session. Node and cluster handlers use the same bounded,
// complete-body decoder so their direct and proxied APIs cannot drift.
package execsession

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/kuasar-sandbox/orchestrator/internal/execadmission/limits"
)

const MaxRequestBodyBytes int64 = 64 << 10

var (
	ErrRequestTooLarge = errors.New("exec session request body is too large")
	ErrInvalidRequest  = errors.New("invalid exec session request body")
)

type Request struct {
	TTLSeconds int64       `json:"ttlSeconds,omitempty"`
	Conditions []Condition `json:"conditions,omitempty"`
}

type Condition struct {
	Expr string `json:"expr"`
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

	decoder := json.NewDecoder(bytes.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return Request{}, ErrInvalidRequest
	}

	var request Request
	seenTTL := false
	seenConditions := false
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return Request{}, ErrInvalidRequest
		}
		switch key {
		case "ttlSeconds":
			if seenTTL {
				return Request{}, ErrInvalidRequest
			}
			seenTTL = true
			var value json.RawMessage
			if err := decoder.Decode(&value); err != nil ||
				bytes.Equal(bytes.TrimSpace(value), []byte("null")) ||
				json.Unmarshal(value, &request.TTLSeconds) != nil || request.TTLSeconds < 0 {
				return Request{}, ErrInvalidRequest
			}
		case "conditions":
			if seenConditions {
				return Request{}, ErrInvalidRequest
			}
			seenConditions = true
			conditions, err := decodeConditions(decoder)
			if err != nil {
				return Request{}, ErrInvalidRequest
			}
			request.Conditions = conditions
		default:
			return Request{}, ErrInvalidRequest
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return Request{}, ErrInvalidRequest
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Request{}, ErrInvalidRequest
	}
	return request, nil
}

func decodeConditions(decoder *json.Decoder) ([]Condition, error) {
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('[') {
		return nil, ErrInvalidRequest
	}
	conditions := make([]Condition, 0)
	expressions := make([]string, 0)
	for decoder.More() {
		opening, err := decoder.Token()
		if err != nil || opening != json.Delim('{') {
			return nil, ErrInvalidRequest
		}
		seenExpr := false
		var condition Condition
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil || key != "expr" || seenExpr {
				return nil, ErrInvalidRequest
			}
			seenExpr = true
			var value json.RawMessage
			if err := decoder.Decode(&value); err != nil ||
				bytes.Equal(bytes.TrimSpace(value), []byte("null")) ||
				json.Unmarshal(value, &condition.Expr) != nil {
				return nil, ErrInvalidRequest
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') || !seenExpr {
			return nil, ErrInvalidRequest
		}
		conditions = append(conditions, condition)
		expressions = append(expressions, condition.Expr)
		if len(conditions) > limits.MaxConditions {
			return nil, ErrInvalidRequest
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim(']') ||
		limits.ValidateExpressions(expressions) != nil {
		return nil, ErrInvalidRequest
	}
	if len(conditions) == 0 {
		return nil, nil
	}
	return conditions, nil
}

// Expressions returns a defensive ordered copy of condition sources.
func (r Request) Expressions() []string {
	if len(r.Conditions) == 0 {
		return nil
	}
	out := make([]string, len(r.Conditions))
	for i := range r.Conditions {
		out[i] = r.Conditions[i].Expr
	}
	return out
}
