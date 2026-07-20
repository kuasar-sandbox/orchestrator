package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"reflect"
	"strings"
	"unicode/utf8"
)

const (
	NodeRequestEnvelopeVersionV1 uint16 = 1
	MaxNodeRequestBodyBytes             = 48 << 10
	MaxNodeRequestHeaderBytes           = 8 << 10
	MaxNodeRequestQueryBytes            = 8 << 10
	MaxNodeRequestPathBytes             = 2 << 10
)

// NodeRequestEnvelopeV1 is the complete, replayable node-facing portion of a
// create or Build registration request. Caller credentials and transport/fence
// headers are deliberately excluded before this value enters consensus state.
type NodeRequestEnvelopeV1 struct {
	Version  uint16              `json:"version"`
	Method   string              `json:"method"`
	Path     string              `json:"path"`
	RawQuery string              `json:"query,omitempty"`
	Header   map[string][]string `json:"headers,omitempty"`
	Body     json.RawMessage     `json:"body"`
}

// NewNodeRequestEnvelopeV1 canonicalizes one bounded JSON-object request and
// retains only ordinary end-to-end headers. It is the sole constructor used by
// Router before the request is placed in a dispatch intent.
func NewNodeRequestEnvelopeV1(method, path, rawQuery string, header http.Header, body []byte) (NodeRequestEnvelopeV1, error) {
	if len(body) == 0 {
		body = []byte("{}")
	}
	canonicalBody, err := CanonicalJSONObject(body)
	if err != nil {
		return NodeRequestEnvelopeV1{}, err
	}
	canonicalQuery, err := canonicalRawQuery(rawQuery)
	if err != nil {
		return NodeRequestEnvelopeV1{}, err
	}
	canonicalHeader, err := canonicalNodeRequestHeaders(header)
	if err != nil {
		return NodeRequestEnvelopeV1{}, err
	}
	envelope := NodeRequestEnvelopeV1{
		Version:  NodeRequestEnvelopeVersionV1,
		Method:   strings.ToUpper(method),
		Path:     path,
		RawQuery: canonicalQuery,
		Header:   canonicalHeader,
		Body:     canonicalBody,
	}
	return envelope, envelope.Validate()
}

func (e NodeRequestEnvelopeV1) Validate() error {
	if e.Version != NodeRequestEnvelopeVersionV1 || e.Method != http.MethodPost {
		return errors.New("cluster: node request envelope requires version 1 POST semantics")
	}
	if len(e.Path) == 0 || len(e.Path) > MaxNodeRequestPathBytes {
		return errors.New("cluster: node request envelope path has invalid size")
	}
	u, err := url.ParseRequestURI(e.Path)
	if err != nil || u.IsAbs() || u.RawQuery != "" || u.Fragment != "" || u.Path != e.Path {
		return errors.New("cluster: node request envelope path must be an absolute request path without query or fragment")
	}
	canonicalQuery, err := canonicalRawQuery(e.RawQuery)
	if err != nil || canonicalQuery != e.RawQuery {
		return errors.New("cluster: node request envelope query is not canonical")
	}
	canonicalHeader, err := canonicalNodeRequestHeaders(http.Header(e.Header))
	if err != nil || !reflect.DeepEqual(canonicalHeader, e.Header) {
		return errors.New("cluster: node request envelope headers contain non-canonical or protected fields")
	}
	canonicalBody, err := CanonicalJSONObject(e.Body)
	if err != nil || !bytes.Equal(canonicalBody, e.Body) {
		return errors.New("cluster: node request envelope body is not a canonical JSON object")
	}
	return nil
}

// CanonicalJSONObject rejects duplicate keys at every nesting level and emits a
// deterministic JSON object. This prevents aliases or extensions from changing
// meaning when an intent is replayed by a different leader.
func CanonicalJSONObject(encoded []byte) (json.RawMessage, error) {
	if !utf8.Valid(encoded) {
		return nil, errors.New("cluster: node request body is not valid UTF-8")
	}
	value, err := decodeSingleJSONValue(encoded)
	if err != nil {
		return nil, fmt.Errorf("cluster: invalid JSON request body: %w", err)
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, errors.New("cluster: node request body must be a JSON object")
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("cluster: canonicalize JSON request body: %w", err)
	}
	if len(canonical) > MaxNodeRequestBodyBytes {
		return nil, fmt.Errorf("cluster: node request body exceeds %d bytes", MaxNodeRequestBodyBytes)
	}
	return canonical, nil
}

// DecodeJSONObject returns canonical raw values so Router can inspect aliases,
// remove reserved metadata, and overwrite cluster-owned fields without dropping
// fields it does not understand.
func DecodeJSONObject(encoded []byte) (map[string]json.RawMessage, error) {
	canonical, err := CanonicalJSONObject(encoded)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(canonical, &fields); err != nil {
		return nil, fmt.Errorf("cluster: decode canonical JSON object: %w", err)
	}
	return fields, nil
}

// EncodeJSONObject canonicalizes a possibly modified field map.
func EncodeJSONObject(fields map[string]json.RawMessage) (json.RawMessage, error) {
	if fields == nil {
		fields = map[string]json.RawMessage{}
	}
	values := make(map[string]any, len(fields))
	for key, raw := range fields {
		if !utf8.ValidString(key) {
			return nil, errors.New("cluster: JSON object key is not valid UTF-8")
		}
		value, err := decodeSingleJSONValue(raw)
		if err != nil {
			return nil, fmt.Errorf("cluster: invalid JSON field %q: %w", key, err)
		}
		values[key] = value
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("cluster: encode JSON object: %w", err)
	}
	return CanonicalJSONObject(encoded)
}

func decodeSingleJSONValue(encoded []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	value, err := decodeJSONValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("trailing JSON value")
		}
		return nil, err
	}
	return value, nil
}

func decodeJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return token, nil
	}
	switch delim {
	case '{':
		object := map[string]any{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("JSON object key is not a string")
			}
			if _, duplicate := object[key]; duplicate {
				return nil, fmt.Errorf("duplicate JSON object key %q", key)
			}
			value, err := decodeJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
			return nil, errors.New("unterminated JSON object")
		}
		return object, nil
	case '[':
		array := []any{}
		for decoder.More() {
			value, err := decodeJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
			return nil, errors.New("unterminated JSON array")
		}
		return array, nil
	default:
		return nil, errors.New("unexpected JSON delimiter")
	}
}

func canonicalRawQuery(raw string) (string, error) {
	if len(raw) > MaxNodeRequestQueryBytes {
		return "", fmt.Errorf("cluster: node request query exceeds %d bytes", MaxNodeRequestQueryBytes)
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return "", fmt.Errorf("cluster: invalid node request query: %w", err)
	}
	return values.Encode(), nil
}

func canonicalNodeRequestHeaders(source http.Header) (map[string][]string, error) {
	connectionHeaders := map[string]struct{}{}
	for name, values := range source {
		if strings.EqualFold(name, "Connection") {
			for _, value := range values {
				for _, token := range strings.Split(value, ",") {
					connectionHeaders[strings.ToLower(strings.TrimSpace(token))] = struct{}{}
				}
			}
		}
	}
	result := map[string][]string{}
	total := 0
	for name, values := range source {
		canonicalName := textproto.CanonicalMIMEHeaderKey(name)
		if canonicalName == "" {
			return nil, fmt.Errorf("cluster: invalid request header name %q", name)
		}
		lowerName := strings.ToLower(canonicalName)
		if _, nominated := connectionHeaders[lowerName]; nominated || protectedNodeRequestHeader(lowerName) {
			continue
		}
		if len(values) == 0 {
			continue
		}
		for _, value := range values {
			if strings.ContainsAny(value, "\r\n\x00") {
				return nil, fmt.Errorf("cluster: invalid value for request header %q", canonicalName)
			}
			total += len(canonicalName) + len(value)
			if total > MaxNodeRequestHeaderBytes {
				return nil, fmt.Errorf("cluster: node request headers exceed %d bytes", MaxNodeRequestHeaderBytes)
			}
			result[canonicalName] = append(result[canonicalName], value)
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func protectedNodeRequestHeader(lowerName string) bool {
	switch lowerName {
	case "authorization", "proxy-authorization", "x-api-key", "cookie",
		"connection", "proxy-connection", "keep-alive", "transfer-encoding",
		"te", "trailer", "upgrade", "host", "content-length", "forwarded",
		"via", "x-access-token", "x-kuasar-proxy-error", "x-kuasar-node-id",
		"x-kuasar-node-epoch", "x-kuasar-storage-generation",
		"x-kuasar-registry-generation", "x-kuasar-binding-digest",
		"x-kuasar-execution-kind", "x-kuasar-execution-object-id",
		"x-kuasar-sandbox-group", "x-kuasar-route-key", "x-kuasar-pull-token":
		return true
	default:
		return false
	}
}
