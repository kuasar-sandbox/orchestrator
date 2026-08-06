package sandboxcfg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

var canonicalMMDSJSONFields = [...]string{
	"version",
	"secrets",
	"services",
	"routes",
	"name",
	"target",
	"path",
	"type",
	"secret_name",
	"service_name",
	"content_type",
	"data",
}

type mmdsRouteFields struct {
	typeSet        bool
	secretNameSet  bool
	serviceNameSet bool
	contentTypeSet bool
	dataSet        bool
}

type mmdsJSONRouteFields struct {
	Type        json.RawMessage `json:"type"`
	SecretName  json.RawMessage `json:"secret_name"`
	ServiceName json.RawMessage `json:"service_name"`
	ContentType json.RawMessage `json:"content_type"`
	Data        json.RawMessage `json:"data"`
}

// decodeMMDSSpec applies structural JSON checks and records which route-union
// fields were supplied. Semantic and policy checks remain in ExtractMMDS.
func decodeMMDSSpec(raw string) (MMDSSpec, []mmdsRouteFields, error) {
	if err := validateMMDSJSONObject([]byte(raw)); err != nil {
		return MMDSSpec{}, nil, err
	}

	var spec MMDSSpec
	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		return MMDSSpec{}, nil, invalidMMDSJSONError(err)
	}
	// Decode only consumes one JSON value. A second Decode must return exactly
	// io.EOF; dec.More reports container elements and is not a trailing-data
	// check once the top-level object has been consumed.
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return MMDSSpec{}, nil, mmdsError("contains unexpected trailing data")
	}

	var presence struct {
		Routes []mmdsJSONRouteFields `json:"routes"`
	}
	if err := json.Unmarshal([]byte(raw), &presence); err != nil {
		return MMDSSpec{}, nil, invalidMMDSJSONError(err)
	}
	fields := make([]mmdsRouteFields, len(presence.Routes))
	for i, route := range presence.Routes {
		fields[i] = mmdsRouteFields{
			typeSet:        route.Type != nil,
			secretNameSet:  route.SecretName != nil,
			serviceNameSet: route.ServiceName != nil,
			contentTypeSet: route.ContentType != nil,
			dataSet:        route.Data != nil,
		}
	}
	return spec, fields, nil
}

// validateMMDSJSONObject requires a top-level object and rejects duplicate
// keys in every nested object before encoding/json can apply its usual
// last-value-wins behavior. Trailing data is deliberately left to the typed
// decoder's second Decode above so that check has one canonical implementation.
func validateMMDSJSONObject(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, err := dec.Token()
	if err != nil {
		return invalidMMDSJSONError(err)
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return mmdsError("must be a JSON object")
	}
	return scanMMDSJSONObject(dec)
}

func scanMMDSJSONObject(dec *json.Decoder) error {
	seen := make(map[string]struct{})
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return invalidMMDSJSONError(err)
		}
		key, ok := token.(string)
		if !ok {
			return mmdsError("is not valid JSON")
		}
		if canonical, ok := nonCanonicalMMDSJSONField(key); ok {
			return mmdsErrorWithPublic(
				fmt.Sprintf("contains non-canonical JSON field %q; use %q", key, canonical),
				"JSON field names must use the exact schema spelling",
			)
		}
		if _, ok := seen[key]; ok {
			return mmdsErrorWithPublic(fmt.Sprintf("contains duplicate JSON field %q", key), "contains a duplicate JSON field")
		}
		seen[key] = struct{}{}
		if err := scanMMDSJSONValue(dec); err != nil {
			return err
		}
	}
	if _, err := dec.Token(); err != nil {
		return invalidMMDSJSONError(err)
	}
	return nil
}

func scanMMDSJSONValue(dec *json.Decoder) error {
	token, err := dec.Token()
	if err != nil {
		return invalidMMDSJSONError(err)
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		return scanMMDSJSONObject(dec)
	case '[':
		for dec.More() {
			if err := scanMMDSJSONValue(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return invalidMMDSJSONError(err)
		}
		return nil
	default:
		return mmdsError("is not valid JSON")
	}
}

// encoding/json matches struct fields case-insensitively. Reject aliases such
// as "Version" before typed decoding so differently-cased keys cannot silently
// overwrite the same schema field or bypass duplicate-key detection.
func nonCanonicalMMDSJSONField(key string) (string, bool) {
	for _, canonical := range canonicalMMDSJSONFields {
		if key != canonical && strings.EqualFold(key, canonical) {
			return canonical, true
		}
	}
	return "", false
}
