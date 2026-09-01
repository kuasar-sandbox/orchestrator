package migrationtoken

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"unicode/utf8"
)

var requiredPayloadFields = map[string]struct{}{
	"v":                      {},
	"nodeSandboxID":          {},
	"stableID":               {},
	"apiSecretFingerprint":   {},
	"manifestKeyFingerprint": {},
	"templateID":             {},
	"profile":                {},
	"runtimeDigest":          {},
	"snapshotRef":            {},
	"createdUnix":            {},
	"deadlineUnix":           {},
	"serviceSecret":          {},
	"envdAccessToken":        {},
	"trafficAccessToken":     {},
	"forwardAccessToken":     {},
}

func parsePayload(plaintext []byte) (MigrationTokenPayloadV1, error) {
	var payload MigrationTokenPayloadV1
	if !utf8.Valid(plaintext) {
		return payload, ErrMalformedToken
	}

	dec := json.NewDecoder(bytes.NewReader(plaintext))
	token, err := dec.Token()
	if err != nil {
		return payload, ErrMalformedToken
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return payload, ErrMalformedToken
	}

	seen := make(map[string]struct{}, len(requiredPayloadFields)+2)
	for dec.More() {
		token, err = dec.Token()
		if err != nil {
			return MigrationTokenPayloadV1{}, ErrMalformedToken
		}
		field, ok := token.(string)
		if !ok {
			return MigrationTokenPayloadV1{}, ErrMalformedToken
		}
		if _, duplicate := seen[field]; duplicate {
			return MigrationTokenPayloadV1{}, ErrMalformedToken
		}
		seen[field] = struct{}{}

		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return MigrationTokenPayloadV1{}, ErrMalformedToken
		}
		if err := decodePayloadField(&payload, field, raw); err != nil {
			return MigrationTokenPayloadV1{}, err
		}
	}
	if token, err = dec.Token(); err != nil {
		return MigrationTokenPayloadV1{}, ErrMalformedToken
	}
	if delim, ok := token.(json.Delim); !ok || delim != '}' {
		return MigrationTokenPayloadV1{}, ErrMalformedToken
	}
	if token, err = dec.Token(); err != io.EOF {
		return MigrationTokenPayloadV1{}, ErrMalformedToken
	}
	for field := range requiredPayloadFields {
		if _, ok := seen[field]; !ok {
			return MigrationTokenPayloadV1{}, ErrMalformedToken
		}
	}
	return payload, nil
}

func decodePayloadField(payload *MigrationTokenPayloadV1, field string, raw json.RawMessage) error {
	switch field {
	case "v":
		return decodeStrict(raw, &payload.Version)
	case "nodeSandboxID":
		return decodeStrict(raw, &payload.NodeSandboxID)
	case "stableID":
		return decodeStrict(raw, &payload.StableID)
	case "apiSecretFingerprint":
		return decodeStrict(raw, &payload.APISecretFingerprint)
	case "manifestKeyFingerprint":
		return decodeStrict(raw, &payload.ManifestKeyFingerprint)
	case "templateID":
		return decodeStrict(raw, &payload.TemplateID)
	case "profile":
		return decodeStrict(raw, &payload.Profile)
	case "runtimeDigest":
		return decodeStrict(raw, &payload.RuntimeDigest)
	case "snapshotRef":
		return decodeStrict(raw, &payload.SnapshotRef)
	case "env":
		value, err := decodeStringMap(raw)
		payload.Env = value
		return err
	case "metadata":
		value, err := decodeStringMap(raw)
		payload.Metadata = value
		return err
	case "createdUnix":
		return decodeStrict(raw, &payload.CreatedUnix)
	case "deadlineUnix":
		return decodeStrict(raw, &payload.DeadlineUnix)
	case "serviceSecret":
		return decodeStrict(raw, &payload.ServiceSecret)
	case "envdAccessToken":
		return decodeStrict(raw, &payload.EnvdAccessToken)
	case "trafficAccessToken":
		return decodeStrict(raw, &payload.TrafficAccessToken)
	case "forwardAccessToken":
		return decodeStrict(raw, &payload.ForwardAccessToken)
	default:
		return ErrMalformedToken
	}
}

func decodeStrict(raw []byte, target any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(target); err != nil {
		return ErrMalformedToken
	}
	if _, err := dec.Token(); err != io.EOF {
		return ErrMalformedToken
	}
	return nil
}

func decodeStringMap(raw []byte) (map[string]string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, err := dec.Token()
	if err != nil {
		return nil, ErrMalformedToken
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return nil, ErrMalformedToken
	}

	values := make(map[string]string)
	for dec.More() {
		token, err = dec.Token()
		if err != nil {
			return nil, ErrMalformedToken
		}
		key, ok := token.(string)
		if !ok {
			return nil, ErrMalformedToken
		}
		if _, duplicate := values[key]; duplicate {
			return nil, ErrMalformedToken
		}
		var valueRaw json.RawMessage
		if err := dec.Decode(&valueRaw); err != nil || strings.TrimSpace(string(valueRaw)) == "null" {
			return nil, ErrMalformedToken
		}
		var value string
		if err := decodeStrict(valueRaw, &value); err != nil {
			return nil, err
		}
		values[key] = value
	}
	if token, err = dec.Token(); err != nil {
		return nil, ErrMalformedToken
	}
	if delim, ok := token.(json.Delim); !ok || delim != '}' {
		return nil, ErrMalformedToken
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, ErrMalformedToken
	}
	return values, nil
}
