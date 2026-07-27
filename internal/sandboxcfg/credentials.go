package sandboxcfg

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Credentials is the creation-time override carried by NsCredentials. Empty
// fields mean that the caller did not specify an override.
type Credentials struct {
	ServiceSecret      string `json:"service_secret,omitempty"`
	EnvdAccessToken    string `json:"envd_access_token,omitempty"`
	TrafficAccessToken string `json:"traffic_access_token,omitempty"`
}

const maxE2BAccessTokenBytes = 256

// ValidE2BAccessToken reports whether token is a valid optional e2b service
// credential. Empty means unspecified; a supplied token must be valid UTF-8,
// contain no NUL byte, and be no longer than 256 bytes so every persisted and
// shared-memory representation preserves it exactly.
func ValidE2BAccessToken(token string) bool {
	return utf8.ValidString(token) && !strings.ContainsRune(token, '\x00') && len(token) <= maxE2BAccessTokenBytes
}

// ValidateCredentialsForProfile applies the fixed profile contract after the
// credentials object has been parsed and before lifecycle side effects begin.
func ValidateCredentialsForProfile(profile types.Profile, credentials Credentials) error {
	if !profile.Valid() {
		return fmt.Errorf("sandboxcfg: unknown sandbox profile %q", profile)
	}
	if !ValidE2BAccessToken(credentials.EnvdAccessToken) {
		return errors.New("sandboxcfg: envd_access_token must be valid UTF-8 without NUL bytes and at most 256 bytes")
	}
	if !ValidE2BAccessToken(credentials.TrafficAccessToken) {
		return errors.New("sandboxcfg: traffic_access_token must be valid UTF-8 without NUL bytes and at most 256 bytes")
	}
	if profile == types.ProfileBare && (credentials.EnvdAccessToken != "" || credentials.TrafficAccessToken != "") {
		return errors.New("sandboxcfg: envd_access_token and traffic_access_token are not valid for bare sandboxes")
	}
	return nil
}

// ExtractCredentials strictly parses and removes the credentials namespace from
// metadata. When the namespace is present, cleaned is a clone and meta is never
// modified. When it is absent, the zero Credentials and the original map are
// returned unchanged.
func ExtractCredentials(meta map[string]string) (credentials Credentials, cleaned map[string]string, err error) {
	raw, ok := meta[NsCredentials]
	if !ok {
		return Credentials{}, meta, nil
	}

	credentials, err = parseCredentials(raw)
	if err != nil {
		return Credentials{}, nil, err
	}
	cleaned = make(map[string]string, len(meta)-1)
	for key, value := range meta {
		if key != NsCredentials {
			cleaned[key] = value
		}
	}
	return credentials, cleaned, nil
}

func parseCredentials(raw string) (Credentials, error) {
	var credentials Credentials
	dec := json.NewDecoder(strings.NewReader(raw))

	token, err := dec.Token()
	if err != nil {
		return credentials, credentialsError("must be a JSON object")
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return credentials, credentialsError("must be a JSON object")
	}

	seen := make(map[string]struct{}, 3)
	for dec.More() {
		token, err = dec.Token()
		if err != nil {
			return Credentials{}, credentialsError("is not a valid JSON object")
		}
		field, ok := token.(string)
		if !ok {
			return Credentials{}, credentialsError("is not a valid JSON object")
		}
		if _, duplicate := seen[field]; duplicate {
			return Credentials{}, credentialsError("contains a duplicate field")
		}
		seen[field] = struct{}{}
		switch field {
		case "service_secret", "envd_access_token", "traffic_access_token":
		default:
			return Credentials{}, credentialsError("contains an unknown field")
		}

		var valueRaw json.RawMessage
		if err := dec.Decode(&valueRaw); err != nil {
			return Credentials{}, credentialsError("is not a valid JSON object")
		}
		value, err := parseCredentialString(field, valueRaw)
		if err != nil {
			return Credentials{}, err
		}
		switch field {
		case "service_secret":
			credentials.ServiceSecret = value
		case "envd_access_token":
			credentials.EnvdAccessToken = value
		case "traffic_access_token":
			credentials.TrafficAccessToken = value
		}
	}

	token, err = dec.Token()
	if err != nil {
		return Credentials{}, credentialsError("is not a valid JSON object")
	}
	if delim, ok := token.(json.Delim); !ok || delim != '}' {
		return Credentials{}, credentialsError("is not a valid JSON object")
	}
	if _, err := dec.Token(); err != io.EOF {
		return Credentials{}, credentialsError("contains trailing content")
	}

	if credentials.ServiceSecret != "" {
		decoded, err := hex.DecodeString(credentials.ServiceSecret)
		if err != nil || len(decoded) != 32 || credentials.ServiceSecret != strings.ToLower(credentials.ServiceSecret) {
			return Credentials{}, credentialsError("service_secret must be 64 lowercase hex characters")
		}
	}
	if !ValidE2BAccessToken(credentials.EnvdAccessToken) {
		return Credentials{}, credentialsError("envd_access_token must be valid UTF-8 without NUL bytes and at most 256 bytes")
	}
	if !ValidE2BAccessToken(credentials.TrafficAccessToken) {
		return Credentials{}, credentialsError("traffic_access_token must be valid UTF-8 without NUL bytes and at most 256 bytes")
	}
	return credentials, nil
}

func parseCredentialString(field string, raw json.RawMessage) (string, error) {
	if strings.TrimSpace(string(raw)) == "null" {
		return "", credentialsError(fmt.Sprintf("%s must not be null", field))
	}
	if !utf8.Valid(raw) {
		return "", credentialsError(fmt.Sprintf("%s must contain valid UTF-8", field))
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", credentialsError(fmt.Sprintf("%s must be a string", field))
	}
	return value, nil
}

func credentialsError(message string) error {
	return fmt.Errorf("sandboxcfg: metadata[%q] %s", NsCredentials, message)
}
