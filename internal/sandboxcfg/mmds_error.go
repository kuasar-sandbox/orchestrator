package sandboxcfg

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// MMDSValidationError is returned for tenant-supplied MMDS metadata that does
// not satisfy the admission policy. Error is part of the public error boundary:
// it must contain only a safe, actionable message. Do not add raw payload,
// secret values, or implementation-specific decoder text here.
type MMDSValidationError struct {
	detail string
	public string
}

const maxPublicMMDSMessageBytes = 256

func (e *MMDSValidationError) Error() string {
	message := e.public
	if len(message) > maxPublicMMDSMessageBytes {
		cut := maxPublicMMDSMessageBytes
		for cut > 0 && !utf8.RuneStart(message[cut]) {
			cut--
		}
		message = message[:cut] + "..."
	}
	return "MMDS metadata: " + message
}

// Diagnostic returns the fuller message for trusted server-side logs. It must
// never be used as an API or node-link response reason.
func (e *MMDSValidationError) Diagnostic() string {
	return fmt.Sprintf("sandboxcfg: metadata[%q] %s", NsMMDS, e.detail)
}

func mmdsError(message string) error {
	return &MMDSValidationError{detail: message, public: message}
}

func mmdsErrorWithPublic(detail, public string) error {
	return &MMDSValidationError{detail: detail, public: public}
}

func invalidMMDSJSONError(err error) error {
	// Do not expose encoding/json's complete error text: it is an implementation
	// detail and may include input-controlled field names. The byte offset is
	// useful for correcting the payload and is safe to expose.
	if syntaxErr, ok := err.(*json.SyntaxError); ok {
		return mmdsErrorWithPublic(fmt.Sprintf("is not valid JSON at byte %d", syntaxErr.Offset), "is not valid JSON")
	}
	if typeErr, ok := err.(*json.UnmarshalTypeError); ok && typeErr.Field != "" {
		return mmdsError(fmt.Sprintf("has an invalid value for field %q", typeErr.Field))
	}
	if field, ok := unknownJSONField(err); ok {
		message := fmt.Sprintf("contains unknown field %q", field)
		return mmdsError(message)
	}
	return mmdsError("is not valid JSON")
}

func unknownJSONField(err error) (string, bool) {
	const prefix = "json: unknown field "
	message := err.Error()
	if !strings.HasPrefix(message, prefix) {
		return "", false
	}
	field, err := strconv.Unquote(strings.TrimPrefix(message, prefix))
	if err != nil || len(field) == 0 || len(field) > 64 {
		return "", false
	}
	for i, r := range field {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9' && i > 0) || r == '_' || r == '-' {
			continue
		}
		return "", false
	}
	return field, true
}
