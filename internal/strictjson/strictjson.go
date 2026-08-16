// Package strictjson provides the JSON boundary used by public configuration
// inputs. encoding/json otherwise accepts duplicate object keys and trailing
// values, both of which make immutable definitions ambiguous.
package strictjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Decode decodes exactly one JSON value, rejects duplicate object keys and
// explicit null values at any depth, rejects unknown struct fields, and
// preserves numbers as json.Number. Build definitions use presence as part of
// their immutable contract, so accepting null as an alias for omission would
// make retries ambiguous.
func Decode(raw []byte, out any) error {
	return decode(raw, out, true, true)
}

// DecodeAllowUnknown provides duplicate/trailing protections for an established
// envelope whose unrelated extension fields are intentionally ignored. Null is
// rejected by each owned presence-aware leaf instead of globally: an unrelated
// extension's optional null must not change the compatibility of that envelope.
// Nested schemas that this repository owns (for example kuasar-sandbox.builder)
// must continue to use Decode.
func DecodeAllowUnknown(raw []byte, out any) error {
	return decode(raw, out, false, false)
}

func decode(raw []byte, out any, disallowUnknown, rejectNull bool) error {
	if err := rejectAmbiguousJSON(raw, rejectNull); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if disallowUnknown {
		dec.DisallowUnknownFields()
	}
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON contains trailing data")
		}
		return fmt.Errorf("invalid trailing JSON: %w", err)
	}
	return nil
}

// RejectDuplicateKeys walks exactly one JSON value and rejects duplicate keys
// before encoding/json can apply its last-value-wins behavior.
func RejectDuplicateKeys(raw []byte) error {
	return rejectAmbiguousJSON(raw, false)
}

func rejectAmbiguousJSON(raw []byte, rejectNull bool) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := scanValue(dec, "$", rejectNull); err != nil {
		return err
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON contains trailing data")
		}
		return fmt.Errorf("invalid trailing JSON: %w", err)
	}
	return nil
}

func scanValue(dec *json.Decoder, path string, rejectNull bool) error {
	token, err := dec.Token()
	if err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if token == nil && rejectNull {
		return fmt.Errorf("JSON contains null at %s", path)
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return fmt.Errorf("invalid JSON: %w", err)
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("invalid JSON object key")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("JSON contains duplicate key %q at %s", key, path)
			}
			seen[key] = struct{}{}
			if err := scanValue(dec, path+"."+key, rejectNull); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("invalid JSON: %w", err)
		}
		return nil
	case '[':
		index := 0
		for dec.More() {
			if err := scanValue(dec, fmt.Sprintf("%s[%d]", path, index), rejectNull); err != nil {
				return err
			}
			index++
		}
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("invalid JSON: %w", err)
		}
		return nil
	default:
		return errors.New("invalid JSON delimiter")
	}
}
