package keys

import "encoding/json"

// Secret is the existing typed secret wire format shared by Group and cluster
// inputs. Referenced material still requires an explicit capable resolver.
type Secret struct {
	Type        string `json:"type"`
	Value       string `json:"value,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
}

const (
	SecretInline = "inline"
	SecretRef    = "ref"
)

func (s *Secret) UnmarshalJSON(raw []byte) error {
	if string(raw) == "null" {
		*s = Secret{}
		return nil
	}
	var shorthand string
	if len(raw) > 0 && raw[0] == '"' {
		if err := json.Unmarshal(raw, &shorthand); err != nil {
			return err
		}
		if shorthand == "" {
			*s = Secret{}
		} else {
			*s = Secret{Type: SecretInline, Value: shorthand}
		}
		return nil
	}
	type alias Secret
	var out alias
	if err := json.Unmarshal(raw, &out); err != nil {
		return err
	}
	if out.Type == "" && out.Value != "" {
		out.Type = SecretInline
	}
	*s = Secret(out)
	return nil
}
