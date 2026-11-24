// Package placement contains request-scoped Node selector constraints. Group
// support, authentication and runtime identity are independent hard filters.
package placement

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"unicode/utf8"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	Header             = "X-Kuasar-Sandbox-Placement"
	Metadata           = "kuasar-sandbox.placement"
	RuntimeLabelPrefix = "kuasar-sandbox.boot/"
	MaxBytes           = 8 << 10
)

var ErrHeaderTooLarge = errors.New("placement header exceeds 8 KiB")

type Request struct {
	NodeSelectors []map[string]string `json:"node_selectors,omitempty"`
}

func (p Request) Clone() Request {
	if p.NodeSelectors == nil {
		return Request{}
	}
	out := Request{NodeSelectors: make([]map[string]string, len(p.NodeSelectors))}
	for i, branch := range p.NodeSelectors {
		if branch == nil {
			continue
		}
		out.NodeSelectors[i] = make(map[string]string, len(branch))
		for k, v := range branch {
			out.NodeSelectors[i][k] = v
		}
	}
	return out
}

func Parse(raw string) (Request, error) {
	var out Request
	data := bytes.TrimSpace([]byte(raw))
	if len(raw) > MaxBytes || !utf8.ValidString(raw) || len(data) == 0 || data[0] != '{' {
		return out, errors.New("placement must be a UTF-8 JSON object of at most 8 KiB")
	}
	if err := strictjson.DecodeAllowNull(data, &out); err != nil {
		return Request{}, fmt.Errorf("placement: %w", err)
	}
	// encoding/json accepts null into a string as its zero value. Only the
	// selector list itself has null-as-empty semantics, never a label value.
	var wire struct {
		NodeSelectors []map[string]json.RawMessage `json:"node_selectors"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return Request{}, err
	}
	for _, branch := range wire.NodeSelectors {
		for _, raw := range branch {
			value := bytes.TrimSpace(raw)
			if len(value) == 0 || value[0] != '"' {
				return Request{}, errors.New("placement label values must be strings")
			}
		}
	}
	if err := out.Validate(); err != nil {
		return Request{}, err
	}
	return out, nil
}

func (p Request) Validate() error {
	if len(p.NodeSelectors) > 32 {
		return errors.New("placement exceeds 32 selector branches")
	}
	total := 0
	for _, branch := range p.NodeSelectors {
		if branch == nil || len(branch) > 32 {
			return errors.New("placement branches must be objects with at most 32 items")
		}
		total += len(branch)
		for key, value := range branch {
			if len(key) > 512 || len(value) > 256 || !utf8.ValidString(key) || !utf8.ValidString(value) {
				return errors.New("placement selector key/value exceeds the UTF-8 byte budget")
			}
		}
	}
	if total > 256 {
		return errors.New("placement exceeds 256 selector items")
	}
	return nil
}

// Extract validates both supplied layers before selecting the entire header
// object, then removes the request-only metadata from the persistence input.
func Extract(meta map[string]string, headers http.Header) (map[string]string, Request, error) {
	selected := Request{}
	if raw, ok := meta[Metadata]; ok {
		var err error
		selected, err = Parse(raw)
		if err != nil {
			return nil, Request{}, err
		}
	}
	if values, ok := headers[http.CanonicalHeaderKey(Header)]; ok {
		if len(values) != 1 {
			return nil, Request{}, errors.New("placement header must appear exactly once")
		}
		if len(values[0]) > MaxBytes {
			return nil, Request{}, ErrHeaderTooLarge
		}
		var err error
		selected, err = Parse(values[0])
		if err != nil {
			return nil, Request{}, err
		}
	}
	if _, present := meta[Metadata]; !present {
		return meta, selected, nil
	}
	clean := make(map[string]string, len(meta)-1)
	for key, value := range meta {
		if key != Metadata {
			clean[key] = value
		}
	}
	return clean, selected, nil
}

func (p Request) Matches(labels map[string]string) bool {
	if len(p.NodeSelectors) == 0 {
		return true
	}
	for _, branch := range p.NodeSelectors {
		matches := true
		for key, value := range branch {
			got, exists := labels[key]
			if !exists || got != value {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func RuntimeMatches(template types.TemplateID, labels map[string]string) bool {
	ref, err := manifest.ParseRef(template.Ref)
	if err != nil {
		return false
	}
	if ref.Runtime.IsZero() {
		return true
	}
	return template.Kind == types.KindSnp && labels[RuntimeLabelPrefix+ref.Runtime.Basename] == "digest:"+ref.Runtime.Digest
}
