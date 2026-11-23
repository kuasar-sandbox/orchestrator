// Package nodegroup defines the node-owned Group business resource. Transport
// adapters share this contract; credentials and placement policies are not part
// of its public context.
package nodegroup

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/storage"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	MaxNameBytes = 1024
	// Leave room in the existing 1 MiB NodeLink frame for the command envelope,
	// identity, revision and optional key renewal. Context is private Node data;
	// only the much smaller Support projection enters shared metadata.
	MaxContextBytes   = (1 << 20) - (8 << 10)
	MaxPageSize       = 128
	PlacementMetadata = "kuasar-sandbox.placement"
)

var (
	ErrInvalid      = errors.New("invalid Group request")
	ErrTooLarge     = errors.New("Group exceeds encoding budget")
	ErrUnauthorized = errors.New("malformed API key")
	ErrForbidden    = errors.New("API key is not allowed")
	ErrNotFound     = errors.New("Group not found")
	ErrConflict     = errors.New("Group version conflict")
)

// VersionError exposes a revision only after complete owner authentication.
type VersionError struct {
	Kind    error
	Version int64
}

func (e *VersionError) Error() string { return e.Kind.Error() }
func (e *VersionError) Unwrap() error { return e.Kind }

type Context struct {
	TemplateID    *string           `json:"templateID,omitempty"`
	SandboxConfig map[string]string `json:"sandbox_config,omitempty"`
	ImageRepo     string            `json:"image_repo,omitempty"`
	RegistryAuth  *keys.Secret      `json:"registry_auth,omitempty"`
	TargetPort    int               `json:"target_port,omitempty"`
	ProjectID     string            `json:"project_id,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

func decodeStrict(raw []byte, into any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(into); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("%w: expected one JSON value", ErrInvalid)
	}
	return nil
}

func (c *Context) UnmarshalJSON(raw []byte) error {
	if len(raw) > MaxContextBytes {
		return ErrTooLarge
	}
	if len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
		return fmt.Errorf("%w: context must be an object", ErrInvalid)
	}
	type plain Context
	var out plain
	if err := decodeStrict(raw, &out); err != nil {
		return err
	}
	*c = Context(out)
	return nil
}

type PutRequest struct {
	ExpectedVersion int64   `json:"expected_version"`
	Context         Context `json:"context"`
}

func (p *PutRequest) UnmarshalJSON(raw []byte) error {
	var wire struct {
		ExpectedVersion *int64          `json:"expected_version"`
		Context         json.RawMessage `json:"context"`
	}
	if err := decodeStrict(raw, &wire); err != nil {
		return err
	}
	if wire.ExpectedVersion == nil || *wire.ExpectedVersion < 0 {
		return fmt.Errorf("%w: expected_version is required and nonnegative", ErrInvalid)
	}
	var c Context
	if err := json.Unmarshal(wire.Context, &c); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	*p = PutRequest{ExpectedVersion: *wire.ExpectedVersion, Context: c}
	return nil
}

// KeyPut is only accepted on a trusted NodeLink maintenance command. HTTP/UDS
// callers never provide roots in a Group body. Expiry is absolute, including
// retries; zero means permanent.
type KeyPut struct {
	APISecret   string `json:"api_secret"`
	ManifestKey string `json:"manifest_key"`
	ExpiresUnix int64  `json:"expires_unix"`
}

type Result struct {
	Group   string `json:"group"`
	Version int64  `json:"version"`
}
type Detail struct {
	Group   string  `json:"group"`
	Version int64   `json:"version"`
	Context Context `json:"context"`
}
type Page struct {
	Groups     []Result `json:"groups"`
	NextCursor string   `json:"next_cursor,omitempty"`
}
type DefaultContext struct {
	TemplateID string `json:"templateID"`
}

func (c *DefaultContext) UnmarshalJSON(raw []byte) error {
	type plain DefaultContext
	var decoded plain
	if err := decodeStrict(raw, &decoded); err != nil {
		return err
	}
	if _, err := types.ParseTemplateID(decoded.TemplateID); err != nil {
		return fmt.Errorf("%w: default_context.templateID", ErrInvalid)
	}
	*c = DefaultContext(decoded)
	return nil
}

type AuthMaterial struct {
	APISecret string `json:"api_secret"`
}
type Routing struct {
	TargetPort int `json:"target_port"`
}
type Entry struct {
	AuthRef      string       `json:"auth_ref"`
	AuthMaterial AuthMaterial `json:"auth_material"`
	Routing      Routing      `json:"routing"`
}
type Support struct {
	Group          string          `json:"group"`
	Version        int64           `json:"version"`
	DefaultContext *DefaultContext `json:"default_context,omitempty"`
	Entries        []Entry         `json:"entries"`
}

func (c Context) Support(group string, version int64, owner, apiSecret string) Support {
	out := Support{Group: group, Version: version, Entries: []Entry{{AuthRef: owner, AuthMaterial: AuthMaterial{APISecret: apiSecret}, Routing: Routing{TargetPort: c.TargetPort}}}}
	if c.TemplateID != nil {
		out.DefaultContext = &DefaultContext{TemplateID: *c.TemplateID}
	}
	return out
}

func ValidateName(name string) error {
	if name == "" || !utf8.ValidString(name) {
		return fmt.Errorf("%w: group name is required and must be UTF-8", ErrInvalid)
	}
	if len(name) > MaxNameBytes {
		return ErrTooLarge
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: control character in group name", ErrInvalid)
		}
	}
	return nil
}

// Normalize returns a detached context and stable JSON for whole-context CAS
// no-ops. Only established resource/traffic normalization applies; request-only
// overrides are filtered later by MergeCreateMetadata at default creation.
func Normalize(c Context) (Context, []byte, error) {
	if c.TemplateID != nil {
		if _, err := types.ParseTemplateID(*c.TemplateID); err != nil {
			return Context{}, nil, fmt.Errorf("%w: templateID: %v", ErrInvalid, err)
		}
	} else if len(c.SandboxConfig) != 0 {
		return Context{}, nil, fmt.Errorf("%w: sandbox_config requires templateID", ErrInvalid)
	}
	if _, exists := c.SandboxConfig[PlacementMetadata]; exists {
		return Context{}, nil, fmt.Errorf("%w: placement is request-scoped", ErrInvalid)
	}
	if c.TargetPort < 0 || c.TargetPort > 65535 {
		return Context{}, nil, fmt.Errorf("%w: target_port is outside 0..65535", ErrInvalid)
	}
	if c.RegistryAuth != nil && c.RegistryAuth.Value == "" {
		if c.RegistryAuth.Type != "" || c.RegistryAuth.Fingerprint != "" {
			return Context{}, nil, fmt.Errorf("%w: empty registry_auth material", ErrInvalid)
		}
		c.RegistryAuth = nil
	} else if c.RegistryAuth != nil {
		copy := *c.RegistryAuth
		c.RegistryAuth = &copy
		if c.RegistryAuth.Type == "" {
			c.RegistryAuth.Type = keys.SecretInline
		}
		if c.RegistryAuth.Type != keys.SecretInline || c.RegistryAuth.Fingerprint != "" {
			return Context{}, nil, fmt.Errorf("%w: registry_auth must contain resolved inline material", ErrInvalid)
		}
	}
	var err error
	c.SandboxConfig, err = sandboxcfg.NormalizeResourceMetadata(c.SandboxConfig)
	if err == nil {
		c.SandboxConfig, err = sandboxcfg.NormalizeTrafficMetadata(c.SandboxConfig)
	}
	if err != nil {
		return Context{}, nil, fmt.Errorf("%w: sandbox_config: %v", ErrInvalid, err)
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return Context{}, nil, fmt.Errorf("%w: context encoding", ErrInvalid)
	}
	if len(raw) > MaxContextBytes {
		return Context{}, nil, ErrTooLarge
	}
	var out Context
	if err := json.Unmarshal(raw, &out); err != nil {
		return Context{}, nil, err
	}
	return out, raw, nil
}

// ValidateSupportBudget reserves the shared record envelope in addition to the
// payload. It is checked before the Node commit, including local-only writes.
func ValidateSupportBudget(s Support) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("%w: support encoding", ErrInvalid)
	}
	if len(raw) > storage.MaxValueBytes-1024 {
		return ErrTooLarge
	}
	return nil
}
