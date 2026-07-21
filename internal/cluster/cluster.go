// Package cluster contains shared contracts for the cluster control plane.
package cluster

import (
	"context"
	"encoding/json"
)

// Secret is a typed secret value. Inline values are carried by registry/placer;
// ref values are resolved out-of-band by the node or provider.
type Secret struct {
	Type        string `json:"type"`                  // inline | ref
	Value       string `json:"value,omitempty"`       // inline secret or provider reference
	Fingerprint string `json:"fingerprint,omitempty"` // required for referenced AuthKey/ManifestKey material
}

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

const (
	SecretInline = "inline"
	SecretRef    = "ref"
)

// SandboxGroup is the group-level configuration consumed by route_link/placer.
type SandboxGroup struct {
	Group                 string            `json:"group"`
	Config                map[string]string `json:"sandbox_config,omitempty"`
	ImageRepo             string            `json:"image_repo,omitempty"`
	RegistryAuth          Secret            `json:"registry_auth,omitempty"`
	TemplateRef           string            `json:"template_ref,omitempty"`
	AllowTemplateOverride bool              `json:"allow_template_override,omitempty"`
	TargetPort            int               `json:"target_port,omitempty"`
	Metadata              map[string]string `json:"metadata,omitempty"`
}

// SandboxGroupRecord is the importer record owned by placer/provider side. It is
// intentionally richer than SandboxGroup: placer needs placement selectors and
// secret material to answer Place and refresh complete node key leases.
type SandboxGroupRecord struct {
	Group                 string              `json:"group"`
	ProjectID             string              `json:"project_id,omitempty"`
	ManifestKey           Secret              `json:"manifest_key,omitempty"`
	AuthKey               Secret              `json:"auth_key,omitempty"`
	RegistryAuth          Secret              `json:"registry_auth,omitempty"`
	Config                map[string]string   `json:"sandbox_config,omitempty"`
	ImageRepo             string              `json:"image_repo,omitempty"`
	TemplateRef           string              `json:"template_ref,omitempty"`
	AllowTemplateOverride bool                `json:"allow_template_override,omitempty"`
	TargetPort            int                 `json:"target_port,omitempty"`
	Metadata              map[string]string   `json:"metadata,omitempty"`
	NodeSelectors         []map[string]string `json:"node_selectors,omitempty"`
	ShuffleLabels         map[string]string   `json:"shuffle_labels,omitempty"`
}

// PlacementHint is the raw placement input. Placer folds shuffle-sharding into
// effective selectors before writing the group placement record.
type PlacementHint struct {
	NodeSelectors []map[string]string `json:"node_selectors,omitempty"`
	ShuffleLabels map[string]string   `json:"shuffle_labels,omitempty"`
}

// SandboxGroupProvider is the unified group provider consumed by the cluster
// kernel.
type SandboxGroupProvider interface {
	GetRecord(ctx context.Context, group string) (SandboxGroupRecord, bool, error)
	Get(ctx context.Context, group string) (SandboxGroup, bool, error)
	GetPlacementHint(ctx context.Context, group string) (PlacementHint, bool, error)
	GetManifestKey(ctx context.Context, group string) (Secret, bool, error) // content key, node-facing
	GetAuthKey(ctx context.Context, group string) (Secret, bool, error)     // API auth key, router/node-facing
}

// SandboxGroupAuthorizer lets a Provider verify caller credentials without
// exposing referenced AuthKey material to Router or Registry.
type SandboxGroupAuthorizer interface {
	VerifyAPIKey(ctx context.Context, group, apiKey string) (bool, error)
}
