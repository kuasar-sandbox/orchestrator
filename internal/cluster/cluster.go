// Package cluster contains shared contracts for the cluster control plane:
// versioned member placement, route/node records, and placer-side sandbox-group
// provider/importer interfaces.
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/kuasar-sandbox/accelerator/pkg/maglev"
)

// MemberView is the versioned registry member set. LocateN must use this stable
// view, never SWIM's live set; SWIM only affects availability/retry decisions.
type MemberView struct {
	Version  int64
	Label    string
	Members  []string
	ReadOnly bool
}

func (v MemberView) LabelOrDefault() string {
	if v.Label != "" {
		return v.Label
	}
	if v.Version > 0 {
		return fmt.Sprintf("membership.%d", v.Version)
	}
	return "membership.0"
}

// Owners returns the deterministic owner set for key. The caller chooses n
// according to the namespace (route_link K, node_link N, node_list M).
func (v MemberView) Owners(key string, n int) ([]string, error) {
	if n <= 0 {
		return nil, errors.New("cluster: owner count must be positive")
	}
	members := append([]string(nil), v.Members...)
	sort.Strings(members)
	if len(members) == 0 {
		return nil, errors.New("cluster: empty member view")
	}
	if n > len(members) {
		n = len(members)
	}
	owners, err := maglev.LocateN([]byte(key), members, n)
	if err != nil {
		return nil, fmt.Errorf("cluster: locate owners: %w", err)
	}
	return owners, nil
}

// Secret is a typed secret value. Inline values are carried by registry/placer;
// ref values are resolved out-of-band by the node or provider.
type Secret struct {
	Type        string `json:"type"`                  // inline | ref
	Value       string `json:"value,omitempty"`       // inline secret or provider reference
	Fingerprint string `json:"fingerprint,omitempty"` // required for referenced credentials
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
	Group        string            `json:"group"`
	Config       map[string]string `json:"sandbox_config,omitempty"`
	ImageRepo    string            `json:"image_repo,omitempty"`
	RegistryAuth Secret            `json:"registry_auth,omitempty"`
	TemplateRef  string            `json:"template_ref,omitempty"`
	TargetPort   int               `json:"target_port,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// SandboxGroupRecord is the importer record owned by placer/provider side. It is
// intentionally richer than SandboxGroup: placer needs placement selectors and
// secret material to answer Place and refresh the node_link credential cache.
type SandboxGroupRecord struct {
	Group         string              `json:"group"`
	ProjectID     string              `json:"project_id,omitempty"`
	ManifestKey   Secret              `json:"manifest_key,omitempty"`
	APISecret     Secret              `json:"api_secret,omitempty"`
	RegistryAuth  Secret              `json:"registry_auth,omitempty"`
	Config        map[string]string   `json:"sandbox_config,omitempty"`
	ImageRepo     string              `json:"image_repo,omitempty"`
	TemplateRef   string              `json:"template_ref,omitempty"`
	TargetPort    int                 `json:"target_port,omitempty"`
	Metadata      map[string]string   `json:"metadata,omitempty"`
	NodeSelectors []map[string]string `json:"node_selectors,omitempty"`
	ShuffleLabels map[string]string   `json:"shuffle_labels,omitempty"`
}

// PlacementHint is the raw placement input. Placer folds shuffle-sharding into
// effective selectors before writing the group placement record.
type PlacementHint struct {
	NodeSelectors []map[string]string `json:"node_selectors,omitempty"`
	ShuffleLabels map[string]string   `json:"shuffle_labels,omitempty"`
}

type GroupPage struct {
	Groups     []string
	NextCursor string
}

// SandboxGroupProvider is the unified group provider consumed by the cluster
// kernel.
type SandboxGroupProvider interface {
	Get(ctx context.Context, group string) (SandboxGroup, bool, error)
	GetPlacementHint(ctx context.Context, group string) (PlacementHint, bool, error)
	GetKey(ctx context.Context, group string) (Secret, bool, error)       // manifest-key, node-facing
	GetAPISecret(ctx context.Context, group string) (Secret, bool, error) // API authentication root
}

type SandboxGroupImporter interface {
	Range(ctx context.Context, cursor string, limit int) (GroupPage, error)
}
