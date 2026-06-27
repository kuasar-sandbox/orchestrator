// Package cluster contains the shared contracts for the cluster control plane's
// stateful registry design: versioned member placement and sandbox-group providers.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/maglev"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/groupcfg"
)

// MemberView is the versioned registry member set. LocateN must use this stable
// view, never SWIM's live set; SWIM only affects availability/retry decisions.
type MemberView struct {
	Version int64
	Members []string
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

// Secret is a typed secret value. Inline values are carried by registry/scaler;
// ref values are resolved out-of-band by the node or provider.
type Secret struct {
	Type  string `json:"type"`            // inline | ref
	Value string `json:"value,omitempty"` // inline secret or provider reference
}

const (
	SecretInline = "inline"
	SecretRef    = "ref"
)

// SandboxGroup is the group-level configuration consumed by route_link/scaler.
type SandboxGroup struct {
	Group        string            `json:"group"`
	Config       map[string]string `json:"sandbox_config,omitempty"`
	ImageRepo    string            `json:"image_repo,omitempty"`
	RegistryAuth Secret            `json:"registry_auth,omitempty"`
	TemplateRef  string            `json:"template_ref,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// PlacementHint is the raw placement input. Scaler folds shuffle-sharding into
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
	GetKey(ctx context.Context, group string) (Secret, bool, error)     // manifest-key, node-facing
	GetAuthKey(ctx context.Context, group string) (Secret, bool, error) // auth-key, router/node-facing
}

type SandboxGroupImporter interface {
	Range(ctx context.Context, cursor string, limit int) (GroupPage, error)
}

// ResolverAdapter projects the existing groupcfg resolver into the unified
// provider interface.
type ResolverAdapter struct {
	Resolver groupcfg.Resolver
}

func (a ResolverAdapter) Get(ctx context.Context, group string) (SandboxGroup, bool, error) {
	sc, found, err := a.Resolver.Sandbox.SandboxConfig(ctx, group)
	if err != nil || !found {
		return SandboxGroup{}, found, err
	}
	out := SandboxGroup{Group: group, Config: sc.Config, ImageRepo: sc.ImageRepo, TemplateRef: sc.TemplateRef}
	if a.Resolver.ImagePull != nil {
		if ip, ok, err := a.Resolver.ImagePull.ImagePull(ctx, group); err != nil {
			return SandboxGroup{}, false, err
		} else if ok {
			if ip.ImageRepo != "" {
				out.ImageRepo = ip.ImageRepo
			}
			if ip.RegistryAuth != "" {
				out.RegistryAuth = Secret{Type: SecretInline, Value: ip.RegistryAuth}
			}
		}
	}
	return out, true, nil
}

func (a ResolverAdapter) GetPlacementHint(ctx context.Context, group string) (PlacementHint, bool, error) {
	p, found, err := a.Resolver.Placement.Placement(ctx, group)
	if err != nil || !found {
		return PlacementHint{}, found, err
	}
	return PlacementHint{NodeSelectors: p.NodeSelectors, ShuffleLabels: p.ShuffleLabels}, true, nil
}

func (a ResolverAdapter) GetKey(ctx context.Context, group string) (Secret, bool, error) {
	k, found, err := a.Resolver.Key.Key(ctx, group)
	if err != nil || !found || k.ManifestKey == "" {
		return Secret{}, found, err
	}
	return Secret{Type: SecretInline, Value: k.ManifestKey}, true, nil
}

func (a ResolverAdapter) GetAuthKey(ctx context.Context, group string) (Secret, bool, error) {
	k, found, err := a.Resolver.Key.Key(ctx, group)
	if err != nil || !found || k.AuthKey == "" {
		return Secret{}, found, err
	}
	return Secret{Type: SecretInline, Value: k.AuthKey}, true, nil
}
