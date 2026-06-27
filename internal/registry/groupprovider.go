package registry

import (
	"context"
	"crypto/tls"
	"sort"
	"time"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/groupcfg"
)

// Store-backed group-config providers (the default: the registry self-stores group
// config). Each projects a GroupConfig to its interface's value type. The external
// alternatives live in internal/groupcfg; NewGroupResolver picks per interface.

type storeKey struct{ s *Stores }

func (p storeKey) Key(ctx context.Context, group string) (groupcfg.Key, bool, error) {
	g, found, err := p.s.GetGroupByID(ctx, group)
	if err != nil || !found {
		return groupcfg.Key{}, found, err
	}
	return groupcfg.Key{ProjectID: g.ProjectID, ManifestKey: g.ManifestKey, AuthKey: g.AuthKey}, true, nil
}

type storeSandbox struct{ s *Stores }

func (p storeSandbox) SandboxConfig(ctx context.Context, group string) (groupcfg.SandboxConfig, bool, error) {
	g, found, err := p.s.GetGroupByID(ctx, group)
	if err != nil || !found {
		return groupcfg.SandboxConfig{}, found, err
	}
	return groupcfg.SandboxConfig{Config: g.SandboxConfig, ImageRepo: g.ImageRepo, TemplateRef: g.TemplateRef}, true, nil
}

type storePlacement struct{ s *Stores }

func (p storePlacement) Placement(ctx context.Context, group string) (groupcfg.Placement, bool, error) {
	g, found, err := p.s.GetGroupByID(ctx, group)
	if err != nil || !found {
		return groupcfg.Placement{}, found, err
	}
	return groupcfg.Placement{NodeSelectors: g.NodeSelectors, ShuffleLabels: g.ShuffleLabels}, true, nil
}

// StorePlacement is the store-backed placement provider (the in-process scaler's
// default; exported so the scaler / tests can use it without the full resolver).
func StorePlacement(s *Stores) groupcfg.PlacementProvider { return storePlacement{s} }

type storeImagePull struct{ s *Stores }

func (p storeImagePull) ImagePull(ctx context.Context, group string) (groupcfg.ImagePull, bool, error) {
	g, found, err := p.s.GetGroupByID(ctx, group)
	if err != nil || !found {
		return groupcfg.ImagePull{}, found, err
	}
	return groupcfg.ImagePull{ImageRepo: g.ImageRepo, RegistryAuth: g.RegistryAuth}, true, nil
}

// storeResolver is the all-store resolver (the registry default).
func storeResolver(s *Stores) groupcfg.Resolver {
	return groupcfg.Resolver{Key: storeKey{s}, Sandbox: storeSandbox{s}, Placement: storePlacement{s}, ImagePull: storeImagePull{s}}
}

type storeGroupImporter struct{ s *Stores }

func (p storeGroupImporter) Range(ctx context.Context, cursor string, limit int) (clusterstate.GroupPage, error) {
	if limit <= 0 {
		limit = 1024
	}
	groups := make([]string, 0)
	if err := p.s.RangeGroups(ctx, func(g *GroupConfig) error {
		if g.Group != "" {
			groups = append(groups, g.Group)
		}
		return nil
	}); err != nil {
		return clusterstate.GroupPage{}, err
	}
	sort.Strings(groups)
	start := 0
	if cursor != "" {
		start = sort.SearchStrings(groups, cursor)
		for start < len(groups) && groups[start] <= cursor {
			start++
		}
	}
	end := start + limit
	if end > len(groups) {
		end = len(groups)
	}
	page := clusterstate.GroupPage{Groups: append([]string(nil), groups[start:end]...)}
	if end < len(groups) {
		page.NextCursor = groups[end-1]
	}
	return page, nil
}

// NewGroupResolver builds a resolver, picking store vs external per fine-grained
// interface from sandbox_group.providers (cluster.md §6.2). extTLS dials an
// external:<addr> over mTLS; ttl caches external reads (hot path).
func NewGroupResolver(cfg clustercfg.SandboxGroupConfig, s *Stores, ttl time.Duration, extTLS *tls.Config) groupcfg.Resolver {
	r := storeResolver(s)
	if _, ext, addr := cfg.ProviderFor(clustercfg.ProviderKey); ext {
		r.Key = groupcfg.NewExternalKey(addr, extTLS, ttl)
	}
	if _, ext, addr := cfg.ProviderFor(clustercfg.ProviderSandboxConfig); ext {
		r.Sandbox = groupcfg.NewExternalSandbox(addr, extTLS, ttl)
	}
	if _, ext, addr := cfg.ProviderFor(clustercfg.ProviderPlacement); ext {
		r.Placement = groupcfg.NewExternalPlacement(addr, extTLS, ttl)
	}
	if _, ext, addr := cfg.ProviderFor(clustercfg.ProviderImagePull); ext {
		r.ImagePull = groupcfg.NewExternalImagePull(addr, extTLS, ttl)
	}
	return r
}
