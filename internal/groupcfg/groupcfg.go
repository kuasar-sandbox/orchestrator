// Package groupcfg is the fine-grained group-config provider split (cluster.md
// §6.2): each interface — key, sandbox-config, placement — resolves independently
// to the registry store or an external cloud-provider service
// (clustercfg group_config.providers). The value types are self-contained (no
// registry import → no import cycle); the registry supplies the store-backed
// implementations and builds the Resolver, while this package supplies the
// external HTTP implementations and a TTL cache for the hot path.
package groupcfg

import (
	"context"
	"sync"
	"time"
)

// Key is the tenant key pair. ManifestKey from an external provider is
// pass-through only — it is never persisted by the registry (cluster.md §5.4).
type Key struct {
	ProjectID   string `json:"project_id,omitempty"`
	ManifestKey string `json:"manifest_key,omitempty"` // hex
}

// SandboxConfig is a group's sandbox defaults (folded into create config, §7.2)
// plus the snapshot template ref.
type SandboxConfig struct {
	Config      map[string]string `json:"sandbox_config,omitempty"`
	ImageRepo   string            `json:"image_repo,omitempty"`
	TemplateRef string            `json:"template_ref,omitempty"`
}

// Placement is a group's blast-radius config (the scaler's selectors/shuffle).
type Placement struct {
	NodeSelectors []map[string]string `json:"node_selectors,omitempty"`
	ShuffleLabels map[string]string   `json:"shuffle_labels,omitempty"`
}

// The three provider interfaces. Each returns (value, found, err): found==false is
// "no such group"; err!=nil is "provider unavailable" (callers map it to 503, not
// a 403/404, so a provider blip doesn't masquerade as a missing group).
type (
	KeyProvider interface {
		Key(ctx context.Context, group string) (Key, bool, error)
	}
	SandboxConfigProvider interface {
		SandboxConfig(ctx context.Context, group string) (SandboxConfig, bool, error)
	}
	PlacementProvider interface {
		Placement(ctx context.Context, group string) (Placement, bool, error)
	}
)

// Resolver bundles the three resolved providers (built by the registry from
// clustercfg, picking store vs external per interface).
type Resolver struct {
	Key       KeyProvider
	Sandbox   SandboxConfigProvider
	Placement PlacementProvider
}

// cache is a generic single-flight-free TTL cache over a fetch func, shared by the
// external providers (the key/placement reads are on the reserve hot path). On a
// refresh error it serves the last good value (availability over consistency — a
// stale selector/key only affects new placements), bounded by maxStale.
type cache[T any] struct {
	fetch    func(ctx context.Context, group string) (T, bool, error)
	ttl      time.Duration
	maxStale time.Duration
	mu       sync.Mutex
	entries  map[string]cacheEntry[T]
}

type cacheEntry[T any] struct {
	v       T
	found   bool
	fresh   time.Time // refetch after this
	expires time.Time // discard after this (hard max-stale)
}

func newCache[T any](ttl time.Duration, fetch func(context.Context, string) (T, bool, error)) *cache[T] {
	if ttl <= 0 {
		ttl = time.Minute
	}
	return &cache[T]{fetch: fetch, ttl: ttl, maxStale: 10 * ttl, entries: map[string]cacheEntry[T]{}}
}

func (c *cache[T]) lookup(ctx context.Context, group string) (T, bool, error) {
	now := time.Now()
	c.mu.Lock()
	e, has := c.entries[group]
	c.mu.Unlock()
	if has && now.Before(e.fresh) {
		return e.v, e.found, nil
	}
	v, found, err := c.fetch(ctx, group)
	if err != nil {
		if has && now.Before(e.expires) {
			return e.v, e.found, nil // serve stale within max-stale
		}
		var zero T
		return zero, false, err
	}
	c.mu.Lock()
	c.entries[group] = cacheEntry[T]{v: v, found: found, fresh: now.Add(c.ttl), expires: now.Add(c.maxStale)}
	c.mu.Unlock()
	return v, found, nil
}
