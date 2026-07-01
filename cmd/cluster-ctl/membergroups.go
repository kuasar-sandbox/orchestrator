package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/membergroup"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
)

var (
	registryMembershipReadyPoll = 100 * time.Millisecond
)

type registryMemberRuntime struct {
	mu     sync.RWMutex
	hub    *membergroup.Hub
	groups map[string]*managedMemberGroup
	selfID string
	tlsCfg *tls.Config
	log    *slog.Logger
}

type managedMemberGroup struct {
	group   *membergroup.Group
	members map[string]bool
	cancel  context.CancelFunc
}

func newRegistryMemberRuntime(ctx context.Context, cfg *clustercfg.RegistryConfig, hub *membergroup.Hub, log *slog.Logger) (*registryMemberRuntime, error) {
	tlsCfg, err := membergroupTLSConfig(cfg.Member.TLS)
	if err != nil {
		return nil, fmt.Errorf("registry memberlist tls: %w", err)
	}
	rt := &registryMemberRuntime{
		hub: hub, groups: map[string]*managedMemberGroup{},
		selfID: cfg.Member.ID, tlsCfg: tlsCfg, log: log,
	}
	if err := rt.Sync(ctx, cfg); err != nil {
		return nil, err
	}
	return rt, nil
}

func (r *registryMemberRuntime) Sync(ctx context.Context, cfg *clustercfg.RegistryConfig) error {
	desired := map[string]bool{}
	for _, version := range cfg.Membership.MemberVersions() {
		label := version.Label
		desired[label] = true
		if _, ok := r.group(label); ok {
			continue
		}
		seeds := map[string]string{}
		members := map[string]bool{}
		selfInVersion := false
		for _, m := range version.Members {
			if m.ID == "" {
				continue
			}
			members[m.ID] = true
			if m.Advertise != "" {
				seeds[m.ID] = m.Advertise
			}
			if m.ID == cfg.Member.ID {
				selfInVersion = true
			}
		}
		name := cfg.Member.ID
		role := membergroup.RoleRegistry
		if !selfInVersion {
			name = fmt.Sprintf("observer.%s.%d", cfg.Member.ID, version.Version)
			role = membergroup.RoleObserver
		}
		selfAdvertise := cfg.SelfAdvertise()
		meta := membergroup.Meta{
			Role: role, ID: name, APIAdvertise: selfAdvertise,
			MemberlistAdvertise: selfAdvertise,
		}
		g, err := membergroup.New(membergroup.Options{
			Label: label, Name: name, Hub: r.hub, Seeds: seeds, Meta: meta, Log: r.log, TLSConfig: r.tlsCfg,
		})
		if err != nil {
			return err
		}
		gctx, cancel := context.WithCancel(ctx)
		mg := &managedMemberGroup{group: g, members: members, cancel: cancel}
		r.mu.Lock()
		r.groups[label] = mg
		r.mu.Unlock()
		go r.joinRegistryMembers(gctx, g, members)
	}
	r.mu.Lock()
	for label, mg := range r.groups {
		if desired[label] {
			continue
		}
		mg.cancel()
		_ = mg.group.Shutdown()
		delete(r.groups, label)
	}
	r.mu.Unlock()
	return nil
}

func (r *registryMemberRuntime) WaitReady(ctx context.Context, cfg *clustercfg.RegistryConfig) error {
	if r == nil {
		return nil
	}
	waitCtx := ctx
	cancel := func() {}
	if timeout := cfg.Membership.ReloadReadyTimeoutDur(); timeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	ticker := time.NewTicker(registryMembershipReadyPoll)
	defer ticker.Stop()
	for {
		missing := r.missingOwnerMembers(cfg.Membership.OwnerVersions())
		if len(missing) == 0 {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("registry memberlist barrier: %w; missing %s", waitCtx.Err(), strings.Join(missing, ","))
		case <-ticker.C:
		}
	}
}

func (r *registryMemberRuntime) missingOwnerMembers(versions []clustercfg.MembershipVersion) []string {
	var missing []string
	for _, version := range versions {
		mg, ok := r.group(version.Label)
		if !ok {
			missing = append(missing, version.Label+":<group>")
			continue
		}
		for _, member := range version.Members {
			if member.ID == "" {
				continue
			}
			if !mg.group.Alive(member.ID) {
				missing = append(missing, version.Label+":"+member.ID)
			}
		}
	}
	return missing
}

func (r *registryMemberRuntime) Shutdown() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for label, mg := range r.groups {
		mg.cancel()
		_ = mg.group.Shutdown()
		delete(r.groups, label)
	}
}

func (r *registryMemberRuntime) group(label string) (*managedMemberGroup, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.groups[label]
	return g, ok
}

func (r *registryMemberRuntime) joinRegistryMembers(ctx context.Context, g *membergroup.Group, members map[string]bool) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	join := func() {
		ids := make([]string, 0, len(members))
		for id := range members {
			if id != r.selfID {
				ids = append(ids, id)
			}
		}
		if _, err := g.Join(ids...); err != nil && r.log != nil {
			r.log.Debug("registry memberlist join", "label", g.Label(), "err", err)
		}
	}
	join()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			join()
		}
	}
}

func (r *registryMemberRuntime) AliveAny(id string) bool {
	if id == "" || id == r.selfID {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := false
	for _, mg := range r.groups {
		if !mg.members[id] {
			continue
		}
		if mg.group.Alive(id) {
			return true
		}
		if mg.group.Seen(id) {
			seen = true
		}
	}
	return !seen
}

type scalerObserverRuntime struct {
	mu    sync.Mutex
	group *membergroup.Group
	label string
}

func newScalerObserverRuntime(cfg *clustercfg.RegistryConfig, hub *membergroup.Hub, log *slog.Logger) (*scalerObserverRuntime, error) {
	tlsCfg, err := membergroupTLSConfig(cfg.Member.TLS)
	if err != nil {
		return nil, fmt.Errorf("scaler observer memberlist tls: %w", err)
	}
	label := cfg.ScaleLink.ScalerLabel
	name := "observer." + cfg.Member.ID
	g, err := membergroup.New(membergroup.Options{
		Label: label, Name: name, Hub: hub, Log: log, TLSConfig: tlsCfg,
		Meta: membergroup.Meta{
			Role: membergroup.RoleObserver, ID: name,
			APIAdvertise: cfg.SelfAdvertise(), MemberlistAdvertise: cfg.SelfAdvertise(),
		},
	})
	if err != nil {
		return nil, err
	}
	return &scalerObserverRuntime{group: g, label: label}, nil
}

func (s *scalerObserverRuntime) JoinSeed(_ context.Context, id, label, advertise string) error {
	if s == nil || s.group == nil {
		return fmt.Errorf("scaler observer is not initialized")
	}
	if label == "" {
		label = s.label
	}
	if label != s.label {
		return fmt.Errorf("scaler label %q does not match %q", label, s.label)
	}
	if id == "" || advertise == "" {
		return fmt.Errorf("scaler seed id and advertise are required")
	}
	s.mu.Lock()
	s.group.AddSeed(id, advertise)
	_, err := s.group.Join(id)
	s.mu.Unlock()
	return err
}

func (s *scalerObserverRuntime) ReadyScalers(readyLabel string) []registry.ScalerPeer {
	if s == nil || s.group == nil {
		return nil
	}
	metas := s.group.ReadyScalers(readyLabel)
	out := make([]registry.ScalerPeer, 0, len(metas))
	for _, meta := range metas {
		out = append(out, registry.ScalerPeer{ID: meta.ID, Advertise: meta.APIAdvertise, ReadyLabel: meta.ReadyLabel})
	}
	return out
}

func membergroupTLSConfig(material clustercfg.TLS) (*tls.Config, error) {
	if material.Cert == "" && material.Key == "" && material.CA == "" {
		return nil, nil
	}
	return material.ClientConfig("")
}
