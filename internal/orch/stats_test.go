package orch

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type resourceStatsProviderStub struct {
	stats api.ResourceStats
	found bool
	calls int
}

func (p *resourceStatsProviderStub) SandboxResourceStats(string) (api.ResourceStats, bool) {
	p.calls++
	return p.stats, p.found
}

func TestResourceStatsAuthenticatesAndMapsControllerState(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{
		ID: "resource", Profile: types.ProfileBare, State: types.StateRunning,
		APISecret: strings.Repeat("1", 64), ManifestKey: strings.Repeat("2", 64),
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	apiKey := mintTestAPIKey(t, sb.APISecret)
	mem := uint64(128 << 20)
	provider := &resourceStatsProviderStub{stats: api.ResourceStats{MemUsed: &mem}, found: true}
	o.SetSandboxResourceProvider(provider)

	if _, err := o.ResourceStats(context.Background(), sb.ID, mintTestAPIKey(t, strings.Repeat("3", 64))); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("wrong owner error = %v", err)
	}
	if provider.calls != 0 {
		t.Fatal("provider was queried before ownership validation")
	}
	stats, err := o.ResourceStats(context.Background(), sb.ID, apiKey)
	if err != nil || stats.MemUsed == nil || *stats.MemUsed != mem || provider.calls != 1 {
		t.Fatalf("ResourceStats = %+v err=%v calls=%d", stats, err, provider.calls)
	}
}

func TestResourceStatsDisabledAndMissingReservationStates(t *testing.T) {
	for _, tc := range []struct {
		state types.State
		want  error
	}{
		{types.StateStarting, api.ErrStatsUnavailable},
		{types.StateRunning, api.ErrStatsUnavailable},
		{types.StatePaused, api.ErrStatsConflict},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			o := testOrch(t)
			sb := &types.Sandbox{
				ID: "resource-" + string(tc.state), Profile: types.ProfileBare, State: tc.state,
				APISecret: strings.Repeat("4", 64), ManifestKey: strings.Repeat("5", 64),
			}
			materializeTestSandboxCredentials(t, sb)
			if err := o.st.Put(context.Background(), sb); err != nil {
				t.Fatal(err)
			}
			apiKey := mintTestAPIKey(t, sb.APISecret)
			if _, err := o.ResourceStats(context.Background(), sb.ID, apiKey); !errors.Is(err, api.ErrStatsUnsupported) {
				t.Fatalf("disabled provider error = %v", err)
			}
			o.SetSandboxResourceProvider(&resourceStatsProviderStub{})
			if _, err := o.ResourceStats(context.Background(), sb.ID, apiKey); !errors.Is(err, tc.want) {
				t.Fatalf("missing reservation error = %v, want %v", err, tc.want)
			}
		})
	}
}
