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

type trafficStatsProviderStub struct {
	stats   *api.TrafficStats
	err     error
	calls   int
	sid     string
	runID   string
	profile types.Profile
	state   types.State
}

func (p *trafficStatsProviderStub) SandboxTrafficStats(_ context.Context, sandboxID, runID string, profile types.Profile, state types.State) (*api.TrafficStats, error) {
	p.calls++
	p.sid, p.runID, p.profile, p.state = sandboxID, runID, profile, state
	return p.stats, p.err
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

func TestTrafficStatsAuthenticatesBeforeProviderAndPassesRunIdentity(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{
		ID: "traffic", RunID: "run-7", Profile: types.ProfileE2B, State: types.StatePaused,
		APISecret: strings.Repeat("6", 64), ManifestKey: strings.Repeat("7", 64),
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	provider := &trafficStatsProviderStub{stats: &api.TrafficStats{State: string(types.StatePaused)}}
	o.SetSandboxTrafficProvider(provider)

	if _, err := o.TrafficStats(context.Background(), sb.ID, mintTestAPIKey(t, strings.Repeat("8", 64))); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("wrong owner error = %v", err)
	}
	if provider.calls != 0 {
		t.Fatal("traffic provider was queried before ownership validation")
	}
	stats, err := o.TrafficStats(context.Background(), sb.ID, mintTestAPIKey(t, sb.APISecret))
	if err != nil || stats.State != string(types.StatePaused) || provider.calls != 1 {
		t.Fatalf("TrafficStats = %+v err=%v calls=%d", stats, err, provider.calls)
	}
	if provider.sid != sb.ID || provider.runID != sb.RunID || provider.profile != sb.Profile || provider.state != sb.State {
		t.Fatalf("provider identity = %q/%q/%q/%q", provider.sid, provider.runID, provider.profile, provider.state)
	}
}

func TestTrafficStatsDisabledAndProviderErrors(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{
		ID: "traffic-errors", RunID: "run-1", Profile: types.ProfileBare, State: types.StateRunning,
		APISecret: strings.Repeat("9", 64), ManifestKey: strings.Repeat("a", 64),
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	apiKey := mintTestAPIKey(t, sb.APISecret)
	if _, err := o.TrafficStats(context.Background(), sb.ID, apiKey); !errors.Is(err, api.ErrStatsUnsupported) {
		t.Fatalf("disabled provider error = %v", err)
	}
	provider := &trafficStatsProviderStub{err: api.ErrStatsUnavailable}
	o.SetSandboxTrafficProvider(provider)
	if _, err := o.TrafficStats(context.Background(), sb.ID, apiKey); !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatalf("provider error = %v", err)
	}
}

func TestTrafficStatsTerminalStateConflictsBeforeProvider(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{
		ID: "traffic-dead", RunID: "run-1", Profile: types.ProfileBare, State: types.StateDead,
		APISecret: strings.Repeat("b", 64), ManifestKey: strings.Repeat("c", 64),
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	provider := &trafficStatsProviderStub{}
	o.SetSandboxTrafficProvider(provider)
	if _, err := o.TrafficStats(context.Background(), sb.ID, mintTestAPIKey(t, sb.APISecret)); !errors.Is(err, api.ErrStatsConflict) {
		t.Fatalf("terminal state error = %v, want ErrStatsConflict", err)
	}
	if provider.calls != 0 {
		t.Fatalf("terminal state reached provider %d times", provider.calls)
	}
}
