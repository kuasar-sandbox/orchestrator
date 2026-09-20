package orch

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

type resourceStatsProviderStub struct {
	stats        api.ResourceStats
	found        bool
	calls        int
	beforeReturn func()
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
	if p.beforeReturn != nil {
		p.beforeReturn()
	}
	return p.stats, p.found
}

func startResourceOwner(t *testing.T, sb *types.Sandbox, handler func(ctl.Request) (ctl.Response, error)) {
	t.Helper()
	server := &ctl.Server{Path: filepath.Join(sb.RunDir, "ctl.sock"), ResourceStatsHandler: handler,
		SnapshotHandler: func(ctl.Request) (ctl.Response, error) {
			t.Error("stats triggered snapshot")
			return ctl.Response{}, errors.New("unexpected snapshot")
		}}
	if err := server.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
}

func TestResourceStatsAuthenticatesAndCombinesNativeWithReservation(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{ID: "resource", StableIDValue: "alias", RunID: "run-1", RunDir: t.TempDir(),
		Profile: types.ProfileBare, State: types.StateRunning, APISecret: strings.Repeat("1", 64), ManifestKey: strings.Repeat("2", 64)}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	used, reserved, cpu, stamp := uint64(512<<20), uint64(1<<30), uint64(9007199254740993), int64(123)
	var calls atomic.Int64
	startResourceOwner(t, sb, func(ctl.Request) (ctl.Response, error) {
		calls.Add(1)
		return ctl.Response{ResourceStats: &ctl.ResourceStats{SandboxID: sb.ID, CPUCapacity: 2, CPUAllocatable: .5, MemoryCapacity: 4 << 30, MemoryHeadroom: 256 << 20,
			MemoryUsed: &used, CPUUsageUsec: &cpu, TimestampUnix: &stamp}}, nil
	})
	provider := &resourceStatsProviderStub{stats: api.ResourceStats{MemoryReserved: &reserved}, found: true}
	o.SetSandboxResourceProvider(provider)
	if _, err := o.ResourceStats(context.Background(), sb.ID, mintTestAPIKey(t, strings.Repeat("3", 64))); !errors.Is(err, api.ErrNotFound) {
		t.Fatal(err)
	}
	key := mintTestAPIKey(t, sb.APISecret)
	if _, err := o.ResourceStats(context.Background(), "alias", key); !errors.Is(err, api.ErrNotFound) {
		t.Fatal("StableID fallback", err)
	}
	if calls.Load() != 0 || provider.calls != 0 {
		t.Fatal("native source read before ownership validation")
	}
	for _, dynamic := range []bool{true, false} {
		if !dynamic {
			o.SetSandboxResourceProvider(nil)
		}
		stats, err := o.ResourceStats(context.Background(), sb.ID, key)
		if err != nil {
			t.Fatal(err)
		}
		if *stats.CPUCapacity != 2 || *stats.CPUAllocatable != .5 || *stats.MemoryCapacity != 4<<30 || *stats.MemoryHeadroom != 256<<20 || *stats.MemoryUsed != used || *stats.TimestampUnix != stamp {
			t.Fatalf("native resource semantics: %+v", stats)
		}
		if dynamic && (stats.MemoryReserved == nil || *stats.MemoryReserved != reserved) {
			t.Fatal("reservation lost")
		}
		if !dynamic && stats.MemoryReserved != nil {
			t.Fatal("static mode fabricated reservation")
		}
		if stats.CPUSeconds == nil || stats.CPUSeconds.String() != "9007199254.740993" {
			t.Fatal("CPU precision", stats.CPUSeconds)
		}
		raw, err := json.Marshal(stats)
		if err != nil || !strings.Contains(string(raw), `"cpuSeconds":9007199254.740993`) {
			t.Fatal(string(raw), err)
		}
		for _, old := range []string{"cpuCount", "memTotal", "memAllocatable", "runId", "run_id"} {
			if strings.Contains(string(raw), old) {
				t.Fatal("obsolete/private field", string(raw))
			}
		}
	}
}

func TestResourceStatsMissingRuntimeAndPaused(t *testing.T) {
	for _, state := range []types.State{types.StateStarting, types.StateRunning, types.StatePaused} {
		t.Run(string(state), func(t *testing.T) {
			o := testOrch(t)
			sb := &types.Sandbox{ID: "resource-" + string(state), Profile: types.ProfileBare, State: state,
				TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
				APISecret:  strings.Repeat("4", 64), ManifestKey: strings.Repeat("5", 64)}
			if state == types.StateStarting {
				sb.LaunchMode = types.LaunchImage
			}
			if state == types.StatePaused {
				sb.ResumeSource = types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("b", 64)}
			}
			materializeTestSandboxCredentials(t, sb)
			if err := o.st.Put(context.Background(), sb); err != nil {
				t.Fatal(err)
			}
			want := api.ErrStatsUnavailable
			if state == types.StatePaused {
				want = api.ErrStatsConflict
			}
			if _, err := o.ResourceStats(context.Background(), sb.ID, mintTestAPIKey(t, sb.APISecret)); !errors.Is(err, want) {
				t.Fatal(err, want)
			}
			current, err := o.st.Get(context.Background(), sb.ID)
			if err != nil || current.State != state {
				t.Fatal("resource query changed lifecycle", err)
			}
		})
	}
}

func TestResourceStatsRejectsChangedRuntimeBinding(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{ID: "changing", RunID: "old-run", RunDir: t.TempDir(), Profile: types.ProfileBare, State: types.StateRunning,
		APISecret: strings.Repeat("1", 64), ManifestKey: strings.Repeat("2", 64)}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	startResourceOwner(t, sb, func(ctl.Request) (ctl.Response, error) {
		replacement := *sb
		replacement.RunID = "new-run"
		if err := o.st.Put(context.Background(), &replacement); err != nil {
			return ctl.Response{}, err
		}
		return ctl.Response{ResourceStats: &ctl.ResourceStats{SandboxID: sb.ID, CPUCapacity: 2, CPUAllocatable: .5, MemoryCapacity: 4096, MemoryHeadroom: 1024}}, nil
	})
	if _, err := o.ResourceStats(context.Background(), sb.ID, mintTestAPIKey(t, sb.APISecret)); !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatal("old runtime published as current", err)
	}
}

func TestResourceStatsRejectsDeletedRowAfterOwnerRead(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{ID: "deleted-resource", RunID: "run", RunDir: t.TempDir(), Profile: types.ProfileBare, State: types.StateRunning,
		APISecret: strings.Repeat("1", 64), ManifestKey: strings.Repeat("2", 64)}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	startResourceOwner(t, sb, func(ctl.Request) (ctl.Response, error) {
		if err := o.st.Delete(context.Background(), sb.ID); err != nil {
			return ctl.Response{}, err
		}
		return ctl.Response{ResourceStats: &ctl.ResourceStats{SandboxID: sb.ID, CPUCapacity: 2, CPUAllocatable: .5, MemoryCapacity: 4096, MemoryHeadroom: 1024}}, nil
	})
	stats, err := o.ResourceStats(context.Background(), sb.ID, mintTestAPIKey(t, sb.APISecret))
	if stats != nil || !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatalf("deleted row published: %+v, %v", stats, err)
	}
}

func TestResourceStatsPostReadCancellationIsUnavailable(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{ID: "cancel-resource", RunID: "run", RunDir: t.TempDir(), Profile: types.ProfileBare, State: types.StateRunning,
		APISecret: strings.Repeat("1", 64), ManifestKey: strings.Repeat("2", 64)}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	startResourceOwner(t, sb, func(ctl.Request) (ctl.Response, error) {
		return ctl.Response{ResourceStats: &ctl.ResourceStats{SandboxID: sb.ID, CPUCapacity: 2, CPUAllocatable: .5, MemoryCapacity: 4096, MemoryHeadroom: 1024}}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	provider := &resourceStatsProviderStub{beforeReturn: cancel}
	o.SetSandboxResourceProvider(provider)
	stats, err := o.ResourceStats(ctx, sb.ID, mintTestAPIKey(t, sb.APISecret))
	if provider.calls != 1 || stats != nil || !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatalf("post-read cancellation: calls=%d stats=%+v err=%v", provider.calls, stats, err)
	}
	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	if err := o.statsBindingCurrent(expired, sb); !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatalf("post-read deadline: %v", err)
	}
}

func TestTrafficStatsAuthenticatesBeforeProviderAndPassesRunIdentity(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{
		ID: "traffic", RunID: "run-7", Profile: types.ProfileE2B, State: types.StatePaused,
		ResumeSource: types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://" + strings.Repeat("c", 64)},
		APISecret:    strings.Repeat("6", 64), ManifestKey: strings.Repeat("7", 64),
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
		ID: "traffic-dead", Profile: types.ProfileBare, State: types.StateDead,
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
