package conductorapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/orch"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

type startupExtensionFunc func(context.Context, conductorextension.Host) error

func (f startupExtensionFunc) Start(ctx context.Context, host conductorextension.Host) error {
	return f(ctx, host)
}

type startupTrafficProvider struct{}

func (startupTrafficProvider) SandboxTrafficStats(context.Context, string, string, types.Profile, types.State) (*api.TrafficStats, error) {
	return &api.TrafficStats{State: "running"}, nil
}

type startupResourceProvider struct{}

func (startupResourceProvider) SandboxResourceStats(string) (api.ResourceStats, bool) {
	reserved := uint64(4096)
	return api.ResourceStats{MemoryReserved: &reserved}, true
}

func TestExtensionStatsWaitsForProviderWiringWithoutBlockingStart(t *testing.T) {
	storage := extensionTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sb := &types.Sandbox{ID: "stats-startup", State: types.StateRunning, Profile: types.ProfileBare,
		RunDir: t.TempDir(), APISecret: strings.Repeat("1", 64), ManifestKey: strings.Repeat("2", 64), ServiceSecret: strings.Repeat("3", 64)}
	var err error
	sb.ForwardAccessToken, err = keys.MintForwardAccessToken(sb.ServiceSecret, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	var ownerReads atomic.Int64
	owner := &ctl.Server{Path: filepath.Join(sb.RunDir, "ctl.sock"), ResourceStatsHandler: func(ctl.Request) (ctl.Response, error) {
		ownerReads.Add(1)
		used, stamp := uint64(1), int64(123)
		return ctl.Response{ResourceStats: &ctl.ResourceStats{SandboxID: sb.ID, CPUCapacity: 2, CPUAllocatable: 1,
			MemoryCapacity: 1 << 30, MemoryHeadroom: 256 << 20, MemoryUsed: &used, TimestampUnix: &stamp}}, nil
	}, SnapshotHandler: func(ctl.Request) (ctl.Response, error) { return ctl.Response{}, errors.New("unexpected snapshot") }}
	if err := owner.Listen(); err != nil {
		t.Fatal(err)
	}
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- owner.Serve(ctx) }()
	defer func() { cancel(); <-ownerDone }()
	core := orch.NewResolved(&config.Conductor{}, storage, nil, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := conductorextension.StatsRequest{SandboxIDs: []string{sb.ID}, Sections: []string{"resource", "traffic"}}
	entered, finished := make(chan struct{}, 8), make(chan error, 8)
	extension := startupExtensionFunc(func(ctx context.Context, host conductorextension.Host) error {
		if rows, err := host.Stats().ReadStats(ctx, request); rows != nil || !errors.Is(err, api.ErrStatsUnavailable) {
			return fmt.Errorf("Start read exposed partial wiring: rows=%v error=%v", rows, err)
		}
		for range 8 {
			go func() {
				if _, err := host.Stats().ReadStats(ctx, request); !errors.Is(err, api.ErrStatsUnavailable) {
					finished <- fmt.Errorf("early asynchronous read: %v", err)
					entered <- struct{}{}
					return
				}
				entered <- struct{}{}
				for ctx.Err() == nil {
					rows, err := host.Stats().ReadStats(ctx, request)
					if errors.Is(err, api.ErrStatsUnavailable) {
						runtime.Gosched()
						continue
					}
					if err != nil || len(rows) != 1 || rows[0].Resource == nil || rows[0].Resource.MemoryReserved == nil ||
						*rows[0].Resource.MemoryReserved != 4096 || rows[0].Traffic == nil || rows[0].Traffic.State != "running" {
						finished <- fmt.Errorf("wired read: rows=%+v error=%v", rows, err)
						return
					}
					finished <- nil
					return
				}
				finished <- ctx.Err()
			}()
		}
		return nil
	})
	started, err := startExtension(ctx, &Runtime{Extension: extension}, storage, core)
	if err != nil {
		t.Fatal(err)
	}
	for range 8 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if ownerReads.Load() != 0 {
		t.Fatal("extension queried native sources before provider wiring")
	}
	// Reads are already running while these formerly racy setters execute.
	core.SetSandboxResourceProvider(startupResourceProvider{})
	core.SetSandboxTrafficProvider(startupTrafficProvider{})
	close(started.statsReady)
	for range 8 {
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
	}
	if ownerReads.Load() != 8 {
		t.Fatalf("real native owner reads: %d", ownerReads.Load())
	}
}
