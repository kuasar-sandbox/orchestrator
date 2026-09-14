package router

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	proxypkg "github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type clusterTrafficNodeRouter struct {
	binding         proxypkg.RouteBinding
	route           proxypkg.Route
	activateSeen    chan struct{}
	activateRelease <-chan struct{}
}

func (r *clusterTrafficNodeRouter) LookupRoute(_ context.Context, sandboxID string, target proxypkg.ConnectTarget) (proxypkg.RouteBinding, bool, error) {
	if sandboxID != r.binding.SandboxID || target != r.binding.Target {
		return proxypkg.RouteBinding{}, false, nil
	}
	return r.binding, true, nil
}

func (r *clusterTrafficNodeRouter) ActivateRoute(_ context.Context, expected proxypkg.RouteBinding) (proxypkg.Route, bool, error) {
	if expected != r.binding {
		return proxypkg.Route{}, false, nil
	}
	close(r.activateSeen)
	<-r.activateRelease
	return r.route, true, nil
}

func TestClusterKnownTargetCountsOnlyFinalNodeWorker(t *testing.T) {
	workerStats := proxystats.NewWorkerStats()
	masterStats := proxystats.NewMasterStats(metrics.New(), []string{"node-worker"})
	if err := masterStats.BeginWorker("node-worker", 1); err != nil {
		t.Fatal(err)
	}
	statsCtx, cancelStats := context.WithCancel(context.Background())
	defer cancelStats()
	if _, err := workerStats.StartSender(statsCtx, "node-worker", 1, func(frame proxystats.Frame) error {
		return masterStats.Receive("node-worker", 1, frame)
	}); err != nil {
		t.Fatal(err)
	}

	activateRelease := make(chan struct{})
	nodeRouter := &clusterTrafficNodeRouter{
		activateSeen:    make(chan struct{}),
		activateRelease: activateRelease,
	}
	backend, proxySide := net.Pipe()
	backendReceived := make(chan struct{})
	backendDone := make(chan error, 1)
	backendRelease := make(chan struct{})
	go func() {
		defer backend.Close()
		request, err := http.ReadRequest(bufio.NewReader(backend))
		if err != nil {
			backendDone <- err
			return
		}
		_ = request.Body.Close()
		close(backendReceived)
		<-backendRelease
		_, err = io.WriteString(backend, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		backendDone <- err
	}()
	var dials atomic.Int32
	nodeProxy := proxypkg.NewWithDialer(nodeRouter, func() string { return "enforce" }, nil, workerStats,
		func(context.Context, proxypkg.Route) (net.Conn, error) {
			if dials.Add(1) != 1 {
				return nil, fmt.Errorf("unexpected repeated final backend dial")
			}
			return proxySide, nil
		}, "").WithTrafficTracker(workerStats)
	node := httptest.NewServer(nodeProxy)
	defer node.Close()

	route := routerTestRouteResolve(t, "stable-s1", "/g", "rk", strings.TrimPrefix(node.URL, "http://"), types.ProfileBare)
	route.State = "paused"
	route.ForwardAccessToken = "forward-token"
	target := proxypkg.LegacyTarget(8080)
	nodeRouter.binding = proxypkg.BindRoute(route.NodeSandboxID, route.StableID, types.ProfileBare, "envd-token", route.ForwardAccessToken, target)
	nodeRouter.route = proxypkg.Route{Kind: proxypkg.KindTCP, Addr: "final-backend"}

	var reserveHits atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/route-link/route":
			_ = json.NewEncoder(w).Encode(route)
		case "/route-link/reserve":
			reserveHits.Add(1)
			http.Error(w, "known target must not reserve", http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetDataPlaneAuth("enforce")
	front := httptest.NewServer(rt.Handler())
	defer front.Close()

	request, err := http.NewRequest(http.MethodGet, front.URL+"/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Host = "8080-stable-s1.test.local"
	request.Header.Set(HeaderGroup, "/g")
	request.Header.Set(HeaderRouteKey, "rk")
	request.Header.Set(HeaderAccessTok, route.ForwardAccessToken)
	responseDone := make(chan error, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			defer response.Body.Close()
			if response.StatusCode != http.StatusNoContent {
				err = fmt.Errorf("cluster response status=%d", response.StatusCode)
			}
		}
		responseDone <- err
	}()

	select {
	case <-nodeRouter.activateSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("known paused target did not reach final node activation")
	}
	waitClusterTraffic(t, masterStats, route.NodeSandboxID, 1, 0)
	close(activateRelease)
	select {
	case <-backendReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("cluster request did not reach final backend")
	}
	waitClusterTraffic(t, masterStats, route.NodeSandboxID, 0, 1)
	close(backendRelease)
	if err := <-backendDone; err != nil {
		t.Fatal(err)
	}
	if err := <-responseDone; err != nil {
		t.Fatal(err)
	}
	waitClusterTraffic(t, masterStats, route.NodeSandboxID, 0, 0)
	if reserveHits.Load() != 0 || dials.Load() != 1 {
		t.Fatalf("reserve hits=%d final dials=%d, want 0/1", reserveHits.Load(), dials.Load())
	}
}

func waitClusterTraffic(t *testing.T, master *proxystats.MasterStats, sandboxID string, parking, egress uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := master.SandboxTrafficStats(context.Background(), sandboxID, "run-1", types.ProfileBare, types.StateRunning)
		if err == nil && stats.Inflight.Parking == parking && stats.Inflight.Connected == egress {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("cluster final-node traffic did not reach parking=%d egress=%d", parking, egress)
}
