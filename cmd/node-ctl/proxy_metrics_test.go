package main

import (
	"context"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestProxyWorkerMetricsForwardToMaster(t *testing.T) {
	mx := metrics.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	masterFile, workerFile, err := newSocketpair()
	if err != nil {
		t.Fatal(err)
	}
	defer masterFile.Close()
	workerConn, err := net.FileConn(workerFile)
	workerFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer workerConn.Close()
	master := proxystats.NewMasterStats(mx, []string{"proxy-0"})
	if err := master.BeginWorker("proxy-0", 1); err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() { readDone <- master.ReadWorkerStream(ctx, masterFile, "proxy-0", 1) }()
	counter := proxystats.NewWorkerStats()
	if _, err := counter.StartSender(ctx, "proxy-0", 1, proxystats.StreamSender(workerConn)); err != nil {
		t.Fatal(err)
	}

	counter.Inc(`data_requests_total{result="ok"}`)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rr := httptest.NewRecorder()
		mx.Handler().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
		if strings.Contains(rr.Body.String(), `data_requests_total{result="ok"} 1`) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	rr := httptest.NewRecorder()
	mx.Handler().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	t.Fatalf("metrics did not include forwarded counter:\n%s", rr.Body.String())
}

func TestProxyStatsStreamAcrossProcessBoundary(t *testing.T) {
	if os.Getenv("KUASAR_TEST_PROXY_STATS_CHILD") == "1" {
		runProxyStatsChild(t)
		return
	}
	masterFile, workerFile, err := newSocketpair()
	if err != nil {
		t.Fatal(err)
	}
	defer masterFile.Close()
	releaseR, releaseW, err := os.Pipe()
	if err != nil {
		workerFile.Close()
		t.Fatal(err)
	}
	defer releaseW.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestProxyStatsStreamAcrossProcessBoundary$")
	cmd.Env = append(os.Environ(), "KUASAR_TEST_PROXY_STATS_CHILD=1")
	cmd.ExtraFiles = []*os.File{workerFile, releaseR}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		workerFile.Close()
		releaseR.Close()
		t.Fatal(err)
	}
	workerFile.Close()
	releaseR.Close()

	mx := metrics.New()
	master := proxystats.NewMasterStats(mx, []string{"proxy-child"})
	if err := master.BeginWorker("proxy-child", 7); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readDone := make(chan error, 1)
	go func() { readDone <- master.ReadWorkerStream(ctx, masterFile, "proxy-child", 7) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stats, statsErr := master.SandboxTrafficStats(context.Background(), "s1", "run-1", types.ProfileBare, types.StateRunning)
		if statsErr == nil && stats.Inflight.Parking == 1 {
			recorder := httptest.NewRecorder()
			mx.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
			if strings.Contains(recorder.Body.String(), "child_requests_total 2") {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	stats, statsErr := master.SandboxTrafficStats(context.Background(), "s1", "run-1", types.ProfileBare, types.StateRunning)
	if statsErr != nil || stats.Inflight.Parking != 1 {
		t.Fatalf("cross-process traffic stats=%+v err=%v", stats, statsErr)
	}
	recorder := httptest.NewRecorder()
	mx.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(recorder.Body.String(), "child_requests_total 2") {
		t.Fatalf("cross-process absolute counter missing:\n%s", recorder.Body.String())
	}
	if _, err := releaseW.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("stats reader: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stats reader did not observe child exit")
	}
	master.WorkerExited("proxy-child", 7)
	if master.Available() {
		t.Fatal("exited worker contribution remained available")
	}
}

func runProxyStatsChild(t *testing.T) {
	statsFile := os.NewFile(3, "proxy-stats-child")
	if statsFile == nil {
		t.Fatal("missing inherited stats fd")
	}
	conn, err := net.FileConn(statsFile)
	statsFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	release := os.NewFile(4, "proxy-stats-release")
	if release == nil {
		t.Fatal("missing inherited release fd")
	}
	defer release.Close()
	ctx, cancel := context.WithCancel(context.Background())
	worker := proxystats.NewWorkerStats()
	done, err := worker.StartSender(ctx, "proxy-child", 7, proxystats.StreamSender(conn))
	if err != nil {
		t.Fatal(err)
	}
	worker.Inc("child_requests_total")
	worker.Inc("child_requests_total")
	_ = worker.BeginParking("s1", proxy.ConnectServiceForward)
	if _, err := io.ReadFull(release, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
