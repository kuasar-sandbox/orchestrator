package main

import (
	"context"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
)

func TestProxyWorkerMetricsForwardToMaster(t *testing.T) {
	mx := metrics.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	go readMetricsLoop(ctx, r, mx)
	counter := &metricsPipeCounter{f: w, ch: make(chan string, 4)}
	go counter.run(ctx, nil)

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
