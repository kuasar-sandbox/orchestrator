package proxyapp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTakeoverAllowsLocalPolicyChanges(t *testing.T) {
	cfg := testProxyConfig(t)
	next := cfg.Clone()
	next.Auth = "off"
	next.ParkTimeout = "17s"
	if !compatibleTakeover(cfg, next) {
		t.Fatal("local policy changes unnecessarily rejected takeover")
	}
	next.Workers++
	if compatibleTakeover(cfg, next) {
		t.Fatal("worker contract change accepted")
	}
	next = cfg.Clone()
	next.TLS.Cert, next.TLS.Key = "certificate.pem", "key.pem"
	if compatibleTakeover(cfg, next) {
		t.Fatal("TLS mode change accepted")
	}
	cfg.TLS.Cert, cfg.TLS.Key = "old.pem", "old-key.pem"
	if !compatibleTakeover(cfg, next) {
		t.Fatal("certificate path change unnecessarily rejected takeover")
	}
}

func TestOccupiedMetricsAllowsOrdinaryServiceButRejectsRestart(t *testing.T) {
	effective, _ := testMasterEffective(t)
	cfg := effective.Config()
	dir, err := os.MkdirTemp("", "proxy-metrics-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	cfg.ConfigSocket, cfg.StatsSocket, cfg.ShmPath = filepath.Join(dir, "config.sock"), filepath.Join(dir, "stats.sock"), filepath.Join(dir, "routes.shm")
	cfg.ShutdownTimeout, cfg.DataListen = "2s", "fd=data"
	serveFixtureRoutes(t, cfg.ConfigSocket, false)
	metrics, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer metrics.Close()
	cfg.MetricsListen = metrics.Addr().String()
	data, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	file, err := listenerFile(data)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	effective, err = FreezeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService()
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- service.Run(runCtx, ServiceInput{}, nil, func(context.Context, ServiceInput) (*Startup, error) {
			return &Startup{Effective: effective, Runtime: &Runtime{Logger: testMasterLogger(), ListenerFiles: map[string]*os.File{"data": file}}}, nil
		}, RunMaster)
	}()
	wait, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWait()
	if err := service.WaitReady(wait); err != nil {
		t.Fatal(err)
	}
	if id, err := service.InplaceRestart(wait); id != "" || !errors.Is(err, ErrIncompatible) || !strings.Contains(err.Error(), "metrics listener") {
		t.Fatalf("missing metrics preflight: %s %v", id, err)
	}
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: time.Second}
	response, err := client.Get("http://" + data.Addr().String() + "/fixture/id")
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || !service.Status().Ready || service.Status().InplaceRestart != nil {
		t.Fatalf("ordinary service lost business: status=%d err=%v service=%+v", response.StatusCode, err, service.Status())
	}
	if err := service.Shutdown(wait); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestUnboundedStopSurvivesContinuationAndDuplicate(t *testing.T) {
	record, err := newStopRecord()
	if err != nil {
		t.Fatal(err)
	}
	defer record.file.Close()
	if _, requested, err := record.read(); requested || err != nil {
		t.Fatalf("new record: %v %v", requested, err)
	}
	if deadline, err := record.request(time.Time{}); !deadline.IsZero() || err != nil {
		t.Fatalf("unbounded request: %v %v", deadline, err)
	}
	continued, err := receiveStopRecord(record.file)
	if err != nil {
		t.Fatal(err)
	}
	if deadline, requested, err := continued.read(); !requested || !deadline.IsZero() || err != nil {
		t.Fatalf("restored request: %v %v %v", deadline, requested, err)
	}
	if deadline, err := continued.request(time.Now()); !deadline.IsZero() || err != nil {
		t.Fatalf("duplicate changed unbounded request: %v %v", deadline, err)
	}
}
