package proxyapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestZeroTimeoutExplicitAndAutomaticStop(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		t.Run(fmt.Sprint(automatic), func(t *testing.T) {
			effective, _ := testMasterEffective(t)
			cfg := effective.Config()
			cfg.ShutdownTimeout, cfg.DataListen = "", "fd=data"
			dir, err := os.MkdirTemp("", "proxy-zero-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			cfg.ConfigSocket, cfg.StatsSocket, cfg.ShmPath = filepath.Join(dir, "config.sock"), filepath.Join(dir, "stats.sock"), filepath.Join(dir, "routes.shm")
			serveFixtureRoutes(t, cfg.ConfigSocket, false)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			file, err := listenerFile(listener)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			effective, err = FreezeConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			service := NewService()
			runCtx, cancelRun := context.WithCancel(context.Background())
			defer cancelRun()
			done := make(chan error, 1)
			go func() {
				done <- service.Run(runCtx, ServiceInput{}, nil, func(context.Context, ServiceInput) (*Startup, error) {
					return &Startup{Effective: effective, Runtime: &Runtime{Logger: testMasterLogger(), ListenerFiles: map[string]*os.File{"data": file}}}, nil
				}, RunMaster)
			}()
			wait, stopWait := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopWait()
			if err := service.WaitReady(wait); err != nil {
				t.Fatal(err)
			}
			if _, err := service.InplaceRestart(wait); !errors.Is(err, ErrIncompatible) || !strings.Contains(err.Error(), "shutdown_timeout") {
				t.Fatalf("zero timeout restart: %v", err)
			}
			release := make(chan struct{})
			var once sync.Once
			finish := func() { once.Do(func() { close(release) }) }
			defer finish()
			entered := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				select {
				case <-release:
					fmt.Fprint(w, "complete\n")
				case <-r.Context().Done():
				}
			}))
			defer func() { finish(); upstream.Close() }()
			client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 5 * time.Second}
			req, _ := http.NewRequest("GET", "http://"+listener.Addr().String()+"/fixture/hold", nil)
			req.Header.Set("X-Fixture-Target", upstream.URL)
			response, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			select {
			case <-entered:
			case <-wait.Done():
				t.Fatal("upstream not entered")
			}
			if automatic {
				cancelRun()
			} else {
				caller, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
				err := service.Shutdown(caller)
				cancel()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("caller wait: %v", err)
				}
				select {
				case err := <-done:
					t.Fatalf("explicit zero-timeout drain ended early: %v", err)
				default:
				}
				if service.Status().InplaceRestart != nil {
					t.Fatal("unexpected operation")
				}
				finish()
			}
			body, readErr := io.ReadAll(response.Body)
			if !automatic && (readErr != nil || string(body) != "begin\ncomplete\n") {
				t.Fatalf("explicit drain truncated response: %q %v", body, readErr)
			}
			if automatic && readErr == nil {
				t.Fatalf("automatic zero-timeout stop silently completed held work: %q", body)
			}
			select {
			case err := <-done:
				if errors.Is(err, ErrForcedClose) != automatic || (!automatic && err != nil) {
					t.Fatalf("stop result: %v", err)
				}
			case <-wait.Done():
				t.Fatal("service did not end")
			}
			finish()
		})
	}
}
