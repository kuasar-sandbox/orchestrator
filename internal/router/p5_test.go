package router

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestAuthModeOff: with router.auth=off, caller auth is skipped (front with an
// external gateway) — a bad key is NOT rejected.
func TestAuthModeOff(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/control/reserve" {
			_ = json.NewEncoder(w).Encode(reserveResult{NodeID: "n1", SID: "sb-1", AccessToken: "t", DataEndpoint: "10.0.0.1:1"})
			return
		}
		w.WriteHeader(http.StatusForbidden) // verify-key would reject
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sandboxes", nil)
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderAPIKey, "e2b_bad")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusForbidden {
		t.Fatal("auth=off should not reject a bad key")
	}
}

// TestServeDataByKey: a data-plane request carrying group + route-key headers (no
// prior create) Reserves and forwards to the node (router §4).
func TestServeDataByKey(t *testing.T) {
	var gotHost string
	var reserveHits int
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		w.WriteHeader(http.StatusNoContent)
	}))
	defer node.Close()
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/control/reserve":
			reserveHits++
			_ = json.NewEncoder(w).Encode(reserveResult{NodeID: "n1", SID: "sb-9", AccessToken: "tok", DataEndpoint: nodeHost})
		case "/control/verify-key":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetDataPlaneAuth("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req.Host = "data.test.local" // not api.<domain> → data plane; no <port>-<sid>
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderRouteKey, "u1:s1")
	req.Header.Set(HeaderAPIKey, "e2b_ok")
	req.Header.Set("E2b-Sandbox-Port", "49983")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("by-key forward status=%d, want 204", resp.StatusCode)
	}
	if gotHost != "49983-sb-9.test.local" {
		t.Fatalf("node saw Host=%q, want 49983-sb-9.test.local (synthesized <port>-<sid>)", gotHost)
	}

	req2, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req2.Host = "data.test.local"
	req2.Header.Set(HeaderGroup, "/g")
	req2.Header.Set(HeaderRouteKey, "u1:s1")
	req2.Header.Set(HeaderAPIKey, "e2b_ok")
	req2.Header.Set("E2b-Sandbox-Port", "49983")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("second by-key forward status=%d, want 204", resp2.StatusCode)
	}
	if reserveHits != 1 {
		t.Fatalf("reserve hits=%d, want 1 (second request should use route cache)", reserveHits)
	}
}

func TestServeDataByKeyUsesActiveRouteWhenRouteCacheEvicted(t *testing.T) {
	var reserveHits int32
	var nodeHits int32
	started := make(chan struct{})
	release := make(chan struct{})
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&nodeHits, 1) == 1 {
			close(started)
			<-release
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer node.Close()
	nodeHost := strings.TrimPrefix(node.URL, "http://")
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/control/reserve" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		atomic.AddInt32(&reserveHits, 1)
		_ = json.NewEncoder(w).Encode(reserveResult{NodeID: "n1", SID: "sb-9", AccessToken: "tok", DataEndpoint: nodeHost})
	}))
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rt.SetAuthMode("off")
	rt.SetDataPlaneAuth("off")
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	firstDone := make(chan error, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
		req.Host = "data.test.local"
		req.Header.Set(HeaderGroup, "/g")
		req.Header.Set(HeaderRouteKey, "u1:s1")
		req.Header.Set("E2b-Sandbox-Port", "49983")
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				err = fmt.Errorf("first status=%d", resp.StatusCode)
			}
		}
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first request did not reach node")
	}

	// Simulate ordinary route-cache churn while the first data-plane request is
	// still active. The active-route map must still carry this route.
	rt.cacheMu.Lock()
	delete(rt.cache, "sb-9")
	delete(rt.byKey, routeCacheKey("/g", "u1:s1"))
	rt.cacheMu.Unlock()

	req2, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req2.Host = "data.test.local"
	req2.Header.Set(HeaderGroup, "/g")
	req2.Header.Set(HeaderRouteKey, "u1:s1")
	req2.Header.Set("E2b-Sandbox-Port", "49983")
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("second status=%d, want 204", resp2.StatusCode)
	}
	if got := atomic.LoadInt32(&reserveHits); got != 1 {
		t.Fatalf("reserve hits=%d, want 1 (second request should use active route)", got)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}
