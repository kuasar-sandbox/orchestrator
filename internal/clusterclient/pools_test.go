package clusterclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
)

func poolMembership(address string) clustercfg.MembershipConfig {
	return clustercfg.MembershipConfig{Active: 1, Owners: clustercfg.MembershipOwnerConfig{RouteLink: 1, NodeLink: 1, NodeList: 1, PlacerLink: 1}, Versions: []clustercfg.MembershipVersion{{Version: 1, Members: []clustercfg.MembershipMember{{ID: "a", Advertise: address}}}}}
}

func poolServer(t *testing.T, chunked bool) (*httptest.Server, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	var created, closed atomic.Int64
	var address string
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if chunked {
			w.(http.Flusher).Flush()
		}
		json.NewEncoder(w).Encode(poolMembership(address))
		if chunked {
			w.(http.Flusher).Flush()
			fmt.Fprint(w, strings.Repeat(" ", 2048))
		}
	}))
	s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			created.Add(1)
		}
		if state == http.StateClosed {
			closed.Add(1)
		}
	}
	// Set before serving to avoid racing handlers.
	address = "http://" + s.Listener.Addr().String()
	s.Start()
	t.Cleanup(s.Close)
	return s, &created, &closed
}

func TestRegistryPoolRefresh(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		t.Run(fmt.Sprint(chunked), func(t *testing.T) {
			s, count, _ := poolServer(t, chunked)
			bootstrap := httptest.NewServer(s.Config.Handler)
			defer bootstrap.Close()
			r, err := NewRegistry(bootstrap.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 300; i++ {
				if err := r.Refresh(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("300 membership refreshes: accepted connections=%d", count.Load())
			if count.Load() != 1 {
				t.Fatalf("connections=%d want 1", count.Load())
			}
		})
	}
}

func TestRegistryPoolEndpointPaths(t *testing.T) {
	s, count, _ := poolServer(t, true)
	r, _ := NewRegistry(s.URL, nil)
	paths := []func(context.Context) ([]Endpoint, error){r.OwnerEndpoints, r.NodeListEndpoints, r.ActiveEndpoints, func(ctx context.Context) ([]Endpoint, error) { return r.RouteCandidates(ctx, "g") }, func(ctx context.Context) ([]Endpoint, error) { return r.NodeCandidates(ctx, "n") }, func(ctx context.Context) ([]Endpoint, error) { return r.PlacerLinkEndpoints(ctx, "g") }}
	for i := 0; i < 100; i++ {
		for _, path := range paths {
			eps, err := path(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			for _, ep := range eps {
				if _, err := fetchMembershipFrom(t.Context(), ep); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	t.Logf("600 endpoint requests plus bootstrap: accepted connections=%d", count.Load())
	if count.Load() != 1 {
		t.Fatal(count.Load())
	}
}

func TestRegistryPoolConcurrent(t *testing.T) {
	s, count, _ := poolServer(t, false)
	r, _ := NewRegistry(s.URL, nil)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 200; n++ {
				if err := r.Refresh(t.Context()); err != nil {
					t.Error(err)
					return
				}
				eps, err := r.OwnerEndpoints(t.Context())
				if err != nil {
					t.Error(err)
					return
				}
				for _, ep := range eps {
					if _, err := fetchMembershipFrom(t.Context(), ep); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("2 concurrent workers, 800 requests: accepted connections=%d", count.Load())
	// Transport may complete a speculative dial after an idle connection becomes available.
	if count.Load() > 4 {
		t.Fatal(count.Load())
	}
}

func awaitPool(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-deadline.C:
			t.Fatal("connection state deadline")
		case <-tick.C:
		}
	}
}

func TestRegistryPoolRetiresAddressAndKeepsStream(t *testing.T) {
	old, _, closed := poolServer(t, false)
	next, _, _ := poolServer(t, false)
	r, _ := NewRegistry(old.URL, nil)
	r.httpClient.Transport.(*http.Transport).IdleConnTimeout = 30 * time.Millisecond
	if err := r.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.membership = poolMembership(next.URL)
	r.mu.Unlock()
	eps, err := r.OwnerEndpoints(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fetchMembershipFrom(t.Context(), eps[0]); err != nil {
		t.Fatal(err)
	}
	awaitPool(t, func() bool { return closed.Load() == 1 })
	release := make(chan struct{})

	stream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprint(w, "first\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
			fmt.Fprint(w, "second\n")
		case <-req.Context().Done():
		}
	}))
	defer stream.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", stream.URL, nil)
	resp, err := r.httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 6)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		t.Fatal(err)
	}
	// A second finite connection's idle expiry synchronizes passage of the idle timeout.
	finite, _, finiteClosed := poolServer(t, false)
	ep := Endpoint{BaseURL: finite.URL, Client: r.httpClient}
	if _, err := fetchMembershipFrom(ctx, ep); err != nil {
		t.Fatal(err)
	}
	awaitPool(t, func() bool { return finiteClosed.Load() == 1 })
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	// Closing idle connections must not terminate an active response either.
	r.httpClient.CloseIdleConnections()
	close(release)
	rest, err := io.ReadAll(resp.Body)
	if err != nil || string(rest) != "second\n" {
		t.Fatalf("active stream: %q %v", rest, err)
	}
	cancel()
}

func TestRegistryPoolUnixIsolation(t *testing.T) {
	r, _ := NewRegistry("http://bootstrap", nil)
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "r.sock")
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		var count atomic.Int64
		expected := fmt.Sprint(i)
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.Host != "registry" {
				t.Errorf("host=%s", req.Host)
			}
			fmt.Fprint(w, expected)
		}), ConnState: func(_ net.Conn, s http.ConnState) {
			if s == http.StateNew {
				count.Add(1)
			}
		}}
		go srv.Serve(ln)
		t.Cleanup(func() { srv.Close() })
		for n := 0; n < 50; n++ {
			base, client, err := r.httpBase(path)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Get(base)
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || string(b) != expected {
				t.Fatalf("%s %v", b, err)
			}
		}
		if count.Load() != 1 {
			t.Fatal(count.Load())
		}
	}
}

func TestRegistryPoolTLSAndOwnership(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(poolMembership("")) }))
	defer s.Close()
	roots := x509.NewCertPool()
	roots.AddCert(s.Certificate())
	good, _ := NewRegistry(s.URL, &tls.Config{RootCAs: roots})
	if err := good.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	bad, _ := NewRegistry(s.URL, &tls.Config{RootCAs: x509.NewCertPool()})
	if err := bad.Refresh(t.Context()); err == nil {
		t.Fatal("TLS trust leaked between registries")
	}
	injected := s.Client()
	injected.Timeout = time.Hour
	r := NewRegistryWithClient(s.URL, injected)
	if err := r.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	eps, err := r.OwnerEndpoints(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if eps[0].Client != injected || injected.Timeout != time.Hour {
		t.Fatal("external client changed")
	}
	if good.client.Timeout != 0 || good.httpsClient.Transport.(*http.Transport).IdleConnTimeout != 90*time.Second {
		t.Fatal("transport defaults")
	}
}

func TestRegistryPoolUnixMembershipTargets(t *testing.T) {
	paths := []string{filepath.Join(t.TempDir(), "bootstrap.sock"), filepath.Join(t.TempDir(), "member.sock")}
	var hits [2]atomic.Int64
	for i, path := range paths {
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits[i].Add(1)
			json.NewEncoder(w).Encode(poolMembership(paths[1]))
		})}
		go srv.Serve(ln)
		t.Cleanup(func() { srv.Close() })
	}
	r, err := NewRegistry(paths[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := r.Refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if hits[0].Load() != 3 || hits[1].Load() != 2 {
		t.Fatalf("Unix fetch targets: %d %d", hits[0].Load(), hits[1].Load())
	}
}

func TestDecodeResponseDoesNotDrainInvalidOrUnboundedTail(t *testing.T) {
	var out any
	if err := DecodeResponse(io.NopCloser(strings.NewReader("invalid")), &out); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	// An oversized tail must terminate after the bounded consumption budget.
	if err := DecodeResponse(io.NopCloser(io.MultiReader(strings.NewReader("{}"), endlessSpaces{})), &out); err != nil {
		t.Fatal(err)
	}
}

type endlessSpaces struct{}

func (endlessSpaces) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = ' '
	}
	return len(p), nil
}

func TestDecodeResponseCancelsStalledTail(t *testing.T) {
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprint(w, "{}")
		w.(http.Flusher).Flush()
		close(started)
		<-req.Context().Done()
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL, nil)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	<-started
	done := make(chan error, 1)
	go func() { var out any; done <- DecodeResponse(resp.Body, &out) }()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("drain ignored cancellation")
	}
}
