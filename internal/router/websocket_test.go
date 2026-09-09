package router

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

func TestClusterWebSocketUsesExistingConnectRelay(t *testing.T) {
	for _, response := range []string{"upgrade", "inner rejection", "invalid upgrade"} {
		t.Run(response, func(t *testing.T) {
			guest, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer guest.Close()
			guestDone := make(chan error, 1)
			go func() {
				conn, err := guest.Accept()
				if err != nil {
					guestDone <- err
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				reader := bufio.NewReader(conn)
				r, err := http.ReadRequest(reader)
				if err == nil {
					defer r.Body.Close()
					if r.Host != "8080-stable-s1-g0.test.local" || r.Header.Get(proxypkg.HeaderSandboxID) != "stable-s1-g0" || r.Header.Get("Connection") != "Upgrade" || r.Header.Get("Sec-WebSocket-Protocol") != "chat" || r.URL.Path != "/ws" {
						err = fmt.Errorf("inner identity or handshake changed: host=%q headers=%v", r.Host, r.Header)
					} else {
						switch response {
						case "upgrade":
							_, err = io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Protocol: chat\r\n\r\n\x81\x05hello")
							if err == nil {
								var input []byte
								input, err = io.ReadAll(reader)
								if err == nil && string(input) != "\x81\x84\x01\x02\x03\x04qkmc" {
									err = fmt.Errorf("inner buffered frame=%x", input)
								}
								if err == nil {
									_, err = io.WriteString(conn, "\x82\x04tail\x88\x00")
								}
							}
						case "inner rejection":
							_, err = io.WriteString(conn, "HTTP/1.1 401 Unauthorized\r\nX-Kuasar-Proxy-Error: unauthorized\r\nContent-Length: 6\r\n\r\ndenied")
						case "invalid upgrade":
							_, err = io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n")
						}
					}
				}
				guestDone <- err
			}()
			workerStats := proxystats.NewWorkerStats()
			masterStats := proxystats.NewMasterStats(metrics.New(), []string{"node-worker"})
			if err := masterStats.BeginWorker("node-worker", 1); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if _, err := workerStats.StartSender(ctx, "node-worker", 1, func(frame proxystats.Frame) error {
				return masterStats.Receive("node-worker", 1, frame)
			}); err != nil {
				t.Fatal(err)
			}
			release := make(chan struct{})
			close(release)
			nodeRouter := &clusterTrafficNodeRouter{
				binding: proxypkg.BindRoute("stable-s1-g0", "stable-s1", types.ProfileBare, "", "forward-token", proxypkg.LegacyTarget(8080)),
				route:   proxypkg.Route{Kind: proxypkg.KindTCP, Addr: guest.Addr().String()}, activateSeen: make(chan struct{}), activateRelease: release,
			}
			nodeProxy := proxypkg.New(nodeRouter, nil, nil, workerStats).WithTrafficTracker(workerStats)
			var connects, resolves atomic.Int32
			nodeDone := make(chan struct{}, 1)
			node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodConnect || r.Header.Get(proxypkg.HeaderSandboxID) != "stable-s1-g0" || r.Header.Get(HeaderAccessTok) != "forward-token" || connects.Add(1) != 1 {
					http.Error(w, "unexpected second-hop request", http.StatusBadRequest)
					return
				}
				nodeProxy.ServeHTTP(w, r)
				nodeDone <- struct{}{}
			}))
			defer node.Close()
			route := routerTestRouteResolve(t, "stable-s1", "/g", "rk", strings.TrimPrefix(node.URL, "http://"), types.ProfileBare)
			route.ForwardAccessToken = "forward-token"
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/route-link/route" {
					resolves.Add(1)
					_ = json.NewEncoder(w).Encode(route)
				} else {
					http.Error(w, "unexpected route refresh", http.StatusInternalServerError)
				}
			}))
			defer control.Close()
			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
			rt.SetDataPlaneAuth("enforce")
			handlerDone := make(chan struct{})
			front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				rt.Handler().ServeHTTP(w, r)
				close(handlerDone)
			}))
			defer front.Close()
			conn, err := net.Dial("tcp", front.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			wire := fmt.Sprintf("GET /ws HTTP/1.1\r\nHost: 8080-stable-s1.test.local\r\n%s: stable-s1\r\n%s: /g\r\n%s: rk\r\n%s: forward-token\r\nConnection: keep-alive, Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Protocol: chat\r\n\r\n", proxypkg.HeaderSandboxID, HeaderGroup, HeaderRouteKey, HeaderAccessTok)
			if response == "upgrade" {
				wire += "\x81\x84\x01\x02\x03\x04qkmc"
			}
			if _, err := io.WriteString(conn, wire); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(conn)
			resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if response == "upgrade" {
				if resp.StatusCode != 101 {
					t.Fatalf("handshake status=%d", resp.StatusCode)
				}
				waitClusterTraffic(t, masterStats, route.NodeSandboxID, 0, 1)
				rt.cacheMu.RLock()
				active := rt.active[activeSIDKey("/g", "rk", "stable-s1")]
				held := active != nil && active.refs == 1
				rt.cacheMu.RUnlock()
				if !held {
					t.Fatal("active route was not held for WebSocket lifetime")
				}
				if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(reader)
				if err != nil || string(body) != "\x81\x05hello\x82\x04tail\x88\x00" {
					t.Fatalf("relayed frames=%x err=%v", body, err)
				}
			} else {
				want := http.StatusUnauthorized
				if response == "invalid upgrade" {
					want = http.StatusBadGateway
				}
				if resp.StatusCode != want {
					t.Fatalf("inner response=%d want=%d", resp.StatusCode, want)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
			}
			select {
			case err := <-guestDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("guest relay did not finish")
			}
			for _, done := range []<-chan struct{}{handlerDone, nodeDone} {
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("cluster relay did not finish")
				}
			}
			waitClusterTraffic(t, masterStats, route.NodeSandboxID, 0, 0)
			rt.cacheMu.RLock()
			active := len(rt.active)
			rt.cacheMu.RUnlock()
			if active != 0 || resolves.Load() != 1 || connects.Load() != 1 || rt.cachedRoute("/g", "rk", "stable-s1") == nil {
				t.Fatalf("unexpected cleanup/replay: active=%d resolves=%d connects=%d", active, resolves.Load(), connects.Load())
			}
		})
	}
}
