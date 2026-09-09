package proxy_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const websocketReply = "HTTP/1.1 101 Switching Protocols\r\nConnection: keep-alive, UpGrAdE\r\nUpgrade: WebSocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\nSec-WebSocket-Protocol: chat\r\nSet-Cookie: guest=yes\r\nConnection: X-Hop\r\nX-Hop: remove\r\n\r\n"

func websocketHeaders(r *http.Request) {
	r.Header["Connection"] = []string{"keep-alive, X-Hop", "UpGrAdE"}
	r.Header.Set("Upgrade", "WebSocket")
	r.Header.Set("X-Hop", "remove")
	r.Header.Set("Proxy-Authorization", "remove")
	r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	r.Header.Set("Sec-WebSocket-Version", "13")
	r.Header.Set("Sec-WebSocket-Protocol", "chat")
	r.Header.Set("Origin", "https://client.example")
	r.Header.Set("Cookie", "session=test")
	r.Header.Set("Authorization", "Bearer guest-test")
}

func websocketBackend(t *testing.T, serve func(net.Conn, *bufio.Reader, *http.Request) error) (string, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		reader := bufio.NewReader(conn)
		r, err := http.ReadRequest(reader)
		if err == nil {
			defer r.Body.Close()
			err = serve(conn, reader, r)
		}
		done <- err
	}()
	return listener.Addr().String(), done
}

func awaitWebsocket(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WebSocket transport did not finish")
	}
}

type unwrapHTTPWriter struct{ http.ResponseWriter }

func (w unwrapHTTPWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type prefetchUpgradeWriter struct {
	http.ResponseWriter
	count int
}

func (w prefetchUpgradeWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		_, err = rw.Reader.Peek(w.count)
	}
	return conn, rw, err
}

func TestWebSocketCanonicalAndPrivateTrafficLifetime(t *testing.T) {
	for _, limited := range []bool{false, true} {
		for _, private := range []bool{false, true} {
			t.Run(fmt.Sprintf("limited=%v/private=%v", limited, private), func(t *testing.T) {
				harness := newLimitedTrafficHarness(t)
				binding := proxy.BindRoute("node-s1", "stable-s1", types.ProfileBare, "", "forward", proxy.LegacyTarget(8080))
				if limited {
					binding.Admission = harness.bind(t, binding.SandboxID, publicconfig.MaxInflight{Forward: 1})
				}
				clientFrame := []byte("\x81\x84\x01\x02\x03\x04qkmc") // masked "ping"
				firstFrame := []byte("\x81\x05hello")
				tail := []byte("\x82\x04tail\x88\x00")
				addr, backendDone := websocketBackend(t, func(conn net.Conn, reader *bufio.Reader, r *http.Request) error {
					wantHost, wantPath := "8080-node-s1.example", "/ws"
					if private {
						wantHost, wantPath = "guest.internal", "/guest"
					}
					if r.Close || r.Host != wantHost || r.URL.Path != wantPath || r.Header.Get("Connection") != "Upgrade" || r.Header.Get("Upgrade") != "websocket" || r.Header.Get("X-Hop") != "" || r.Header.Get("Proxy-Authorization") != "" {
						return fmt.Errorf("incorrect guest request: host=%q path=%q headers=%v", r.Host, r.URL.Path, r.Header)
					}
					for key, want := range map[string]string{
						"Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==", "Sec-WebSocket-Version": "13", "Sec-WebSocket-Protocol": "chat",
						"Origin": "https://client.example", "Cookie": "session=test", "Authorization": "Bearer guest-test",
					} {
						if r.Header.Get(key) != want {
							return fmt.Errorf("guest lost %s", key)
						}
					}
					if _, err := conn.Write(append([]byte("HTTP/1.1 103 Early Hints\r\n\r\n"+websocketReply), firstFrame...)); err != nil {
						return err
					}
					got, err := io.ReadAll(reader)
					if err != nil || !bytes.Equal(got, clientFrame) {
						return fmt.Errorf("guest frames=%x err=%v", got, err)
					}
					// The tail is only available after downstream's clean TCP EOF.
					_, err = conn.Write(tail)
					return err
				})
				router := &admissionRouter{binding: binding, route: proxy.Route{Kind: proxy.KindTCP, Addr: addr}, found: true, activateOK: true}
				registry := metrics.New()
				px := proxy.New(router, nil, nil, registry).WithTrafficTracker(harness.traffic)
				var rewrites, revalidations atomic.Int32
				handlerDone := make(chan error, 1)
				forward := func(w http.ResponseWriter, r *http.Request) {
					if private {
						px.ForwardAuthorized(w, r, proxy.AuthorizedForwardRequest{
							SandboxID: binding.SandboxID, Target: binding.Target,
							Revalidate: func(context.Context) error { revalidations.Add(1); return nil },
							Rewrite: func(out *http.Request) error {
								rewrites.Add(1)
								out.Host, out.URL.Path = "guest.internal", "/guest"
								out.Header.Set("X-Hop", "rewrite-must-also-be-cleaned")
								return nil
							},
						})
					} else {
						px.ServeHTTP(w, r)
					}
				}
				front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					original := r.Clone(r.Context())
					w.Header().Set("X-Wrapper", "preserved")
					forward(unwrapHTTPWriter{prefetchUpgradeWriter{w, len(clientFrame)}}, r)
					if !reflect.DeepEqual(original.Header, r.Header) || !reflect.DeepEqual(original.URL, r.URL) || original.Host != r.Host {
						handlerDone <- errors.New("forwarding mutated the original request")
					} else {
						handlerDone <- nil
					}
				}))
				defer front.Close()
				conn, err := net.Dial("tcp", front.Listener.Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				r := httptest.NewRequest(http.MethodGet, "http://8080-node-s1.example/ws", nil)
				websocketHeaders(r)
				if !private {
					r.Header.Set(proxy.HeaderAccessToken, "forward")
				}
				var wire bytes.Buffer
				if err := r.Write(&wire); err != nil {
					t.Fatal(err)
				}
				wire.Write(clientFrame)
				if _, err := conn.Write(wire.Bytes()); err != nil {
					t.Fatal(err)
				}
				reader := bufio.NewReader(conn)
				resp, err := http.ReadResponse(reader, r)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				if resp.StatusCode != 101 || resp.Header.Get("Connection") != "Upgrade" || resp.Header.Get("Upgrade") != "websocket" || resp.Header.Get("Sec-WebSocket-Protocol") != "chat" || resp.Header.Get("Set-Cookie") != "guest=yes" || resp.Header.Get("X-Wrapper") != "preserved" || resp.Header.Get("X-Hop") != "" {
					t.Fatalf("handshake = %d %v", resp.StatusCode, resp.Header)
				}
				waitInflight(t, harness.stats, 0, 1)
				if got := metricValue(t, registry, `data_requests_total{result="ok"}`); got != 1 {
					t.Fatalf("open WebSocket result count=%d", got)
				}
				if limited {
					busy := httptest.NewRecorder()
					forward(busy, r)
					if busy.Code != http.StatusTooManyRequests || busy.Header().Get(proxy.HeaderProxyError) != proxy.ProxyErrorMaxInflightReached || router.activateCalls.Load() != 1 {
						t.Fatalf("open WebSocket did not hold admission: status=%d activations=%d", busy.Code, router.activateCalls.Load())
					}
				}
				if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(reader)
				if err != nil || !bytes.Equal(got, append(firstFrame, tail...)) {
					t.Fatalf("downstream frames=%x err=%v", got, err)
				}
				awaitWebsocket(t, backendDone)
				awaitWebsocket(t, handlerDone)
				waitInflight(t, harness.stats, 0, 0)
				if limited {
					harness.assertAvailable(t, binding.Admission, proxyadmission.ServiceForward)
				}
				if private && (rewrites.Load() != 1 || revalidations.Load() != 1) {
					t.Fatalf("rewrite=%d revalidate=%d", rewrites.Load(), revalidations.Load())
				}
				if metricValue(t, registry, `data_requests_total{result="ok"}`) != 1 || metricValue(t, registry, `data_requests_total{result="upstream_error"}`) != 0 {
					t.Fatal("WebSocket was counted more than once")
				}
			})
		}
	}
}

func TestWebSocketHandshakeFailuresAndOrdinaryResponses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		change   func(*http.Request)
		response string
		status   int
		upgrade  bool
		body     string
	}{
		{name: "endpoint rejection", response: "HTTP/1.1 403 Forbidden\r\nContent-Length: 6\r\nX-Guest: yes\r\n\r\ndenied", status: 403, upgrade: true, body: "denied"},
		{name: "ordinary response", response: "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok", status: 200, upgrade: true, body: "ok"},
		{name: "version rejection", response: "HTTP/1.1 426 Upgrade Required\r\nSec-WebSocket-Version: 13\r\nContent-Length: 0\r\n\r\n", status: 426, upgrade: true},
		{name: "unsupported writer", response: websocketReply, status: 500, upgrade: true, body: "upgrade unsupported\n"},
		{name: "missing response connection", response: "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n", status: 502, upgrade: true},
		{name: "wrong response protocol", response: "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n", status: 502, upgrade: true},
		{name: "ambiguous response protocol", response: strings.Replace(websocketReply, "Upgrade: WebSocket", "Upgrade: websocket\r\nUpgrade: h2c", 1), status: 502, upgrade: true},
		{name: "malformed response", response: "invalid\r\n\r\n", status: 502, upgrade: true},
		{name: "unsolicited upgrade", change: func(r *http.Request) { r.Header.Del("Upgrade") }, response: websocketReply, status: 502},
		{name: "not an upgrade token", change: func(r *http.Request) { r.Header.Set("Connection", "upgrader") }, response: websocketReply, status: 502},
		{name: "not GET", change: func(r *http.Request) { r.Method = http.MethodPost }, response: websocketReply, status: 502},
		{name: "not HTTP 1.1", change: func(r *http.Request) { r.Proto, r.ProtoMinor = "HTTP/1.0", 0 }, response: websocketReply, status: 502},
		{name: "HTTP 2 is not an upgrade", change: func(r *http.Request) { r.Proto, r.ProtoMajor, r.ProtoMinor = "HTTP/2.0", 2, 0 }, response: websocketReply, status: 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend, peer := net.Pipe()
			defer backend.Close()
			defer peer.Close()
			peerDone := make(chan error, 1)
			go func() {
				defer peer.Close()
				r, err := http.ReadRequest(bufio.NewReader(peer))
				if err == nil {
					defer r.Body.Close()
					if (r.Header.Get("Upgrade") == "websocket") != tc.upgrade || r.Close == tc.upgrade {
						err = fmt.Errorf("guest transport upgrade=%q close=%v", r.Header.Get("Upgrade"), r.Close)
					} else {
						_, err = io.WriteString(peer, tc.response)
					}
				}
				peerDone <- err
			}()
			r := ordinaryRequest(http.MethodGet, "s1", 8080, "test-token")
			websocketHeaders(r)
			if tc.change != nil {
				tc.change(r)
			}
			px := proxy.NewWithDialer(cancellationPolicyRouter{}, nil, nil, nil,
				func(context.Context, proxy.Route) (net.Conn, error) { return backend, nil }, "")
			w := httptest.NewRecorder()
			px.ServeHTTP(w, r)
			awaitWebsocket(t, peerDone)
			if w.Code != tc.status || (tc.body != "" && w.Body.String() != tc.body) {
				t.Fatalf("response=%d body=%q", w.Code, w.Body.String())
			}
			if tc.status == 502 && (w.Header().Get("Upgrade") != "" || w.Header().Get(proxy.HeaderProxyError) != proxy.ProxyErrorUpstreamError) {
				t.Fatalf("invalid upgrade leaked downstream: %v", w.Header())
			}
		})
	}
}

func TestWebSocketCancellationClosesBothPumps(t *testing.T) {
	for _, limited := range []bool{false, true} {
		for _, private := range []bool{false, true} {
			t.Run(fmt.Sprintf("limited=%v/private=%v", limited, private), func(t *testing.T) {
				var binding proxyadmission.Binding
				if limited {
					binding = proxyadmission.Binding{Slot: 1, Generation: 1, Limits: publicconfig.MaxInflight{Total: 10}}
				}
				addr, backendDone := websocketBackend(t, func(conn net.Conn, reader *bufio.Reader, _ *http.Request) error {
					if _, err := io.WriteString(conn, websocketReply); err != nil {
						return err
					}
					_, err := io.Copy(io.Discard, reader)
					return err
				})
				tracker := &cancellationPolicyTracker{}
				px := proxy.NewWithDialer(cancellationPolicyRouter{binding}, nil, nil, nil,
					func(ctx context.Context, _ proxy.Route) (net.Conn, error) {
						return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
					}, "").WithTrafficTracker(tracker)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				handlerDone := make(chan error, 1)
				front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					r = r.WithContext(ctx)
					if private {
						px.ForwardAuthorized(w, r, proxy.AuthorizedForwardRequest{SandboxID: "s1", Target: proxy.LegacyTarget(8080)})
					} else {
						px.ServeHTTP(w, r)
					}
					handlerDone <- nil
				}))
				defer front.Close()
				conn, err := net.Dial("tcp", front.Listener.Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				r := ordinaryRequest(http.MethodGet, "s1", 8080, "test-token")
				websocketHeaders(r)
				if err := r.Write(conn); err != nil {
					t.Fatal(err)
				}
				reader := bufio.NewReader(conn)
				resp, err := http.ReadResponse(reader, r)
				if err != nil || resp.StatusCode != 101 {
					t.Fatalf("handshake=%v err=%v", resp, err)
				}
				cancel()
				awaitWebsocket(t, handlerDone)
				awaitWebsocket(t, backendDone)
				if body, err := io.ReadAll(reader); err != nil || len(body) != 0 {
					t.Fatalf("unexpected post-upgrade HTTP error: %q %v", body, err)
				}
				if tracker.closes.Load() != 1 {
					t.Fatalf("tracked connection closes=%d", tracker.closes.Load())
				}
			})
		}
	}
}

func TestWebSocketPreservesOuterConnectReadAhead(t *testing.T) {
	addr, backendDone := websocketBackend(t, func(conn net.Conn, reader *bufio.Reader, outer *http.Request) error {
		if outer.Method != http.MethodConnect {
			return fmt.Errorf("outer method=%s", outer.Method)
		}
		// Deliberately coalesce bytes from both handshakes and the first frame.
		if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n"+websocketReply+"\x81\x02hi"); err != nil {
			return err
		}
		inner, err := http.ReadRequest(reader)
		if err != nil {
			return err
		}
		defer inner.Body.Close()
		if inner.Method != http.MethodGet || inner.Header.Get("Upgrade") != "websocket" {
			return errors.New("inner upgrade request missing")
		}
		_, err = io.Copy(io.Discard, reader)
		return err
	})
	handlerDone := make(chan error, 1)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend, br, resp, err := proxy.DialSandboxConnect(r.Context(), "tcp", addr, "s1", proxy.LegacyTarget(8080), "test-token")
		if err != nil {
			handlerDone <- err
			return
		}
		defer backend.Close()
		if resp.StatusCode != 200 {
			handlerDone <- fmt.Errorf("CONNECT status=%d", resp.StatusCode)
			return
		}
		if _, err := br.Peek(len(websocketReply) + 4); err != nil {
			handlerDone <- err
			return
		}
		handlerDone <- proxy.ForwardHTTP(w, r, backend, br, nil, nil)
	}))
	defer front.Close()
	conn, err := net.Dial("tcp", front.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	r := ordinaryRequest(http.MethodGet, "s1", 8080, "test-token")
	websocketHeaders(r)
	if err := r.Write(conn); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, r)
	if err != nil || resp.StatusCode != 101 {
		t.Fatalf("handshake=%v err=%v", resp, err)
	}
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(reader); err != nil || string(got) != "\x81\x02hi" {
		t.Fatalf("prefetched frame=%x err=%v", got, err)
	}
	awaitWebsocket(t, handlerDone)
	awaitWebsocket(t, backendDone)
}

type failedUpgradeWriter struct {
	*httptest.ResponseRecorder
	conn net.Conn
}

func (w failedUpgradeWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(failedUpgradeOutput{})), nil
}

type failedUpgradeOutput struct{}

func (failedUpgradeOutput) Write([]byte) (int, error) { return 0, errors.New("client write failed") }

func TestWebSocketFailed101FlushDoesNotReturnResponseOwnership(t *testing.T) {
	backend, peer := net.Pipe()
	defer backend.Close()
	defer peer.Close()
	peerDone := make(chan error, 1)
	go func() {
		_, err := http.ReadRequest(bufio.NewReader(peer))
		if err == nil {
			_, err = io.WriteString(peer, websocketReply)
		}
		peerDone <- err
	}()
	client, clientPeer := net.Pipe()
	defer clientPeer.Close()
	w := failedUpgradeWriter{httptest.NewRecorder(), client}
	r := ordinaryRequest(http.MethodGet, "s1", 8080, "test-token")
	websocketHeaders(r)
	accepted := 0
	if err := proxy.ForwardHTTP(w, r, backend, nil, nil, func() { accepted++ }); err != nil {
		t.Fatalf("error returned after hijack would allow a second HTTP response: %v", err)
	}
	awaitWebsocket(t, peerDone)
	if accepted != 1 || w.Body.Len() != 0 {
		t.Fatalf("accepted=%d extra HTTP body=%q", accepted, w.Body.String())
	}
	_ = clientPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := clientPeer.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("hijacked client was not closed: %v", err)
	}
}
