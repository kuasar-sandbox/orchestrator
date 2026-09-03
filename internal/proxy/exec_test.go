package proxy_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/execadmission/limits"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	sandboxctl "github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

const execTestServiceSecret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type execTestRouter struct {
	identity       proxy.ExecIdentity
	found          bool
	lookupErr      error
	activateResult proxy.ExecIdentity
	activateFound  bool
	activateErr    error
	lookupCalls    atomic.Int32
	activateCalls  atomic.Int32
	routeCalls     atomic.Int32
	activateCtx    chan context.Context
}

func (r *execTestRouter) LookupRoute(context.Context, string, proxy.ConnectTarget) (proxy.RouteBinding, bool, error) {
	r.routeCalls.Add(1)
	return proxy.RouteBinding{Kind: proxy.KindDeny}, true, nil
}

func (r *execTestRouter) ActivateRoute(context.Context, proxy.RouteBinding) (proxy.Route, bool, error) {
	r.routeCalls.Add(1)
	return proxy.Route{}, false, nil
}

func (r *execTestRouter) LookupExec(context.Context, string) (proxy.ExecIdentity, bool, error) {
	r.lookupCalls.Add(1)
	return r.identity, r.found, r.lookupErr
}

func (r *execTestRouter) ActivateExec(ctx context.Context, _ string, expected proxy.ExecIdentity) (proxy.ExecIdentity, bool, error) {
	r.activateCalls.Add(1)
	if r.activateCtx != nil {
		select {
		case r.activateCtx <- ctx:
		default:
		}
	}
	if expected != r.identity {
		return proxy.ExecIdentity{}, false, nil
	}
	return r.activateResult, r.activateFound, r.activateErr
}

func TestExecRejectsNonConnectAndInvalidTokenWithoutLifecycleSideEffects(t *testing.T) {
	harness := newLimitedTrafficHarness(t)
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-s1",
		StableID:      "stable-s1",
		ServiceSecret: execTestServiceSecret,
	}
	identity.Admission = harness.bind(t, identity.NodeSandboxID, publicconfig.MaxInflight{Exec: 1})
	router := &execTestRouter{
		identity: identity, found: true,
		activateResult: identity, activateFound: true,
	}
	var dials atomic.Int32
	traffic := &countingAdmissionTracker{traffic: harness.traffic}
	px := proxy.NewWithDialer(router, func() string { return "off" }, discardExecLogger(), nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			dials.Add(1)
			return nil, fmt.Errorf("unexpected dial")
		}, t.TempDir()).WithTrafficTracker(traffic)

	t.Run("ordinary HTTP", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "http://sandbox/exec", nil)
		req.Header.Set(proxy.HeaderSandboxID, "node-s1")
		req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
		resp := httptest.NewRecorder()
		px.ServeHTTP(resp, req)
		if resp.Code != http.StatusMethodNotAllowed || resp.Header().Get("Allow") != http.MethodConnect {
			t.Fatalf("ordinary exec response = %d Allow=%q, want 405 Allow=CONNECT", resp.Code, resp.Header().Get("Allow"))
		}
		if router.lookupCalls.Load() != 0 || router.activateCalls.Load() != 0 || router.routeCalls.Load() != 0 || dials.Load() != 0 {
			t.Fatalf("ordinary exec caused side effects: lookup=%d activate=%d route=%d dial=%d",
				router.lookupCalls.Load(), router.activateCalls.Load(), router.routeCalls.Load(), dials.Load())
		}
	})

	t.Run("invalid KAT ignores auth off mode", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", nil)
		req.Host = "sandbox:443"
		req.Header.Set(proxy.HeaderSandboxID, "node-s1")
		req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
		req.Header.Set(proxy.HeaderAccessToken, "not-a-kat")
		resp := httptest.NewRecorder()
		px.ServeHTTP(resp, req)
		if resp.Code != http.StatusUnauthorized {
			t.Fatalf("invalid KAT response = %d, want 401", resp.Code)
		}
		if router.lookupCalls.Load() != 1 || router.activateCalls.Load() != 0 || router.routeCalls.Load() != 0 || dials.Load() != 0 || traffic.tries.Load() != 0 {
			t.Fatalf("invalid KAT caused lifecycle side effects: lookup=%d activate=%d route=%d dial=%d admission=%d",
				router.lookupCalls.Load(), router.activateCalls.Load(), router.routeCalls.Load(), dials.Load(), traffic.tries.Load())
		}
		harness.assertAvailable(t, identity.Admission, proxyadmission.ServiceExec)
	})
}

func TestExecNotFoundIsRetryableOnlyBeforeAdmission(t *testing.T) {
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-s1",
		StableID:      "stable-s1",
		ServiceSecret: execTestServiceSecret,
	}
	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name           string
		lookupFound    bool
		activateFound  bool
		wantStatus     int
		wantProxyError string
		wantParking    int32
		wantActivate   int32
	}{
		{name: "lookup miss", wantStatus: http.StatusNotFound, wantProxyError: proxy.ProxyErrorNotFound},
		{name: "activation miss", lookupFound: true, wantStatus: http.StatusOK, wantParking: 1, wantActivate: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			router := &execTestRouter{
				identity: identity, found: test.lookupFound,
				activateFound: test.activateFound,
			}
			traffic := &recordingTrafficTracker{}
			px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), nil,
				func(context.Context, proxy.Route) (net.Conn, error) {
					t.Fatal("unexpected dial")
					return nil, nil
				}, t.TempDir()).WithTrafficTracker(traffic)
			req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", bytes.NewReader(validExecTestFrame()))
			req.ProtoMajor = 2
			req.Host = "sandbox:443"
			req.Header.Set(proxy.HeaderSandboxID, identity.NodeSandboxID)
			req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
			req.Header.Set(proxy.HeaderAccessToken, token)
			resp := httptest.NewRecorder()
			px.ServeHTTP(resp, req)
			if resp.Code != test.wantStatus || resp.Header().Get(proxy.HeaderProxyError) != test.wantProxyError {
				t.Fatalf("response=%d proxy-error=%q, want %d/%q", resp.Code, resp.Header().Get(proxy.HeaderProxyError), test.wantStatus, test.wantProxyError)
			}
			if test.wantActivate != 0 {
				assertExecRequestRejected(t, resp.Body.Bytes())
			}
			if router.lookupCalls.Load() != 1 || router.activateCalls.Load() != test.wantActivate ||
				traffic.begins.Load() != test.wantParking || traffic.closes.Load() != test.wantParking {
				t.Fatalf("lookup=%d activate=%d parking=%d closes=%d, want 1/%d/%d/%d",
					router.lookupCalls.Load(), router.activateCalls.Load(), traffic.begins.Load(), traffic.closes.Load(),
					test.wantActivate, test.wantParking, test.wantParking)
			}
		})
	}
}

func TestExecConditionFailureHasNoParkingActivationOrDial(t *testing.T) {
	costArgv := `[` + strings.Repeat(`"x",`, int(limits.MaxRuntimeCost)) + `"x"]`
	for _, test := range []struct {
		name        string
		condition   string
		requestJSON string
	}{
		{
			name:        "false",
			condition:   `request.argv == ['/bin/allowed']`,
			requestJSON: `{"type":"exec_request","exec":{"argv":["/bin/denied"]}}`,
		},
		{
			name:        "evaluation error",
			condition:   `request.argv[99] == 'missing'`,
			requestJSON: `{"type":"exec_request","exec":{"argv":["true"]}}`,
		},
		{
			name:        "cost exceeded",
			condition:   `request.argv.exists(arg, arg == 'never')`,
			requestJSON: `{"type":"exec_request","exec":{"argv":` + costArgv + `}}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := newLimitedTrafficHarness(t)
			identity := proxy.ExecIdentity{
				NodeSandboxID: "node-condition",
				StableID:      "stable-condition",
				ServiceSecret: execTestServiceSecret,
			}
			identity.Admission = harness.bind(t, identity.NodeSandboxID, publicconfig.MaxInflight{Exec: 1})
			token, err := keys.MintExecAccessTokenWithConditions(
				identity.ServiceSecret, identity.StableID, 0, []string{test.condition},
			)
			if err != nil {
				t.Fatal(err)
			}
			router := &execTestRouter{
				identity: identity, found: true,
				activateResult: identity, activateFound: true,
			}
			var dials atomic.Int32
			traffic := &countingAdmissionTracker{traffic: harness.traffic}
			px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), nil,
				func(context.Context, proxy.Route) (net.Conn, error) {
					dials.Add(1)
					return nil, errors.New("unexpected dial")
				}, t.TempDir()).WithTrafficTracker(traffic)
			req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", bytes.NewReader(execTestFrame(test.requestJSON)))
			req.ProtoMajor = 2
			req.Host = "sandbox:443"
			req.Header.Set(proxy.HeaderSandboxID, identity.NodeSandboxID)
			req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
			req.Header.Set(proxy.HeaderAccessToken, token)
			response := httptest.NewRecorder()
			px.ServeHTTP(response, req)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want post-accept 200", response.Code)
			}
			assertExecRequestRejected(t, response.Body.Bytes())
			if router.lookupCalls.Load() != 1 || router.activateCalls.Load() != 0 || dials.Load() != 0 ||
				traffic.tries.Load() != 0 {
				t.Fatalf("denied request side effects lookup=%d activate=%d dial=%d admission=%d",
					router.lookupCalls.Load(), router.activateCalls.Load(), dials.Load(), traffic.tries.Load())
			}
			harness.assertAvailable(t, identity.Admission, proxyadmission.ServiceExec)
		})
	}
}

func TestExecRejectsForwardTokenAndChangedIdentityBeforeDial(t *testing.T) {
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-s1",
		StableID:      "stable-s1",
		ServiceSecret: execTestServiceSecret,
	}
	forwardToken, err := keys.MintForwardAccessToken(execTestServiceSecret, identity.StableID)
	if err != nil {
		t.Fatal(err)
	}
	validToken, err := keys.MintExecAccessToken(execTestServiceSecret, identity.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name             string
		token            string
		activateIdentity proxy.ExecIdentity
		wantActivations  int32
		wantStatus       int
		wantProxyError   string
	}{
		{name: "forward audience", token: forwardToken, activateIdentity: identity, wantStatus: http.StatusUnauthorized, wantActivations: 0, wantProxyError: proxy.ProxyErrorUnauthorized},
		{name: "identity changed after activation", token: validToken, activateIdentity: proxy.ExecIdentity{
			NodeSandboxID: "node-s1", StableID: "other", ServiceSecret: execTestServiceSecret,
		}, wantStatus: http.StatusOK, wantActivations: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			router := &execTestRouter{
				identity: identity, found: true,
				activateResult: test.activateIdentity, activateFound: true,
			}
			var dials atomic.Int32
			px := proxy.NewWithDialer(router, func() string { return "off" }, discardExecLogger(), nil,
				func(context.Context, proxy.Route) (net.Conn, error) {
					dials.Add(1)
					return nil, fmt.Errorf("unexpected dial")
				}, t.TempDir())
			req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", bytes.NewReader(validExecTestFrame()))
			req.ProtoMajor = 2
			req.Host = "sandbox:443"
			req.Header.Set(proxy.HeaderSandboxID, "node-s1")
			req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
			req.Header.Set(proxy.HeaderAccessToken, test.token)
			resp := httptest.NewRecorder()
			px.ServeHTTP(resp, req)
			if resp.Code != test.wantStatus {
				t.Fatalf("response = %d, want %d", resp.Code, test.wantStatus)
			}
			if kind := resp.Header().Get(proxy.HeaderProxyError); kind != test.wantProxyError {
				t.Fatalf("proxy error = %q, want %q", kind, test.wantProxyError)
			}
			if router.lookupCalls.Load() != 1 || router.activateCalls.Load() != test.wantActivations || router.routeCalls.Load() != 0 || dials.Load() != 0 {
				t.Fatalf("calls lookup=%d activate=%d route=%d dial=%d",
					router.lookupCalls.Load(), router.activateCalls.Load(), router.routeCalls.Load(), dials.Load())
			}
			if test.wantActivations != 0 {
				assertExecRequestRejected(t, resp.Body.Bytes())
			}
		})
	}
}

func TestExecActivationFailureReturnsGenericPostAcceptErrorBeforeDial(t *testing.T) {
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-s1",
		StableID:      "stable-s1",
		ServiceSecret: execTestServiceSecret,
	}
	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}
	router := &execTestRouter{
		identity: identity, found: true,
		activateErr: fmt.Errorf("activation timed out"),
	}
	var dials atomic.Int32
	traffic := &recordingTrafficTracker{}
	px := proxy.NewWithDialer(router, func() string { return "off" }, discardExecLogger(), nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			dials.Add(1)
			return nil, fmt.Errorf("unexpected dial")
		}, t.TempDir()).WithTrafficTracker(traffic)
	req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", bytes.NewReader(validExecTestFrame()))
	req.ProtoMajor = 2
	req.Host = "sandbox:443"
	req.Header.Set(proxy.HeaderSandboxID, identity.NodeSandboxID)
	req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
	req.Header.Set(proxy.HeaderAccessToken, token)
	resp := httptest.NewRecorder()
	px.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("activation failure response = %d, want post-accept 200", resp.Code)
	}
	assertExecRequestRejected(t, resp.Body.Bytes())
	if router.lookupCalls.Load() != 1 || router.activateCalls.Load() != 1 || dials.Load() != 0 {
		t.Fatalf("calls lookup=%d activate=%d dial=%d",
			router.lookupCalls.Load(), router.activateCalls.Load(), dials.Load())
	}
	if traffic.begins.Load() != 1 || traffic.closes.Load() != 1 || traffic.attaches.Load() != 0 || traffic.service != proxy.ConnectServiceExec {
		t.Fatalf("exec traffic begins=%d closes=%d attaches=%d service=%q",
			traffic.begins.Load(), traffic.closes.Load(), traffic.attaches.Load(), traffic.service)
	}
}

func TestExecMaxInflightReachedAfterFirstFrameUsesCtlErrorWithoutActivation(t *testing.T) {
	harness := newLimitedTrafficHarness(t)
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-s1",
		StableID:      "stable-s1",
		ServiceSecret: execTestServiceSecret,
	}
	identity.Admission = harness.bind(t, identity.NodeSandboxID, publicconfig.MaxInflight{Exec: 1})
	held, err := harness.traffic.TryBeginParking(identity.NodeSandboxID, proxy.ConnectServiceExec, identity.Admission)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}
	router := &execTestRouter{
		identity: identity, found: true,
		activateResult: identity, activateFound: true,
	}
	registry := metrics.New()
	var dials atomic.Int32
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), registry,
		func(context.Context, proxy.Route) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("unexpected dial")
		}, t.TempDir()).WithTrafficTracker(harness.traffic)
	req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", bytes.NewReader(validExecTestFrame()))
	req.ProtoMajor = 2
	req.Host = "sandbox:443"
	req.Header.Set(proxy.HeaderSandboxID, identity.NodeSandboxID)
	req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
	req.Header.Set(proxy.HeaderAccessToken, token)
	response := httptest.NewRecorder()
	px.ServeHTTP(response, req)
	if response.Code != http.StatusOK || response.Header().Get(proxy.HeaderProxyError) != "" {
		t.Fatalf("response=%d proxy-error=%q, want post-CONNECT ctl error", response.Code, response.Header().Get(proxy.HeaderProxyError))
	}
	assertExecRequestRejected(t, response.Body.Bytes())
	if router.lookupCalls.Load() != 1 || router.activateCalls.Load() != 0 || dials.Load() != 0 {
		t.Fatalf("calls lookup=%d activate=%d dial=%d, want 1/0/0",
			router.lookupCalls.Load(), router.activateCalls.Load(), dials.Load())
	}
	if got := metricValue(t, registry, `data_requests_total{result="max_inflight_reached"}`); got != 1 {
		t.Fatalf("max_inflight_reached counter=%d, want 1", got)
	}
}

func TestExecH1PreservesBufferedInputAndHalfCloseTail(t *testing.T) {
	runRoot := t.TempDir()
	harness := newLimitedTrafficHarness(t)
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-s1",
		StableID:      "stable-s1",
		ServiceSecret: execTestServiceSecret,
	}
	identity.Admission = harness.bind(t, identity.NodeSandboxID, publicconfig.MaxInflight{Exec: 1})
	backend, backendDone := startExecBackend(t, runRoot, identity.NodeSandboxID, []byte("exec-response-tail"))
	router := &execTestRouter{
		identity: identity, found: true,
		activateResult: identity, activateFound: true,
		activateCtx: make(chan context.Context, 1),
	}
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), nil, nil, runRoot).WithTrafficTracker(harness.traffic)
	ts := httptest.NewServer(px)
	defer ts.Close()

	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}
	frame := execTestFrame(` {"type":"exec_request","exec":{"argv":["true"]}} `)
	clientTail := []byte("buffered-mux-tail")
	requestBytes := append(append([]byte(nil), frame...), clientTail...)

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tcpConn := conn.(*net.TCPConn)
	defer tcpConn.Close()
	_ = tcpConn.SetDeadline(time.Now().Add(5 * time.Second))
	var request bytes.Buffer
	fmt.Fprintf(&request, "CONNECT sandbox:443 HTTP/1.1\r\nHost: sandbox:443\r\n%s: %s\r\n%s: exec\r\n%s: 8123\r\n%s: %s\r\n\r\n",
		proxy.HeaderSandboxID, identity.NodeSandboxID,
		proxy.HeaderSandboxService,
		proxy.HeaderSandboxPort,
		proxy.HeaderAccessToken, token)
	request.Write(requestBytes)
	if _, err := tcpConn.Write(request.Bytes()); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(tcpConn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want 200", resp.StatusCode)
	}
	waitInflightFor(t, harness.stats, identity.NodeSandboxID, 0, 1)
	if _, err := harness.worker.TryAcquire(identity.Admission, proxyadmission.ServiceExec); !errors.Is(err, proxyadmission.ErrLimitReached) {
		t.Fatalf("H1 exec tunnel did not hold its admission lease: %v", err)
	}
	if err := tcpConn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	// The tunnel context intentionally survives this valid HTTP/1 write-side EOF
	// so the reverse direction can drain.
	requestCtx := <-router.activateCtx
	select {
	case <-requestCtx.Done():
		t.Fatal("exec tunnel context was canceled by a valid HTTP/1 write-side EOF")
	case <-time.After(100 * time.Millisecond):
	}
	defer resp.Body.Close()
	gotResponse, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	gotRequest := <-backend
	backendErr := <-backendDone
	if got := string(gotResponse); got != "exec-response-tail" {
		t.Errorf("response tail = %q", got)
	}
	if !bytes.Equal(gotRequest, requestBytes) {
		t.Fatalf("backend request changed:\n got %q\nwant %q", gotRequest, requestBytes)
	}
	if backendErr != nil {
		t.Fatal(backendErr)
	}
	if router.lookupCalls.Load() != 1 || router.activateCalls.Load() != 1 || router.routeCalls.Load() != 0 {
		t.Fatalf("calls lookup=%d activate=%d route=%d", router.lookupCalls.Load(), router.activateCalls.Load(), router.routeCalls.Load())
	}
	waitInflightFor(t, harness.stats, identity.NodeSandboxID, 0, 0)
	harness.assertAvailable(t, identity.Admission, proxyadmission.ServiceExec)
}

func TestExecH2StreamsRequestAndFlushesTrailingResponse(t *testing.T) {
	runRoot := t.TempDir()
	harness := newLimitedTrafficHarness(t)
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-h2",
		StableID:      "stable-h2",
		ServiceSecret: execTestServiceSecret,
	}
	identity.Admission = harness.bind(t, identity.NodeSandboxID, publicconfig.MaxInflight{Exec: 1})
	backend, backendDone := startExecBackend(t, runRoot, identity.NodeSandboxID, []byte("h2-response-tail"))
	router := &execTestRouter{
		identity: identity, found: true,
		activateResult: identity, activateFound: true,
	}
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), nil, nil, runRoot).WithTrafficTracker(harness.traffic)
	ts := httptest.NewUnstartedServer(px)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}
	frame := validExecTestFrame()
	requestBytes := append(append([]byte(nil), frame...), []byte("h2-mux-tail")...)
	bodyReader, bodyWriter := io.Pipe()
	req, err := http.NewRequest(http.MethodConnect, ts.URL, bodyReader)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "sandbox:443"
	req.Header.Set(proxy.HeaderSandboxID, identity.NodeSandboxID)
	req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
	req.Header.Set(proxy.HeaderAccessToken, token)
	writeDone := make(chan error, 1)
	releaseEOF := make(chan struct{})
	go func() {
		_, err := bodyWriter.Write(requestBytes)
		<-releaseEOF
		if closeErr := bodyWriter.Close(); err == nil {
			err = closeErr
		}
		writeDone <- err
	}()
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK {
		t.Fatalf("response = %s %d, want HTTP/2 200", resp.Proto, resp.StatusCode)
	}
	waitInflightFor(t, harness.stats, identity.NodeSandboxID, 0, 1)
	if _, err := harness.worker.TryAcquire(identity.Admission, proxyadmission.ServiceExec); !errors.Is(err, proxyadmission.ErrLimitReached) {
		t.Fatalf("H2 exec tunnel did not hold its admission lease: %v", err)
	}
	close(releaseEOF)
	gotResponse, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(gotResponse); got != "h2-response-tail" {
		t.Fatalf("response tail = %q", got)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if gotRequest := <-backend; !bytes.Equal(gotRequest, requestBytes) {
		t.Fatalf("backend request changed:\n got %q\nwant %q", gotRequest, requestBytes)
	}
	if err := <-backendDone; err != nil {
		t.Fatal(err)
	}
	waitInflightFor(t, harness.stats, identity.NodeSandboxID, 0, 0)
	harness.assertAvailable(t, identity.Admission, proxyadmission.ServiceExec)
}

func TestExecGateRejectsNonExecFirstFrameAfterConnect200(t *testing.T) {
	runRoot := t.TempDir()
	harness := newLimitedTrafficHarness(t)
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-gate",
		StableID:      "stable-gate",
		ServiceSecret: execTestServiceSecret,
	}
	identity.Admission = harness.bind(t, identity.NodeSandboxID, publicconfig.MaxInflight{Exec: 1})
	router := &execTestRouter{
		identity: identity, found: true,
		activateResult: identity, activateFound: true,
	}
	var dials atomic.Int32
	traffic := &countingAdmissionTracker{traffic: harness.traffic}
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("unexpected dial")
		}, runRoot).WithTrafficTracker(traffic)
	ts := httptest.NewServer(px)
	defer ts.Close()
	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tcpConn := conn.(*net.TCPConn)
	defer tcpConn.Close()
	_ = tcpConn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(tcpConn, "CONNECT sandbox:443 HTTP/1.1\r\nHost: sandbox:443\r\n%s: %s\r\n%s: exec\r\n%s: %s\r\n\r\n",
		proxy.HeaderSandboxID, identity.NodeSandboxID,
		proxy.HeaderSandboxService,
		proxy.HeaderAccessToken, token)
	reader := bufio.NewReader(tcpConn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want 200", resp.StatusCode)
	}
	if _, err := tcpConn.Write(execTestFrame(`{"type":"snapshot_request"}`)); err != nil {
		t.Fatal(err)
	}
	if err := tcpConn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if router.activateCalls.Load() != 0 || dials.Load() != 0 || traffic.tries.Load() != 0 {
		t.Fatalf("invalid frame crossed gate: activate=%d dial=%d admission=%d",
			router.activateCalls.Load(), dials.Load(), traffic.tries.Load())
	}
	harness.assertAvailable(t, identity.Admission, proxyadmission.ServiceExec)
}

func TestExecH2ContextCancellationClosesCtlStream(t *testing.T) {
	runRoot := t.TempDir()
	harness := newLimitedTrafficHarness(t)
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-cancel",
		StableID:      "stable-cancel",
		ServiceSecret: execTestServiceSecret,
	}
	identity.Admission = harness.bind(t, identity.NodeSandboxID, publicconfig.MaxInflight{Exec: 1})
	dir := nodepath.SandboxRunDir(runRoot, identity.NodeSandboxID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(dir, "ctl.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	frame := validExecTestFrame()
	frameSeen := make(chan struct{})
	backendDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			backendDone <- err
			return
		}
		defer conn.Close()
		got := make([]byte, len(frame))
		if _, err := io.ReadFull(conn, got); err != nil {
			backendDone <- err
			return
		}
		if !bytes.Equal(got, frame) {
			backendDone <- fmt.Errorf("gate changed first frame")
			return
		}
		close(frameSeen)
		_, err = io.Copy(io.Discard, conn)
		backendDone <- err
	}()

	router := &execTestRouter{
		identity: identity, found: true,
		activateResult: identity, activateFound: true,
	}
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), nil, nil, runRoot).
		WithTrafficTracker(harness.traffic)
	ts := httptest.NewUnstartedServer(px)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()
	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}

	bodyReader, bodyWriter := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodConnect, ts.URL, bodyReader)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "sandbox:443"
	req.Header.Set(proxy.HeaderSandboxID, identity.NodeSandboxID)
	req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
	req.Header.Set(proxy.HeaderAccessToken, token)
	writeDone := make(chan error, 1)
	go func() {
		_, err := bodyWriter.Write(frame)
		writeDone <- err
	}()
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK {
		t.Fatalf("response = %s %d, want HTTP/2 200", resp.Proto, resp.StatusCode)
	}
	select {
	case <-frameSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("ctl backend did not receive accepted exec frame")
	}
	waitInflightFor(t, harness.stats, identity.NodeSandboxID, 0, 1)
	cancel()
	_ = bodyWriter.Close()
	_ = resp.Body.Close()
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-backendDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("request context cancellation did not close ctl stream")
	}
	waitInflightFor(t, harness.stats, identity.NodeSandboxID, 0, 0)
	harness.assertAvailable(t, identity.Admission, proxyadmission.ServiceExec)
}

func TestExecH1FullClientCloseTerminatesHandlerAndCtlStream(t *testing.T) {
	runRoot := t.TempDir()
	identity := proxy.ExecIdentity{
		NodeSandboxID: "node-close",
		StableID:      "stable-close",
		ServiceSecret: execTestServiceSecret,
	}
	dir := nodepath.SandboxRunDir(runRoot, identity.NodeSandboxID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(dir, "ctl.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	frame := validExecTestFrame()
	frameSeen := make(chan struct{})
	backendDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			backendDone <- err
			return
		}
		defer conn.Close()
		got := make([]byte, len(frame))
		if _, err := io.ReadFull(conn, got); err != nil {
			backendDone <- err
			return
		}
		if !bytes.Equal(got, frame) {
			backendDone <- fmt.Errorf("gate changed first frame")
			return
		}
		close(frameSeen)
		_, err = io.Copy(io.Discard, conn)
		backendDone <- err
	}()

	router := &execTestRouter{
		identity: identity, found: true,
		activateResult: identity, activateFound: true,
	}
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), nil, nil, runRoot)
	handlerDone := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		px.ServeHTTP(w, r)
	}))
	defer ts.Close()
	token, err := keys.MintExecAccessToken(identity.ServiceSecret, identity.StableID, 0)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tcpConn := conn.(*net.TCPConn)
	_ = tcpConn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(tcpConn, "CONNECT sandbox:443 HTTP/1.1\r\nHost: sandbox:443\r\n%s: %s\r\n%s: exec\r\n%s: %s\r\n\r\n",
		proxy.HeaderSandboxID, identity.NodeSandboxID,
		proxy.HeaderSandboxService,
		proxy.HeaderAccessToken, token)
	resp, err := http.ReadResponse(bufio.NewReader(tcpConn), &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want 200", resp.StatusCode)
	}
	if _, err := tcpConn.Write(frame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-frameSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("ctl backend did not receive accepted exec frame")
	}
	if err := tcpConn.SetLinger(0); err != nil {
		t.Fatal(err)
	}
	if err := tcpConn.Close(); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	select {
	case err := <-backendDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("full client close did not close ctl stream")
	}
	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("full client close left exec handler running")
	}
}

func startExecBackend(t *testing.T, runRoot, sid string, response []byte) (<-chan []byte, <-chan error) {
	t.Helper()
	dir := nodepath.SandboxRunDir(runRoot, sid)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(dir, "ctl.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	request := make(chan []byte, 1)
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		got, err := io.ReadAll(conn)
		if err != nil {
			done <- err
			return
		}
		request <- got
		if _, err := conn.Write(response); err != nil {
			done <- err
			return
		}
		if closer, ok := conn.(interface{ CloseWrite() error }); ok {
			err = closer.CloseWrite()
		}
		done <- err
	}()
	return request, done
}

func execTestFrame(payload string) []byte {
	frame := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	return frame
}

func validExecTestFrame() []byte {
	return execTestFrame(`{"type":"exec_request","exec":{"argv":["/bin/true"]}}`)
}

func assertExecRequestRejected(t *testing.T, raw []byte) {
	t.Helper()
	var response sandboxctl.Response
	if err := sandboxctl.ReadMessage(bytes.NewReader(raw), &response); err != nil {
		t.Fatalf("read ctl rejection %q: %v", raw, err)
	}
	if response.Type != sandboxctl.TypeError || response.Msg != "exec request rejected" {
		t.Fatalf("ctl rejection = %+v", response)
	}
}

func discardExecLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestExecUnavailableWithoutTrustedRunRoot(t *testing.T) {
	identity := proxy.ExecIdentity{NodeSandboxID: "node-s1", StableID: "stable-s1", ServiceSecret: execTestServiceSecret}
	router := &execTestRouter{identity: identity, found: true, activateResult: identity, activateFound: true}
	px := proxy.New(router, func() string { return "enforce" }, discardExecLogger(), nil)
	req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", strings.NewReader("unused"))
	req.Host = "sandbox:443"
	req.Header.Set(proxy.HeaderSandboxID, identity.NodeSandboxID)
	req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
	resp := httptest.NewRecorder()
	px.ServeHTTP(resp, req)
	if resp.Code != http.StatusNotImplemented || router.lookupCalls.Load() != 0 || router.activateCalls.Load() != 0 {
		t.Fatalf("response=%d lookup=%d activate=%d, want side-effect-free 501", resp.Code, router.lookupCalls.Load(), router.activateCalls.Load())
	}
}
