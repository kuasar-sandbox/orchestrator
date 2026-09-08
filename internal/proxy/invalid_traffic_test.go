package proxy_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// A panic tracker covers both limited and unlimited paths: neither may be
// entered for an unavailable policy, including privately authenticated ingress.
type forbiddenTraffic struct{}

func (forbiddenTraffic) BeginParking(string, proxy.ConnectService) proxy.TrafficFlow {
	panic("invalid policy entered parking")
}

func TestInvalidTrafficPolicyAuthenticatesBeforeRejectingWithoutSideEffects(t *testing.T) {
	target := proxy.LegacyTarget(8080)
	binding := proxy.BindRoute("node-invalid", "stable-invalid", types.ProfileE2B, "envd", "forward", target)
	binding.TrafficPolicyInvalid = true
	router := &admissionRouter{binding: binding, found: true, activateOK: true, route: proxy.Route{Kind: proxy.KindTCP, Addr: "unused"}}
	var dials atomic.Int32
	px := proxy.NewWithDialer(router, func() string { return "enforce" }, nil, nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("unexpected dial")
		}, "").WithTrafficTracker(forbiddenTraffic{})
	for _, method := range []string{http.MethodGet, http.MethodConnect} {
		for _, token := range []string{"wrong", "forward"} {
			response := httptest.NewRecorder()
			px.ServeHTTP(response, ordinaryRequest(method, binding.SandboxID, 8080, token))
			want := http.StatusServiceUnavailable
			if token == "wrong" {
				want = http.StatusUnauthorized
			}
			if response.Code != want {
				t.Fatalf("%s token=%s status=%d want=%d", method, token, response.Code, want)
			}
			if want == 503 && response.Header().Get(proxy.HeaderProxyError) != proxy.ProxyErrorRouteError {
				t.Fatalf("wrong error: %s", response.Header())
			}
		}
	}
	response := httptest.NewRecorder()
	px.ForwardAuthorized(response, httptest.NewRequest(http.MethodGet, "http://private/", nil), proxy.AuthorizedForwardRequest{SandboxID: binding.SandboxID, Target: target})
	if response.Code != 503 || response.Header().Get(proxy.HeaderProxyError) != proxy.ProxyErrorRouteError {
		t.Fatalf("private response=%d %s", response.Code, response.Header())
	}
	if router.activateCalls.Load() != 0 || dials.Load() != 0 {
		t.Fatalf("side effects activate=%d dial=%d", router.activateCalls.Load(), dials.Load())
	}
}

func TestInvalidTrafficPolicyExecKeepsKATFrameAndCELOrdering(t *testing.T) {
	for _, test := range []struct {
		name, condition, frame       string
		validToken                   bool
		wantStatus, wantPolicyErrors int
	}{
		{"bad KAT", "true", `{"type":"exec_request","exec":{"argv":["/bin/true"]}}`, false, 401, 0},
		{"bad frame", "true", `{"type":"unexpected"}`, true, 200, 0},
		{"CEL denied", "false", `{"type":"exec_request","exec":{"argv":["/bin/true"]}}`, true, 200, 0},
		{"policy denied after CEL", "true", `{"type":"exec_request","exec":{"argv":["/bin/true"]}}`, true, 200, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity := proxy.ExecIdentity{NodeSandboxID: "node-invalid", StableID: "stable-invalid", ServiceSecret: execTestServiceSecret, TrafficPolicyInvalid: true}
			token, err := keys.MintExecAccessTokenWithConditions(identity.ServiceSecret, identity.StableID, 0, []string{test.condition})
			if err != nil {
				t.Fatal(err)
			}
			if !test.validToken {
				token = "invalid"
			}
			router := &execTestRouter{identity: identity, found: true, activateResult: identity, activateFound: true}
			registry := metrics.New()
			var dials atomic.Int32
			px := proxy.NewWithDialer(router, func() string { return "enforce" }, discardExecLogger(), registry,
				func(context.Context, proxy.Route) (net.Conn, error) {
					dials.Add(1)
					return nil, errors.New("unexpected dial")
				}, t.TempDir()).WithTrafficTracker(forbiddenTraffic{})
			req := httptest.NewRequest(http.MethodConnect, "http://sandbox:443", bytes.NewReader(execTestFrame(test.frame)))
			req.ProtoMajor = 2
			req.Host = "sandbox:443"
			req.Header.Set(proxy.HeaderSandboxID, identity.NodeSandboxID)
			req.Header.Set(proxy.HeaderSandboxService, string(proxy.ConnectServiceExec))
			req.Header.Set(proxy.HeaderAccessToken, token)
			response := httptest.NewRecorder()
			px.ServeHTTP(response, req)
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d want=%d", response.Code, test.wantStatus)
			}
			if response.Code == 200 {
				if test.name == "bad frame" {
					if response.Body.Len() != 0 {
						t.Fatal("unrecognized frame must close directly")
					}
				} else {
					assertExecRequestRejected(t, response.Body.Bytes())
				}
				if response.Header().Get(proxy.HeaderProxyError) != "" {
					t.Fatal("HTTP error after CONNECT 200")
				}
			}
			if router.activateCalls.Load() != 0 || dials.Load() != 0 {
				t.Fatalf("side effects activate=%d dial=%d", router.activateCalls.Load(), dials.Load())
			}
			if got := metricValue(t, registry, `data_requests_total{result="route_error"}`); got != test.wantPolicyErrors {
				t.Fatalf("policy checks=%d want=%d (must follow CEL)", got, test.wantPolicyErrors)
			}
		})
	}
}
