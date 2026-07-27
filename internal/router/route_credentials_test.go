package router

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	routerTestAPISecret          = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
	routerTestManifestKey        = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	routerTestEnvdAccessToken    = "router-envd-access-token"
	routerTestTrafficAccessToken = "router-traffic-access-token"
)

func routerTestFingerprint(t *testing.T, secretHex string) string {
	t.Helper()
	raw, err := hex.DecodeString(secretHex)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func routerTestRouteResolve(t *testing.T, sid, group, routeKey, endpoint string, profile types.Profile) routeResolve {
	t.Helper()
	serviceSecret, err := keys.DeriveServiceSecret(routerTestAPISecret, sid)
	if err != nil {
		t.Fatal(err)
	}
	forwardAccessToken, err := keys.MintForwardAccessToken(serviceSecret, sid)
	if err != nil {
		t.Fatal(err)
	}
	result := routeResolve{
		NodeID:                 "n1",
		SandboxID:              sid,
		NodeSandboxID:          sid + "-g0",
		RouteRevision:          1,
		Group:                  group,
		RouteKey:               routeKey,
		Profile:                string(profile),
		TemplateID:             string(profile) + "-img-template",
		AuthSandboxID:          sid,
		APISecret:              routerTestAPISecret,
		APISecretFingerprint:   routerTestFingerprint(t, routerTestAPISecret),
		ManifestKeyFingerprint: routerTestFingerprint(t, routerTestManifestKey),
		ServiceSecret:          serviceSecret,
		ForwardAccessToken:     forwardAccessToken,
		DataEndpoint:           endpoint,
		State:                  "ready",
	}
	if profile == types.ProfileE2B {
		result.EnvdAccessToken = routerTestEnvdAccessToken
		result.TrafficAccessToken = routerTestTrafficAccessToken
	}
	return result
}

func routerTestReserveResult(t *testing.T, sid, group, routeKey, endpoint string, profile types.Profile) reserveResult {
	t.Helper()
	return reserveResult{Route: routerTestRouteResolve(t, sid, group, routeKey, endpoint, profile)}
}

func routerTestConnectReserveResult(t *testing.T, sid, group, routeKey, endpoint string, profile types.Profile) reserveResult {
	t.Helper()
	result := routerTestReserveResult(t, sid, group, routeKey, endpoint, profile)
	route := &result.Route
	result.Connect = &connectResult{
		NodeSandboxID:      route.NodeSandboxID,
		TemplateID:         route.TemplateID,
		Profile:            route.Profile,
		EnvdAccessToken:    route.EnvdAccessToken,
		TrafficAccessToken: route.TrafficAccessToken,
		ForwardAccessToken: route.ForwardAccessToken,
	}
	return result
}

func TestEffectiveDataPort(t *testing.T) {
	for _, tc := range []struct {
		name        string
		explicit    int
		hasExplicit bool
		target      int
		want        int
		wantErr     bool
	}{
		{name: "explicit", explicit: 8080, hasExplicit: true, want: 8080},
		{name: "target default", target: 7000, want: 7000},
		{name: "matching target", explicit: 7000, hasExplicit: true, target: 7000, want: 7000},
		{name: "target mismatch", explicit: 49983, hasExplicit: true, target: 7000, wantErr: true},
		{name: "missing", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := effectiveDataPort(tc.explicit, tc.hasExplicit, tc.target)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("effectiveDataPort() = (%d, %v), want (%d, error=%v)", got, err, tc.want, tc.wantErr)
			}
		})
	}
}
