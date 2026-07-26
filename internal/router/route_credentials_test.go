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

func routerTestReserveResult(t *testing.T, sid, endpoint string, profile types.Profile) reserveResult {
	t.Helper()
	serviceSecret, err := keys.DeriveServiceSecret(routerTestAPISecret, sid)
	if err != nil {
		t.Fatal(err)
	}
	forwardAccessToken, err := keys.MintForwardAccessToken(serviceSecret, sid)
	if err != nil {
		t.Fatal(err)
	}
	result := reserveResult{
		NodeID:                 "n1",
		SandboxID:              sid,
		NodeSandboxID:          sid + "-g0",
		Profile:                string(profile),
		AuthSandboxID:          sid,
		APISecret:              routerTestAPISecret,
		APISecretFingerprint:   routerTestFingerprint(t, routerTestAPISecret),
		ManifestKeyFingerprint: routerTestFingerprint(t, routerTestManifestKey),
		ServiceSecret:          serviceSecret,
		ForwardAccessToken:     forwardAccessToken,
		DataEndpoint:           endpoint,
	}
	if profile == types.ProfileE2B {
		result.EnvdAccessToken = routerTestEnvdAccessToken
		result.TrafficAccessToken = routerTestTrafficAccessToken
	}
	return result
}

func routerTestRouteResolve(t *testing.T, sid, group, routeKey, endpoint string, profile types.Profile) routeResolve {
	t.Helper()
	reserved := routerTestReserveResult(t, sid, endpoint, profile)
	return routeResolve{
		SandboxID:              reserved.SandboxID,
		NodeSandboxID:          reserved.NodeSandboxID,
		Group:                  group,
		RouteKey:               routeKey,
		NodeID:                 reserved.NodeID,
		DataEndpoint:           reserved.DataEndpoint,
		Profile:                reserved.Profile,
		AuthSandboxID:          reserved.AuthSandboxID,
		APISecret:              reserved.APISecret,
		APISecretFingerprint:   reserved.APISecretFingerprint,
		ManifestKeyFingerprint: reserved.ManifestKeyFingerprint,
		ServiceSecret:          reserved.ServiceSecret,
		EnvdAccessToken:        reserved.EnvdAccessToken,
		TrafficAccessToken:     reserved.TrafficAccessToken,
		ForwardAccessToken:     reserved.ForwardAccessToken,
		State:                  "ready",
	}
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
