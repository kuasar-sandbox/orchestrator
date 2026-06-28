package clusterclient

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
)

func TestRouteCandidatesFetchMembership(t *testing.T) {
	membership := clustercfg.MembershipConfig{
		Active: 7,
		Versions: []clustercfg.MembershipVersion{{
			Version: 7,
			Members: []clustercfg.MembershipMember{
				{ID: "a", Advertise: "http://a:7700"},
				{ID: "b", Advertise: "http://b:7700"},
				{ID: "c", Advertise: "http://c:7700"},
			},
		}},
		Owners: clustercfg.MembershipOwnerConfig{RouteLink: 2, NodeLink: 2},
	}
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != MembershipPath {
			t.Fatalf("unexpected path %s", req.URL.Path)
		}
		return jsonResponse(200, membership), nil
	})}
	reg := NewRegistryWithClient("http://bootstrap:7700", client)
	eps, err := reg.RouteCandidates(t.Context(), "/group")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(eps) != 2 {
		t.Fatalf("candidates len=%d want 2: %+v", len(eps), eps)
	}
	for _, ep := range eps {
		if ep.MemberID == "" || ep.BaseURL == "" || ep.Client == nil {
			t.Fatalf("bad endpoint: %+v", ep)
		}
	}
	label, err := reg.ActiveLabel(t.Context())
	if err != nil {
		t.Fatalf("label: %v", err)
	}
	if label == "" {
		t.Fatal("active label not computed")
	}
	all, err := reg.ActiveEndpoints(t.Context())
	if err != nil {
		t.Fatalf("active endpoints: %v", err)
	}
	if len(all) != 3 || all[0].MemberID != "a" || all[2].MemberID != "c" {
		t.Fatalf("active endpoints = %+v", all)
	}
}

func TestOwnerEndpointsIncludeNextMembership(t *testing.T) {
	membership := clustercfg.MembershipConfig{
		Active: 1,
		Next:   2,
		Versions: []clustercfg.MembershipVersion{
			{Version: 1, Members: []clustercfg.MembershipMember{
				{ID: "a", Advertise: "http://a:7700"},
				{ID: "b", Advertise: "http://b:7700"},
			}},
			{Version: 2, Members: []clustercfg.MembershipMember{
				{ID: "b", Advertise: "http://b:7700"},
				{ID: "c", Advertise: "http://c:7700"},
			}},
		},
		Owners: clustercfg.MembershipOwnerConfig{RouteLink: 1, NodeLink: 1},
	}
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(200, membership), nil
	})}
	reg := NewRegistryWithClient("http://bootstrap:7700", client)
	active, err := reg.ActiveEndpoints(t.Context())
	if err != nil {
		t.Fatalf("active endpoints: %v", err)
	}
	if len(active) != 2 {
		t.Fatalf("active endpoints=%+v, want two active members", active)
	}
	owners, err := reg.OwnerEndpoints(t.Context())
	if err != nil {
		t.Fatalf("owner endpoints: %v", err)
	}
	if len(owners) != 3 || owners[0].MemberID != "a" || owners[1].MemberID != "b" || owners[2].MemberID != "c" {
		t.Fatalf("owner endpoints=%+v", owners)
	}
}

func TestSingleMemberWithoutAdvertiseUsesBootstrap(t *testing.T) {
	membership := clustercfg.MembershipConfig{
		Active: 1,
		Versions: []clustercfg.MembershipVersion{{
			Version: 1,
			Members: []clustercfg.MembershipMember{{ID: "solo"}},
		}},
		Owners: clustercfg.MembershipOwnerConfig{RouteLink: 1},
	}
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(200, membership), nil
	})}
	reg := NewRegistryWithClient("http://bootstrap:7700", client)
	eps, err := reg.RouteCandidates(t.Context(), "/group")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(eps) != 1 || eps[0].BaseURL != "http://bootstrap:7700" || eps[0].Client != client {
		t.Fatalf("endpoint = %+v", eps)
	}
}

func TestHTTPBaseNormalizesEndpoints(t *testing.T) {
	base, _, err := HTTPBase("127.0.0.1:7700", nil)
	if err != nil || base != "http://127.0.0.1:7700" {
		t.Fatalf("plain base=%q err=%v", base, err)
	}
	base, _, err = HTTPBase("https://registry.example:7700/", nil)
	if err != nil || base != "https://registry.example:7700" {
		t.Fatalf("url base=%q err=%v", base, err)
	}
	base, _, err = HTTPBase("/run/registry.sock", nil)
	if err != nil || base != "http://registry" {
		t.Fatalf("uds base=%q err=%v", base, err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func jsonResponse(code int, v any) *http.Response {
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(v)
	return &http.Response{
		StatusCode: code,
		Status:     strconv.Itoa(code) + " " + http.StatusText(code),
		Body:       io.NopCloser(&buf),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}
}
