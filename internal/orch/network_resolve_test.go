package orch

import (
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

// capturingVS records the AttachReq it received and returns a fixed port. It
// satisfies vsClient so attachNetwork can be asserted precisely.
type capturingVS struct {
	got vswitch.AttachReq
}

func (c *capturingVS) Attach(_ context.Context, req vswitch.AttachReq) (*vswitch.Port, error) {
	c.got = req
	return &vswitch.Port{Port: "7", FloatingIP: "169.254.1.9", MAC: "02:00:00:00:00:09", InnerIP: req.InnerIP}, nil
}

func (capturingVS) Detach(context.Context, string) error { return nil }
func (capturingVS) TapFD(string) vswitch.TapFD           { return vswitch.TapFD{Exec: []string{"true"}} }

// netTestCfg builds the node config with explicit e2b/bare profile nets + node
// DNS/hostname defaults, mirroring config.applyDefaults without invoking it.
func netTestCfg() *config.Config {
	cfg := &config.Config{}
	cfg.Sandbox.Network.Hostname = "sandbox"
	cfg.Sandbox.Network.DNS = []string{"169.254.169.253"}
	cfg.Sandbox.Network.E2B = config.ProfileNet{InnerIP: "169.254.0.21/30", Nexthop: "169.254.0.22"}
	cfg.Sandbox.Network.Bare = config.ProfileNet{InnerIP: "169.254.1.1/31", Nexthop: "169.254.1.0"}
	return cfg
}

func TestResolveNetworkDefaults(t *testing.T) {
	o := testOrchCfg(t, netTestCfg())

	// No overrides: every field falls back to the profile/node default.
	got, err := o.resolveNetwork(types.ProfileE2B, sandboxcfg.NetworkSpec{}, "sandbox")
	if err != nil {
		t.Fatalf("resolveNetwork e2b default: %v", err)
	}
	if got.InnerIP != "169.254.0.21/30" || got.Nexthop != "169.254.0.22" ||
		got.Hostname != "sandbox" || len(got.DNS) != 1 || got.DNS[0] != "169.254.169.253" {
		t.Fatalf("e2b default NetworkSpec = %+v", got)
	}

	bare, err := o.resolveNetwork(types.ProfileBare, sandboxcfg.NetworkSpec{}, "sandbox")
	if err != nil {
		t.Fatalf("resolveNetwork bare default: %v", err)
	}
	if bare.InnerIP != "169.254.1.1/31" || bare.Nexthop != "169.254.1.0" {
		t.Fatalf("bare default NetworkSpec = %+v", bare)
	}
}

func TestResolveNetworkExplicitFieldsWin(t *testing.T) {
	o := testOrchCfg(t, netTestCfg())

	in := sandboxcfg.NetworkSpec{
		Hostname:         "myhost",
		DNS:              []string{"8.8.8.8", "1.1.1.1"},
		InnerIP:          "10.0.0.5/24",
		Nexthop:          "10.0.0.1",
		TransitGatewayIP: "192.0.2.1",
		TransitGeneveVNI: 42,
		TransitMAC:       "aa:bb:cc:dd:ee:ff",
	}
	got, err := o.resolveNetwork(types.ProfileE2B, in, "ignored-default")
	if err != nil {
		t.Fatalf("resolveNetwork explicit: %v", err)
	}
	// Every explicit value survives verbatim; the default hostname is NOT used.
	if got.Hostname != "myhost" || len(got.DNS) != 2 || got.DNS[1] != "1.1.1.1" ||
		got.InnerIP != "10.0.0.5/24" || got.Nexthop != "10.0.0.1" ||
		got.TransitGatewayIP != "192.0.2.1" || got.TransitGeneveVNI != 42 || got.TransitMAC != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("explicit NetworkSpec = %+v", got)
	}
}

func TestResolveNetworkPartialOverride(t *testing.T) {
	o := testOrchCfg(t, netTestCfg())

	// Only inner_ip + transit overridden; nexthop/hostname/dns fall back.
	got, err := o.resolveNetwork(types.ProfileE2B, sandboxcfg.NetworkSpec{
		InnerIP:          "10.0.0.5/24",
		TransitGatewayIP: "192.0.2.1",
	}, "sandbox")
	if err != nil {
		t.Fatalf("resolveNetwork partial: %v", err)
	}
	if got.InnerIP != "10.0.0.5/24" || got.Nexthop != "169.254.0.22" ||
		got.Hostname != "sandbox" || got.TransitGatewayIP != "192.0.2.1" || got.TransitGeneveVNI != 0 {
		t.Fatalf("partial NetworkSpec = %+v", got)
	}
}

func TestResolveNetworkDefaultHostnameCallerChosen(t *testing.T) {
	o := testOrchCfg(t, netTestCfg())

	// Build path passes a build-<short-id> default hostname; no override → that wins.
	got, err := o.resolveNetwork(types.ProfileE2B, sandboxcfg.NetworkSpec{}, "build-abcd1234")
	if err != nil {
		t.Fatalf("resolveNetwork build default hostname: %v", err)
	}
	if got.Hostname != "build-abcd1234" {
		t.Fatalf("build default hostname = %q, want build-abcd1234", got.Hostname)
	}
}

func TestResolveNetworkBadInnerIPCIDR(t *testing.T) {
	o := testOrchCfg(t, netTestCfg())

	if _, err := o.resolveNetwork(types.ProfileE2B, sandboxcfg.NetworkSpec{InnerIP: "not-a-cidr"}, "sandbox"); err == nil {
		t.Fatal("resolveNetwork accepted a non-CIDR inner_ip")
	}
}

func TestAttachNetworkDerivesPlainIPAndCarriesTransit(t *testing.T) {
	o := testOrchCfg(t, netTestCfg())
	vs := &capturingVS{}
	o.vs = vs

	// CIDR → plain IP, all three transit fields passed through.
	port, err := o.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{
		InnerIP:          "10.0.0.5/24",
		TransitGatewayIP: "192.0.2.1",
		TransitGeneveVNI: 42,
		TransitMAC:       "aa:bb:cc:dd:ee:ff",
	})
	if err != nil {
		t.Fatalf("attachNetwork: %v", err)
	}
	if port.Port != "7" {
		t.Fatalf("attachNetwork port = %+v", port)
	}
	if vs.got.InnerIP != "10.0.0.5" { // plain IP, not CIDR
		t.Fatalf("attachNetwork plain InnerIP = %q, want 10.0.0.5", vs.got.InnerIP)
	}
	if vs.got.TransitGatewayIP != "192.0.2.1" || vs.got.TransitGeneveVNI != 42 || vs.got.TransitMAC != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("attachNetwork transit = %+v", vs.got)
	}
}

func TestAttachNetworkNoTransitOmitsZeroFields(t *testing.T) {
	o := testOrchCfg(t, netTestCfg())
	vs := &capturingVS{}
	o.vs = vs

	// No transit configured: AttachReq carries only the plain InnerIP; transit
	// fields are zero (the vswitch.Attach CLI omits their flags).
	if _, err := o.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{
		InnerIP: "169.254.1.1/31",
	}); err != nil {
		t.Fatalf("attachNetwork no-transit: %v", err)
	}
	if vs.got.InnerIP != "169.254.1.1" || vs.got.TransitGatewayIP != "" ||
		vs.got.TransitGeneveVNI != 0 || vs.got.TransitMAC != "" {
		t.Fatalf("no-transit AttachReq = %+v", vs.got)
	}
}

// TestBuildSpecForPassesThroughResolvedNetwork asserts BuildSpecFor does NOT
// re-derive network defaults from metadata: it passes pendingBuild.network and
// pendingBuild.spec straight through (including capacity from pend.spec).
func TestBuildSpecForPassesThroughResolvedNetwork(t *testing.T) {
	cfg := netTestCfg()
	cfg.Builder.VCPU = 2
	cfg.Builder.Memory = "4GiB"
	o := testOrchCfg(t, cfg)
	b := &types.Build{BuildID: "build-x", Profile: types.ProfileE2B}
	o.pend[b.BuildID] = &pendingBuild{
		build: b, workdir: t.TempDir(),
		network: sandboxcfg.NetworkSpec{
			Hostname: "build-deadbeef", DNS: []string{"8.8.8.8"},
			InnerIP: "10.0.0.9/24", Nexthop: "10.0.0.1",
			TransitGatewayIP: "192.0.2.1", TransitGeneveVNI: 7, TransitMAC: "aa:bb:cc:dd:ee:ff",
		},
		// capacity from pend.spec is the source of VCPU/Memory (no 2nd ParseSpec).
		spec: sandboxcfg.SandboxSpec{Resource: sandboxcfg.ResourceSpec{Capacity: &rtconfig.CapacityConfig{CPU: 4, Memory: "8GiB"}}},
	}

	spec, _, found, err := o.BuildSpecFor(context.Background(), "build:"+b.BuildID)
	if err != nil || !found {
		t.Fatalf("BuildSpecFor: found=%t err=%v", found, err)
	}
	if spec.Net.InnerIP != "10.0.0.9/24" || spec.Net.Nexthop != "10.0.0.1" ||
		spec.Net.Hostname != "build-deadbeef" || len(spec.Net.DNS) != 1 || spec.Net.DNS[0] != "8.8.8.8" {
		t.Fatalf("BuildSpec Net = %+v", spec.Net)
	}
	if spec.VCPU != 4 || spec.Memory != "8GiB" {
		t.Fatalf("BuildSpec capacity = vcpu=%d mem=%q (want from pend.spec)", spec.VCPU, spec.Memory)
	}
}

// TestBuildSpecForNoTransitInBuildNet asserts transit_* never reaches the guest
// BuildNet (it is host-side, consumed by attachNetwork).
func TestBuildSpecForNoTransitInBuildNet(t *testing.T) {
	o := testOrchCfg(t, netTestCfg())
	b := &types.Build{BuildID: "build-t", Profile: types.ProfileE2B}
	o.pend[b.BuildID] = &pendingBuild{
		build: b, workdir: t.TempDir(),
		network: sandboxcfg.NetworkSpec{InnerIP: "169.254.0.21/30", Nexthop: "169.254.0.22"},
	}
	spec, _, _, err := o.BuildSpecFor(context.Background(), "build:"+b.BuildID)
	if err != nil {
		t.Fatalf("BuildSpecFor: %v", err)
	}
	// BuildNet struct has no transit fields at all; this just asserts the shape
	// carries inner_ip/nexthop/hostname/dns and nothing transit-related.
	if spec.Net.InnerIP == "" {
		t.Fatalf("BuildSpec Net empty: %+v", spec.Net)
	}
}

func TestRegisterBuildRejectsInvalidNetworkMetadata(t *testing.T) {
	o := testOrchCfg(t, netTestCfg())
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	// inner_ip that is not a CIDR must fail at register with 400, not at build time.
	_, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Profile: types.ProfileE2B,
		Metadata: map[string]string{
			sandboxcfg.NsNetwork: `{"inner_ip":"not-a-cidr"}`,
		},
	})
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("RegisterBuild invalid network error = %v, want ErrBadRequest", err)
	}
}

func TestRegisterBuildAcceptsValidNetworkMetadata(t *testing.T) {
	o := testOrchCfg(t, netTestCfg())
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Profile: types.ProfileE2B,
		Metadata: map[string]string{
			sandboxcfg.NsNetwork: `{"inner_ip":"10.0.0.5/24","transit_gateway_ip":"192.0.2.1","transit_geneve_vni":42,"transit_mac":"aa:bb:cc:dd:ee:ff"}`,
		},
	})
	if err != nil {
		t.Fatalf("RegisterBuild valid network: %v", err)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || stored == nil {
		t.Fatalf("stored build = %+v err=%v", stored, err)
	}
}

func TestTriggerBuildRejectsInvalidNetworkMetadata(t *testing.T) {
	o := testOrchCfg(t, netTestCfg())
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)

	// Register with valid metadata, then trigger with an override that makes the
	// merged network invalid (bad nexthop) — must fail at trigger with 400.
	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{Profile: types.ProfileE2B})
	if err != nil {
		t.Fatal(err)
	}
	err = o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{
		FromImage: "registry.test/base:latest",
		Metadata: map[string]string{
			sandboxcfg.NsNetwork: `{"nexthop":"not-an-ip"}`,
		},
	}, api.BuildAuth{})
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("TriggerBuild invalid network error = %v, want ErrBadRequest", err)
	}
}
