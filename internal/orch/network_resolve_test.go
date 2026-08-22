package orch

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

type capturingNetworkVS struct {
	req vswitch.AttachReq
}

func intPointer(value int) *int { return &value }

func (v *capturingNetworkVS) Attach(_ context.Context, req vswitch.AttachReq) (*vswitch.Port, error) {
	v.req = req
	return &vswitch.Port{
		Port:       "7",
		FloatingIP: "169.254.1.9",
		MAC:        "02:00:00:00:00:09",
		InnerIP:    req.InnerIP,
	}, nil
}

func (*capturingNetworkVS) Detach(context.Context, string) error { return nil }
func (*capturingNetworkVS) TapFD(string) vswitch.TapFD {
	return vswitch.TapFD{Exec: []string{"true"}}
}

func buildNetworkTestConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Sandbox.Network.Hostname = "sandbox"
	cfg.Sandbox.Network.DNS = []string{"169.254.169.253"}
	cfg.Sandbox.Network.E2B = config.ProfileNet{
		InnerIP: "169.254.0.21/30",
		Nexthop: "169.254.0.22",
	}
	cfg.Sandbox.Network.Bare = config.ProfileNet{
		InnerIP: "169.254.1.1/31",
		Nexthop: "169.254.1.0",
	}
	return cfg
}

func TestResolveNetworkUsesProfileAndNodeDefaults(t *testing.T) {
	o := testOrchCfg(t, buildNetworkTestConfig())
	tests := []struct {
		name    string
		profile types.Profile
		ip      string
		nexthop string
	}{
		{name: "e2b", profile: types.ProfileE2B, ip: "169.254.0.21/30", nexthop: "169.254.0.22"},
		{name: "bare", profile: types.ProfileBare, ip: "169.254.1.1/31", nexthop: "169.254.1.0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := o.resolveNetwork(tt.profile, sandboxcfg.NetworkSpec{}, "default-host")
			if err != nil {
				t.Fatal(err)
			}
			if got.InnerIP != tt.ip || got.Nexthop != tt.nexthop ||
				got.Hostname != "default-host" ||
				!reflect.DeepEqual(got.DNS, []string{"169.254.169.253"}) {
				t.Fatalf("resolved network = %+v", got)
			}
		})
	}
}

func TestResolveNetworkPreservesEveryExplicitField(t *testing.T) {
	o := testOrchCfg(t, buildNetworkTestConfig())
	want := sandboxcfg.NetworkSpec{
		Hostname:         "tenant-host",
		DNS:              []string{"8.8.8.8", "1.1.1.1"},
		InnerIP:          "10.0.0.5/24",
		Nexthop:          "10.0.0.1",
		TransitGatewayIP: "192.0.2.1",
		TransitGeneveVNI: 42,
		TransitMAC:       "aa:bb:cc:dd:ee:ff",
	}
	got, err := o.resolveNetwork(types.ProfileE2B, want, "ignored")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resolved network = %+v, want %+v", got, want)
	}
}

func TestAttachNetworkUsesResolvedSpec(t *testing.T) {
	o := testOrchCfg(t, buildNetworkTestConfig())
	vs := &capturingNetworkVS{}
	o.vs = vs
	network := sandboxcfg.NetworkSpec{
		InnerIP:          "10.0.0.5/24",
		TransitGatewayIP: "192.0.2.1",
		TransitGeneveVNI: 42,
		TransitMAC:       "aa:bb:cc:dd:ee:ff",
	}
	if _, err := o.attachNetwork(context.Background(), network); err != nil {
		t.Fatal(err)
	}
	want := vswitch.AttachReq{
		InnerIP:          "10.0.0.5",
		TransitGatewayIP: "192.0.2.1",
		TransitGeneveVNI: 42,
		TransitMAC:       "aa:bb:cc:dd:ee:ff",
	}
	if vs.req != want {
		t.Fatalf("AttachReq = %+v, want %+v", vs.req, want)
	}

	if _, err := o.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{
		InnerIP: "169.254.1.1/31",
	}); err != nil {
		t.Fatal(err)
	}
	if vs.req.InnerIP != "169.254.1.1" || vs.req.TransitGatewayIP != "" ||
		vs.req.TransitGeneveVNI != 0 || vs.req.TransitMAC != "" {
		t.Fatalf("no-transit AttachReq = %+v", vs.req)
	}
}

func TestRestoreNetworkPrecedenceFeedsAttachAndGuestFromSameSpec(t *testing.T) {
	o := testOrchCfg(t, buildNetworkTestConfig())
	vs := &capturingNetworkVS{}
	o.vs = vs
	inherited := sandboxcfg.NetworkSpec{
		Hostname:         "snapshot-host",
		DNS:              []string{"9.9.9.9"},
		InnerIP:          "10.0.0.5/24",
		Nexthop:          "10.0.0.1",
		TransitGatewayIP: "192.0.2.1",
		TransitGeneveVNI: 7,
		TransitMAC:       "aa:bb:cc:dd:ee:ff",
	}
	specified := sandboxcfg.NetworkSpec{
		Hostname: "create-host",
		DNS:      []string{"1.1.1.1"},
	}
	network, err := o.resolveNetwork(
		types.ProfileE2B,
		sandboxcfg.MergeNetwork(inherited, specified),
		"sandbox",
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.attachNetwork(context.Background(), network); err != nil {
		t.Fatal(err)
	}
	params := o.sandboxParams(
		&types.Sandbox{InnerIP: network.InnerIP},
		types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindSnp, Ref: "manifest://" + strings.Repeat("a", 64)},
		sandboxcfg.SandboxSpec{},
		network,
		rtconfig.ResourcesConfig{},
	)
	if !reflect.DeepEqual(params.Network, network) {
		t.Fatalf("guest network = %+v, want %+v", params.Network, network)
	}
	if network.Hostname != "create-host" || !reflect.DeepEqual(network.DNS, []string{"1.1.1.1"}) {
		t.Fatalf("explicit create fields did not win: %+v", network)
	}
	if vs.req.InnerIP != "10.0.0.5" || vs.req.TransitGeneveVNI != 7 {
		t.Fatalf("snapshot fields did not reach AttachReq: %+v", vs.req)
	}
}

func TestResolveBuildNetworksSeparatesTemporaryAndTemplateHostname(t *testing.T) {
	o := testOrchCfg(t, buildNetworkTestConfig())
	buildNet, templateNet, err := o.resolveBuildNetworks(
		types.ProfileE2B,
		sandboxcfg.NetworkSpec{},
		sandboxcfg.NetworkSpec{
			TransitGatewayIP: "192.0.2.1",
			TransitGeneveVNI: 42,
			TransitMAC:       "aa:bb:cc:dd:ee:ff",
		},
		"build-deadbeef",
	)
	if err != nil {
		t.Fatal(err)
	}
	if buildNet.Hostname != "build-deadbeef" {
		t.Fatalf("build hostname = %q", buildNet.Hostname)
	}
	if templateNet.Hostname != "sandbox" || strings.HasPrefix(templateNet.Hostname, "build-") {
		t.Fatalf("template hostname = %q", templateNet.Hostname)
	}
	buildNet.Hostname = ""
	templateNet.Hostname = ""
	if !reflect.DeepEqual(buildNet, templateNet) {
		t.Fatalf("build/template network differ beyond default hostname: build=%+v template=%+v", buildNet, templateNet)
	}
}

func TestResolveBuildNetworksFromTemplatePrecedence(t *testing.T) {
	o := testOrchCfg(t, buildNetworkTestConfig())
	inherited := sandboxcfg.NetworkSpec{
		Hostname:         "source-host",
		DNS:              []string{"9.9.9.9"},
		InnerIP:          "10.0.0.5/24",
		Nexthop:          "10.0.0.1",
		TransitGatewayIP: "192.0.2.1",
		TransitGeneveVNI: 7,
		TransitMAC:       "aa:bb:cc:dd:ee:ff",
	}
	specified := sandboxcfg.NetworkSpec{
		Hostname:         "current-host",
		DNS:              []string{"1.1.1.1"},
		TransitGeneveVNI: 9,
	}
	buildNet, templateNet, err := o.resolveBuildNetworks(
		types.ProfileBare,
		inherited,
		specified,
		"build-ignored",
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string]sandboxcfg.NetworkSpec{
		"build":    buildNet,
		"template": templateNet,
	} {
		if got.Hostname != "current-host" || !reflect.DeepEqual(got.DNS, []string{"1.1.1.1"}) ||
			got.InnerIP != "10.0.0.5/24" || got.Nexthop != "10.0.0.1" ||
			got.TransitGatewayIP != "192.0.2.1" || got.TransitGeneveVNI != 9 ||
			got.TransitMAC != "aa:bb:cc:dd:ee:ff" {
			t.Fatalf("%s network = %+v", name, got)
		}
	}
}

func TestBuildPrepareSummaryStrictlyParsesSnapshotMetadata(t *testing.T) {
	got, err := validateBuildPrepareSummary(configsock.SnapshotPrepareSummary{
		SchemaVersion:      configsock.SnapshotPrepareSchemaVersion,
		Capacity:           configsock.SnapshotCapacity{CPU: 2, Memory: "2GiB"},
		RawNetworkMetadata: `{"hostname":"source","inner_ip":"10.0.0.5/24","nexthop":"10.0.0.1","transit_geneve_vni":23}`,
		ResolutionDigest:   strings.Repeat("a", 64),
		RequiredRefCount:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Hostname != "source" || got.InnerIP != "10.0.0.5/24" ||
		got.Nexthop != "10.0.0.1" || got.TransitGeneveVNI != 23 {
		t.Fatalf("source network = %+v", got)
	}

	_, err = validateBuildPrepareSummary(configsock.SnapshotPrepareSummary{
		SchemaVersion:      configsock.SnapshotPrepareSchemaVersion,
		Capacity:           configsock.SnapshotCapacity{CPU: 2, Memory: "2GiB"},
		RawNetworkMetadata: `{malformed`,
		ResolutionDigest:   strings.Repeat("a", 64),
		RequiredRefCount:   1,
	})
	if err == nil || !strings.Contains(err.Error(), "source-template network metadata") {
		t.Fatalf("malformed inherited network = %v", err)
	}
}

func TestBuildSpecCarriesResolvedAndTemplateNetworks(t *testing.T) {
	o := testOrchCfg(t, buildNetworkTestConfig())
	b := &types.Build{
		BuildID: "build-spec", Profile: types.ProfileE2B,
		// The outer Build demand is deliberately unrelated to the phase sandbox
		// document carried below.
		Resources: types.BuildResources{CPU: 9000, Memory: 16 << 30},
	}
	runtimeNetwork := sandboxcfg.NetworkSpec{
		Hostname: "build-spec",
		DNS:      []string{"8.8.8.8"},
		InnerIP:  "10.0.0.9/24",
		Nexthop:  "10.0.0.1",
	}
	templateNetwork := runtimeNetwork
	templateNetwork.Hostname = "sandbox"
	o.pend[b.BuildID] = &pendingBuild{
		build:           b,
		workdir:         t.TempDir(),
		network:         runtimeNetwork,
		templateNetwork: templateNetwork,
		resources: rtconfig.ResourcesConfig{
			Capacity:    rtconfig.CapacityConfig{CPU: 4, Memory: "8GiB"},
			Allocatable: rtconfig.AllocatableConfig{CPU: 4, Memory: "8GiB"},
		},
	}
	spec, _, found, err := o.BuildSpecFor(context.Background(), "build:"+b.BuildID)
	if err != nil || !found {
		t.Fatalf("BuildSpecFor: found=%t err=%v", found, err)
	}
	if spec.Net.Hostname != "build-spec" || spec.Net.InnerIP != "10.0.0.9/24" {
		t.Fatalf("runtime BuildNet = %+v", spec.Net)
	}
	if !reflect.DeepEqual(spec.TemplateNetwork, templateNetwork) {
		t.Fatalf("template network = %+v, want %+v", spec.TemplateNetwork, templateNetwork)
	}
	netJSON, err := json.Marshal(spec.Net)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(netJSON), "transit") {
		t.Fatalf("guest BuildNet contains host-only transit fields: %s", netJSON)
	}
	if spec.Resources.Capacity.CPU != 4 || spec.Resources.Capacity.Memory != "8GiB" {
		t.Fatalf("capacity = %+v", spec.Resources.Capacity)
	}
}

func TestRegisterAndTriggerRejectInvalidNetworkMetadata(t *testing.T) {
	o := testOrchCfg(t, buildNetworkTestConfig())
	ctx := context.Background()
	apiKey, _, _ := allowlistedBuildIdentity(t, o)
	_, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{
		Profile:   types.ProfileE2B,
		Resources: testBuildResources(),
		Metadata: map[string]string{
			sandboxcfg.NsNetwork: `{"inner_ip":"not-a-cidr"}`,
		},
	})
	if !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("register error = %v, want ErrBadRequest", err)
	}

	b, err := o.RegisterBuild(ctx, apiKey, api.RegisterSpec{Profile: types.ProfileE2B, Resources: testBuildResources()})
	if err != nil {
		t.Fatal(err)
	}
	err = o.TriggerBuild(ctx, apiKey, b.TemplateID, b.BuildID, api.TriggerSpec{FromImage: "registry.test/base:latest"}, api.BuildAuth{})
	if err != nil {
		t.Fatalf("sealed registered network should trigger unchanged: %v", err)
	}
}
