package sandboxcfg

import (
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestParseSpecNamespaces(t *testing.T) {
	// Absent / unrelated metadata => zero value, no error.
	if s, err := ParseSpec(nil); err != nil || !s.Network.IsZero() || s.Launch != nil {
		t.Fatalf("absent: %+v %v", s, err)
	}
	if s, err := ParseSpec(map[string]string{"foo": "bar"}); err != nil || !s.Network.IsZero() {
		t.Fatalf("unrelated key: %+v %v", s, err)
	}

	meta := map[string]string{
		NsNetwork:  `{"hostname":"h1","dns":["1.1.1.1","8.8.8.8"],"inner_ip":"10.0.0.5/30","nexthop":"10.0.0.4","transit_gateway_ip":"172.16.0.1","transit_geneve_vni":4242,"transit_mac":"02:00:00:00:00:09"}`,
		NsResource: `{"capacity":{"cpu":4,"memory":"8GiB"}}`,
		NsRestore:  `{"prefetch":"memory"}`,
		NsLaunch:   `{"exec":"/app","args":["-x"],"restart":"always","stop_signal":"SIGINT","user":"1000:1000"}`,
		NsMounts:   `[{"target":"/data","type":"tmpfs"}]`,
		NsFiles:    `[{"path":"/etc/app.conf","content":"k=v","mode":"0644"}]`,
		NsInit:     `[{"exec":"/bin/setup","args":["--once"]}]`,
		NsMetadata: `{"e2b.start_cmd":"npm run start"}`,
	}
	s, err := ParseSpec(meta)
	if err != nil {
		t.Fatalf("valid: %v", err)
	}
	if s.Network.Hostname != "h1" || len(s.Network.DNS) != 2 || s.Network.InnerIP != "10.0.0.5/30" ||
		s.Network.Nexthop != "10.0.0.4" || s.Network.TransitGatewayIP != "172.16.0.1" ||
		s.Network.TransitGeneveVNI != 4242 || s.Network.TransitMAC != "02:00:00:00:00:09" {
		t.Fatalf("network parsed wrong: %+v", s.Network)
	}
	if s.Resource.Capacity == nil || s.Resource.Capacity.CPU != 4 || s.Resource.Capacity.Memory != "8GiB" {
		t.Fatalf("resource parsed wrong: %+v", s.Resource)
	}
	if s.Restore.Prefetch != "memory" {
		t.Fatalf("restore parsed wrong: %+v", s.Restore)
	}
	// stop_signal (snake_case) must bind via the runtime config's yaml tags.
	if s.Launch == nil || s.Launch.Exec != "/app" || s.Launch.StopSignal != "SIGINT" || s.Launch.User != "1000:1000" {
		t.Fatalf("launch parsed wrong: %+v", s.Launch)
	}
	if len(s.Mounts) != 1 || s.Mounts[0].Target != "/data" || s.Mounts[0].Type != "tmpfs" {
		t.Fatalf("mounts parsed wrong: %+v", s.Mounts)
	}
	if len(s.Files) != 1 || s.Files[0].Path != "/etc/app.conf" {
		t.Fatalf("files parsed wrong: %+v", s.Files)
	}
	if len(s.Init) != 1 || s.Init[0].Exec != "/bin/setup" {
		t.Fatalf("init parsed wrong: %+v", s.Init)
	}
	if s.Metadata["e2b.start_cmd"] != "npm run start" {
		t.Fatalf("metadata parsed wrong: %+v", s.Metadata)
	}
}

func TestNormalizeRestoreMetadata(t *testing.T) {
	if got, err := NormalizeRestoreMetadata(nil); err != nil || got != nil {
		t.Fatalf("absent restore: got=%v err=%v", got, err)
	}
	original := map[string]string{NsRestore: " { \"prefetch\" : \"memory\" } ", "keep": "value"}
	got, err := NormalizeRestoreMetadata(original)
	if err != nil {
		t.Fatal(err)
	}
	if got[NsRestore] != `{"prefetch":"memory"}` || got["keep"] != "value" {
		t.Fatalf("normalized metadata = %+v", got)
	}
	if original[NsRestore] == got[NsRestore] {
		t.Fatal("normalization mutated or reused the caller's restore value")
	}
	if got, err := NormalizeRestoreMetadata(map[string]string{NsRestore: `{}`}); err != nil || got[NsRestore] != `{}` {
		t.Fatalf("missing prefetch: got=%v err=%v", got, err)
	}

	bad := map[string]string{
		"empty":           ``,
		"null-object":     `null`,
		"array":           `[]`,
		"invalid-json":    `{`,
		"trailing":        `{"prefetch":"memory"} {}`,
		"unknown":         `{"other":true}`,
		"file-refs":       `{"file_refs":"trust"}`,
		"wrong-case":      `{"Prefetch":"memory"}`,
		"bad-enum":        `{"prefetch":"disk"}`,
		"empty-enum":      `{"prefetch":""}`,
		"non-string":      `{"prefetch":true}`,
		"null-prefetch":   `{"prefetch":null}`,
		"object-prefetch": `{"prefetch":{}}`,
	}
	for name, raw := range bad {
		t.Run(name, func(t *testing.T) {
			if _, err := NormalizeRestoreMetadata(map[string]string{NsRestore: raw}); err == nil {
				t.Fatalf("accepted invalid restore value %q", raw)
			}
		})
	}
}

func TestParseSpecNetworkValidation(t *testing.T) {
	for name, bad := range map[string]string{
		"bad-json":    `{not json`,
		"bad-cidr":    `{"inner_ip":"10.0.0.5"}`, // a plain IP, not a CIDR
		"bad-nexthop": `{"nexthop":"nope"}`,
		"bad-gateway": `{"transit_gateway_ip":"999.1.1.1"}`,
		"bad-mac":     `{"transit_mac":"zz:zz"}`,
	} {
		if _, err := ParseSpec(map[string]string{NsNetwork: bad}); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

func baseParams(profile types.Profile) Params {
	tmpl := types.TemplateID{Profile: profile, Kind: types.KindImg, Key: strings.Repeat("a", 64)}
	return Params{
		Sandbox:  &types.Sandbox{ID: "s1", TemplateID: tmpl.String(), InnerIP: "10.0.0.5/30", PortMAC: "02:00:00:00:00:01"},
		Template: tmpl, Runtime: "/r/sandbox-runtime.erofs", Kernel: "/r/vmlinux",
		VCPU: 2, Memory: "2GiB",
	}
}

func TestBuildE2BForbidsLaunch(t *testing.T) {
	p := baseParams(types.ProfileE2B)
	p.Spec.Launch = &rtconfig.LaunchConfig{Exec: "/evil"}
	if _, err := p.BuildYAML(); err == nil {
		t.Fatal("e2b profile must reject a launch override (envd owns launch)")
	}
	// bare accepts it.
	pb := baseParams(types.ProfileBare)
	pb.Spec.Launch = &rtconfig.LaunchConfig{Exec: "/app"}
	if _, err := pb.BuildYAML(); err != nil {
		t.Fatalf("bare launch override should be accepted: %v", err)
	}
}

func TestBuildInjectsNetworkMetadata(t *testing.T) {
	p := baseParams(types.ProfileE2B)
	p.Network = NetworkSpec{Hostname: "h1", InnerIP: "10.0.0.5/30", Nexthop: "10.0.0.4"}
	b, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	// The resolved logical network rides the snapshot via metadata["kuasar-sandbox.network"].
	if !strings.Contains(out, "kuasar-sandbox.network") || !strings.Contains(out, "h1") {
		t.Fatalf("network not injected into metadata:\n%s", out)
	}
}

func TestBuildRendersTapFDSocket(t *testing.T) {
	p := baseParams(types.ProfileE2B)
	p.TapFD = TapFD{
		Socket:  "/run/kuasar/connector/sw0/tapfd.sock",
		Request: "VSWITCH=sw0 PORT=7",
	}
	b, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	for _, want := range []string{
		"tapfd:",
		"socket: /run/kuasar/connector/sw0/tapfd.sock",
		"request: VSWITCH=sw0 PORT=7",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("rendered yaml missing %q:\n%s", want, out)
		}
	}
}

func TestBuildRendersPrefetchOnlyForRestore(t *testing.T) {
	cold := baseParams(types.ProfileE2B)
	cold.Spec.Restore.Prefetch = "memory"
	b, err := cold.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "prefetch:") || strings.Contains(string(b), "restore:") {
		t.Fatalf("cold boot rendered restore policy:\n%s", b)
	}

	restore := baseParams(types.ProfileE2B)
	restore.Template.Kind = types.KindSnp
	restore.Sandbox.TemplateID = restore.Template.String()
	restore.Spec.Restore.Prefetch = "memory"
	b, err = restore.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "restore:") || !strings.Contains(string(b), "prefetch: memory") {
		t.Fatalf("snapshot restore omitted prefetch policy:\n%s", b)
	}

	resume := baseParams(types.ProfileE2B)
	resume.Sandbox.SnapshotRef = strings.Repeat("b", 64)
	resume.Spec.Restore.Prefetch = "memory"
	b, err = resume.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "restore:") || !strings.Contains(string(b), "prefetch: memory") {
		t.Fatalf("paused image resume omitted prefetch policy:\n%s", b)
	}

	restore.Spec.Restore.Prefetch = "off"
	b, err = restore.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `prefetch: "off"`) {
		t.Fatalf("snapshot restore omitted explicit off policy:\n%s", b)
	}
}

func TestSetCapacityRoundTrip(t *testing.T) {
	// cpu/memory fold into a well-formed resource namespace ParseSpec reads back.
	meta := SetCapacity(nil, 4, 8192)
	s, err := ParseSpec(meta)
	if err != nil {
		t.Fatal(err)
	}
	if s.Resource.Capacity == nil || s.Resource.Capacity.CPU != 4 || s.Resource.Capacity.Memory != "8192MiB" {
		t.Fatalf("capacity round-trip: %+v", s.Resource.Capacity)
	}
	if SetCapacity(nil, 0, 0) != nil {
		t.Fatal("zero cpu/memory must be a no-op")
	}
}

func TestMergeNetworkExplicitWins(t *testing.T) {
	// snapshot-inherited network; create explicitly sets only hostname.
	snap := NetworkSpec{Hostname: "snap-host", Nexthop: "10.0.0.4", TransitGeneveVNI: 100, DNS: []string{"9.9.9.9"}}
	create := NetworkSpec{Hostname: "create-host"}
	m := MergeNetwork(snap, create)
	if m.Hostname != "create-host" {
		t.Fatalf("explicit create hostname must win: %q", m.Hostname)
	}
	if m.Nexthop != "10.0.0.4" || m.TransitGeneveVNI != 100 || len(m.DNS) != 1 {
		t.Fatalf("snapshot must fill fields create left unset: %+v", m)
	}
}

func TestMergeMetadataOverWins(t *testing.T) {
	base := map[string]string{NsNetwork: "from-template", NsLaunch: "tmpl-launch"}
	over := map[string]string{NsNetwork: "from-create"}
	m := MergeMetadata(base, over)
	if m[NsNetwork] != "from-create" || m[NsLaunch] != "tmpl-launch" {
		t.Fatalf("create should win per namespace, template fills the rest: %+v", m)
	}
}

func TestMergeCreateMetadataKeepsRestoreRequestScoped(t *testing.T) {
	defaults := map[string]string{
		NsNetwork: "from-defaults", NsRestore: `{"prefetch":"memory"}`,
	}
	withoutRestore := MergeCreateMetadata(defaults, map[string]string{NsLaunch: "from-request"})
	if withoutRestore[NsNetwork] != "from-defaults" || withoutRestore[NsLaunch] != "from-request" {
		t.Fatalf("normal create metadata did not merge: %+v", withoutRestore)
	}
	if _, ok := withoutRestore[NsRestore]; ok {
		t.Fatalf("restore leaked from defaults: %+v", withoutRestore)
	}
	withRestore := MergeCreateMetadata(defaults, map[string]string{NsRestore: `{"prefetch":"off"}`})
	if withRestore[NsRestore] != `{"prefetch":"off"}` {
		t.Fatalf("explicit create restore did not win: %+v", withRestore)
	}
}

func TestBuildImgCapacityOverride(t *testing.T) {
	p := baseParams(types.ProfileBare)
	p.Spec.Resource.Capacity = &rtconfig.CapacityConfig{CPU: 8, Memory: "16GiB"}
	b, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "cpu: 8") || !strings.Contains(string(b), "16GiB") {
		t.Fatalf("img capacity override not applied:\n%s", b)
	}
}
