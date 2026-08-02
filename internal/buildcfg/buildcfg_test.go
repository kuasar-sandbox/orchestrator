package buildcfg

import (
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestExtractStripsBuilderNamespace(t *testing.T) {
	meta := map[string]string{
		NsBuilder:            `{"referer":{"enabled":false,"writeback":false}}`,
		sandboxcfg.NsNetwork: `{"hostname":"h"}`,
	}
	clean, opts, err := Extract(meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := clean[NsBuilder]; ok {
		t.Fatalf("builder namespace leaked into clean metadata: %+v", clean)
	}
	if clean[sandboxcfg.NsNetwork] == "" {
		t.Fatalf("sandbox metadata was not preserved: %+v", clean)
	}
	if opts.Referer == nil || opts.Referer.Enabled == nil || *opts.Referer.Enabled {
		t.Fatalf("referer.enabled not decoded as false: %+v", opts)
	}
	if opts.Referer.Writeback == nil || *opts.Referer.Writeback {
		t.Fatalf("referer.writeback not decoded as false: %+v", opts)
	}
}

func TestMergeTriState(t *testing.T) {
	tval, fval := true, false
	base := types.BuildOptions{Referer: &types.BuildRefererOptions{Enabled: &tval, Writeback: &tval}}
	over := types.BuildOptions{Referer: &types.BuildRefererOptions{Enabled: &fval}}
	got := Merge(base, over)
	if got.Referer == nil || got.Referer.Enabled == nil || *got.Referer.Enabled {
		t.Fatalf("trigger enabled=false did not override: %+v", got)
	}
	if got.Referer.Writeback == nil || !*got.Referer.Writeback {
		t.Fatalf("absent trigger writeback should preserve base: %+v", got)
	}
}

// TestExtractDecodesRegistryTLS verifies the registry.tls namespace decodes
// from the builder header (register-time), mirroring the referer path.
func TestExtractDecodesRegistryTLS(t *testing.T) {
	meta := map[string]string{
		NsBuilder: `{"registry":{"tls":{"ca_bundle_pem":"-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n"}}}`,
	}
	_, opts, err := Extract(meta)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Registry == nil || opts.Registry.TLS == nil {
		t.Fatalf("registry.tls not decoded: %+v", opts)
	}
	if opts.Registry.TLS.CABundlePEM == "" {
		t.Fatalf("ca_bundle_pem not decoded: %+v", opts)
	}
}

// TestMergePreservesRegistry verifies registry is register-time only: the
// register-time base Registry is preserved verbatim, and a trigger-time
// Registry is ignored (the trigger path is rejected upstream in TriggerBuild,
// but Merge must also not carry it over as defense-in-depth).
func TestMergePreservesRegistry(t *testing.T) {
	base := types.BuildOptions{Registry: &types.BuildRegistryOptions{
		TLS: &types.BuildRegistryTLSOptions{CABundlePEM: "base-ca"},
	}}
	over := types.BuildOptions{Registry: &types.BuildRegistryOptions{
		TLS: &types.BuildRegistryTLSOptions{InsecureSkipVerify: true},
	}}
	got := Merge(base, over)
	if got.Registry == nil || got.Registry.TLS == nil {
		t.Fatalf("register-time registry.tls lost: %+v", got)
	}
	if got.Registry.TLS.CABundlePEM != "base-ca" {
		t.Fatalf("base ca_bundle_pem not preserved: %q", got.Registry.TLS.CABundlePEM)
	}
	if got.Registry.TLS.InsecureSkipVerify {
		t.Fatalf("trigger-time registry.tls leaked into merged result: %+v", got.Registry.TLS)
	}
}

// TestMergePreservesRegistryWhenTriggerHasNone verifies the register-time
// Registry survives a trigger that carries no builder config at all.
func TestMergePreservesRegistryWhenTriggerHasNone(t *testing.T) {
	base := types.BuildOptions{Registry: &types.BuildRegistryOptions{
		TLS: &types.BuildRegistryTLSOptions{InsecureSkipVerify: true},
	}}
	got := Merge(base, types.BuildOptions{})
	if got.Registry == nil || got.Registry.TLS == nil || !got.Registry.TLS.InsecureSkipVerify {
		t.Fatalf("register-time registry.tls lost on empty trigger: %+v", got)
	}
}
