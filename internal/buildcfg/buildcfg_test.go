package buildcfg

import (
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
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

func TestExtractRejectsAmbiguousBuilderJSON(t *testing.T) {
	for _, raw := range []string{
		`null`,
		`[]`,
		`{"resources":null}`,
		`{"referer":{"enabled":null}}`,
		`{"registry":{"tls":null}}`,
		`{"referer":{},"referer":{}}`,
		`{"unknown":true}`,
		`{} {}`,
	} {
		t.Run(raw, func(t *testing.T) {
			if _, _, err := Extract(map[string]string{NsBuilder: raw}); err == nil {
				t.Fatalf("ambiguous builder JSON accepted: %s", raw)
			}
		})
	}
}
