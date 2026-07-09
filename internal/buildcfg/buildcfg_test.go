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
