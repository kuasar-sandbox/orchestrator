package sandboxcfg

import (
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestActiveHostTemplateKeepsURIAndCapturedBase(t *testing.T) {
	p := Params{OverlayDiffTpl: "mkfs://ext4?size=16MiB"}
	for _, mode := range []types.ArtifactDiskMode{types.ArtifactDiskSingle, types.ArtifactDiskOverlay} {
		for _, captured := range []bool{false, true} {
			active, err := p.hostActiveRoot(types.ArtifactDiskShape{Mode: mode, HasActiveBase: captured})
			if err != nil {
				t.Fatal(err)
			}
			template := active.DiffTemplate
			if active.Overlay != nil {
				template = active.Overlay.DiffTemplate
			}
			want := "mkfs://ext4?size=16777216"
			if captured {
				want = ""
			}
			if template != want {
				t.Fatalf("mode=%s captured=%t template=%q", mode, captured, template)
			}
		}
	}
}
