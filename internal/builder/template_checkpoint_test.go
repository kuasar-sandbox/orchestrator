package builder

import (
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func TestTemplateSnapshotArgsCarryCheckpointModeAndPolicy(t *testing.T) {
	merge, drop := false, true
	for _, tc := range []struct {
		name   string
		mode   string
		want   string
		policy sandboxcfg.SnapshotPolicy
	}{
		{name: "default", want: "local"},
		{name: "local", mode: "local", want: "local"},
		{name: "bundle", mode: "bundle", want: "bundle", policy: sandboxcfg.SnapshotPolicy{MergeRef: &merge, DropCaches: &drop}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := []string{
				"snapshot", "--path-id", "c", "--output", "/work/build", "--mode", tc.want, "--run-root", "/run/sandbox",
			}
			if tc.policy.MergeRef != nil {
				want = append(want, "--merge-ref=false")
			}
			if tc.policy.DropCaches != nil {
				want = append(want, "--drop-caches=true")
			}
			got := templateSnapshotArgs("c", "/work/build", "/run/sandbox", tc.mode, tc.policy)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("builder snapshot argv = %#v, want %#v", got, want)
			}
		})
	}
}
