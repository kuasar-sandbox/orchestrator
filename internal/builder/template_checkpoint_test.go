package builder

import (
	"reflect"
	"strings"
	"testing"
)

func TestTemplateSnapshotArgsCarryCheckpointModeAndExcludePausePolicy(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode string
		want string
	}{
		{name: "default", want: "local"},
		{name: "local", mode: "local", want: "local"},
		{name: "bundle", mode: "bundle", want: "bundle"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := []string{
				"snapshot", "--sandbox-id", "builder-sandbox", "--output", "/work/build", "--mode", tc.want, "--run-root", "/run/sandbox",
			}
			got := templateSnapshotArgs("builder-sandbox", "/work/build", "/run/sandbox", tc.mode)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("builder snapshot argv = %#v, want %#v", got, want)
			}
			joined := strings.Join(got, " ")
			for _, forbidden := range []string{"--merge-ref", "--drop-caches"} {
				if strings.Contains(joined, forbidden) {
					t.Fatalf("builder snapshot argv contains %s: %#v", forbidden, got)
				}
			}
		})
	}
}
