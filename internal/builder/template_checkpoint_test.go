package builder

import (
	"reflect"
	"strings"
	"testing"
)

func TestTemplateSnapshotArgsExcludePauseCheckpointPolicy(t *testing.T) {
	want := []string{
		"snapshot", "--sandbox-id", "builder-sandbox", "--output", "/work/build", "--run-root", "/run/sandbox",
	}
	got := templateSnapshotArgs("builder-sandbox", "/work/build", "/run/sandbox")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("builder snapshot argv = %#v, want %#v", got, want)
	}
	joined := strings.Join(got, " ")
	for _, forbidden := range []string{"--merge-ref", "--drop-caches"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("builder snapshot argv contains %s: %#v", forbidden, got)
		}
	}
}
