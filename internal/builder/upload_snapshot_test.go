package builder

import (
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

func publishSpec(parent string) *configsock.BuildSpec {
	return &configsock.BuildSpec{
		BuildID:               "0198f7a1-1234-7234-9abc-0123456789ab",
		PublishLocationParent: parent,
		Paths: configsock.BuildPaths{
			ManifestConfig: "/etc/flatten/manifest.yaml",
			SandboxCtl:     "/usr/bin/sandbox-ctl",
		},
	}
}

// TestPublishArtifactArgsMintPublicationDateAtUploadTime pins the bucketing
// clock to the moment the argv is built: the same spec resolved before and
// after UTC midnight must publish into different date buckets. This is the
// regression test for minting the name at spec-resolution time, which froze
// the date before the (potentially multi-hour) build ran.
func TestPublishArtifactArgsMintPublicationDateAtUploadTime(t *testing.T) {
	spec := publishSpec("file:///mnt/shared/snapshots")
	lateEvening := time.Date(2026, 8, 24, 23, 59, 0, 0, time.UTC)
	earlyNextDay := time.Date(2026, 8, 25, 0, 1, 0, 0, time.UTC)

	before, err := publishArtifactArgs(spec, "/work/build", lateEvening)
	if err != nil {
		t.Fatal(err)
	}
	after, err := publishArtifactArgs(spec, "/work/build", earlyNextDay)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		args []string
		day  string
	}{
		{args: before, day: "20260824"},
		{args: after, day: "20260825"},
	} {
		i := indexOf(tc.args, "--to-ref-location")
		if i < 0 {
			t.Fatalf("argv %q lacks --to-ref-location", tc.args)
		}
		v := tc.args[i+1]
		if !strings.HasPrefix(v, spec.BuildID+"-"+tc.day+"=") {
			t.Fatalf("--to-ref-location %q does not bucket into %s", v, tc.day)
		}
		if !strings.Contains(v, "/"+tc.day+"/") {
			t.Fatalf("--to-ref-location %q URI does not use the %s bucket", v, tc.day)
		}
		if i := indexOf(tc.args, "--manifest-config"); i < 0 || i+1 >= len(tc.args) || tc.args[i+1] != spec.Paths.ManifestConfig {
			t.Fatalf("location publication argv %q lacks Bundle manifest configuration", tc.args)
		}
	}
}

// TestPublishArtifactArgsWithoutPublishParent verifies the manifest-mode
// fallback is unchanged: no ref-location flag, the manifest config path is
// passed instead.
func TestPublishArtifactArgsWithoutPublishParent(t *testing.T) {
	spec := publishSpec("")
	args, err := publishArtifactArgs(spec, "/work/build", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"publish", "--quiet", "--manifest-config", "/etc/flatten/manifest.yaml", "/work/build"}
	if strings.Join(args, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("argv = %#v, want %#v", args, want)
	}
}

func indexOf(list []string, want string) int {
	for i, v := range list {
		if v == want {
			return i
		}
	}
	return -1
}
