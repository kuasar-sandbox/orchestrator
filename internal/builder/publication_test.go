package builder

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func publicationSpec(parent string, manifest bool) *configsock.BuildSpec {
	return &configsock.BuildSpec{
		BuildID:                     "0198f7a1-1234-7234-9abc-0123456789ab",
		CheckpointRefLocationParent: parent,
		CheckpointRemoteManifest:    manifest,
		Paths: configsock.BuildPaths{
			ManifestConfig: "/etc/flatten/manifest.yaml",
			SandboxCtl:     "/usr/bin/sandbox-ctl",
		},
	}
}

func TestBuildPublicationPlanDoesNotUseCheckpointModeForImageClass(t *testing.T) {
	for _, mode := range []string{"local", "bundle"} {
		spec := publicationSpec("file:///mnt/shared/snapshots", true)
		spec.CheckpointMode = mode
		plan, err := resolveBuildPublicationPlan(spec)
		if err != nil {
			t.Fatal(err)
		}
		if plan.ImageClassTarget != ImageClassCheckpointBundleLocation ||
			plan.CheckpointClassTarget != CheckpointClassRefLocation {
			t.Fatalf("mode %q changed publication plan: %+v", mode, plan)
		}
	}
}

func TestImportRefererWritebackFollowsFinalImagePolicy(t *testing.T) {
	for _, test := range []struct {
		name   string
		target types.BuildTarget
		image  ImageClassPublicationTarget
		want   bool
	}{
		{name: "image Manifest", target: types.BuildTarget{Kind: types.BuildTargetImage}, image: ImageClassManifestStore, want: true},
		{name: "top-level Sandbox E", target: types.BuildTarget{Kind: types.BuildTargetSandbox}, image: ImageClassManifestStore},
		{name: "memory Sandbox Manifest image", target: types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true}, image: ImageClassManifestStore, want: true},
		{name: "image Bundle", target: types.BuildTarget{Kind: types.BuildTargetImage}, image: ImageClassCheckpointBundleLocation},
		{name: "memory Sandbox Bundle image", target: types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true}, image: ImageClassCheckpointBundleLocation},
	} {
		t.Run(test.name, func(t *testing.T) {
			pipeline := &buildPipeline{
				target:      test.target,
				publication: BuildPublicationPlan{ImageClassTarget: test.image},
			}
			if got := pipeline.allowsImportRefererManifestWriteback(); got != test.want {
				t.Fatalf("allowsImportRefererManifestWriteback = %t, want %t", got, test.want)
			}
		})
	}
}

func TestResolveBuildPublicationPlanMatrix(t *testing.T) {
	for _, test := range []struct {
		name      string
		parent    string
		manifest  bool
		wantImage ImageClassPublicationTarget
		wantCheck CheckpointClassPublicationTarget
		wantErr   bool
	}{
		{
			name: "manifest stores", wantImage: ImageClassManifestStore,
			wantCheck: CheckpointClassManifestStore,
		},
		{
			name: "checkpoint location only", parent: "file:///mnt/shared/snapshots",
			wantImage: ImageClassManifestStore, wantCheck: CheckpointClassRefLocation,
		},
		{
			name: "image Bundle and checkpoint location", parent: "file:///mnt/shared/snapshots", manifest: true,
			wantImage: ImageClassCheckpointBundleLocation, wantCheck: CheckpointClassRefLocation,
		},
		{name: "image Bundle without parent", manifest: true, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, err := resolveBuildPublicationPlan(publicationSpec(test.parent, test.manifest))
			if (err != nil) != test.wantErr {
				t.Fatalf("resolveBuildPublicationPlan error = %v, wantErr=%t", err, test.wantErr)
			}
			if err == nil && (plan.ImageClassTarget != test.wantImage || plan.CheckpointClassTarget != test.wantCheck) {
				t.Fatalf("plan = %+v, want image=%q checkpoint=%q", plan, test.wantImage, test.wantCheck)
			}
		})
	}
}

// The checkpoint publication clock is independent of an earlier image Bundle
// publication, so a Build crossing UTC midnight uses the actual date for each.
func TestPublishCheckpointArtifactArgsMintPublicationDateAtUse(t *testing.T) {
	spec := publicationSpec("file:///mnt/shared/snapshots", true)
	imageName := reflocation.PublicationName(spec.BuildID, time.Date(2026, 8, 24, 23, 59, 0, 0, time.UTC))
	imageLocation, err := reflocation.Resolve(spec.CheckpointRefLocationParent, imageName)
	if err != nil {
		t.Fatal(err)
	}
	spec.RefLocations = map[string]string{imageName: imageLocation.URI}

	checkpointTime := time.Date(2026, 8, 25, 0, 1, 0, 0, time.UTC)
	args, err := publishCheckpointArtifactArgs(spec, CheckpointClassRefLocation, "/work/build.snapshot", checkpointTime)
	if err != nil {
		t.Fatal(err)
	}
	i := indexOf(args, "--to-ref-location")
	if i < 0 || i+1 >= len(args) || !strings.HasPrefix(args[i+1], spec.BuildID+"-20260825=") ||
		!strings.Contains(args[i+1], "/20260825/") {
		t.Fatalf("checkpoint output location = %#v", args)
	}
	i = indexOf(args, "--ref-location")
	if i < 0 || i+1 >= len(args) || args[i+1] != imageName+"="+imageLocation.URI {
		t.Fatalf("image input location missing from checkpoint publication argv: %#v", args)
	}
	if strings.Contains(args[i+1], "20260825") {
		t.Fatalf("earlier image mapping was rewritten into checkpoint date: %#v", args)
	}
}

func TestPublishCheckpointArtifactArgsManifestStore(t *testing.T) {
	spec := publicationSpec("file:///mnt/shared/snapshots", false)
	spec.RefLocations = map[string]string{"source-20260824": "file:///mnt/source"}
	args, err := publishCheckpointArtifactArgs(spec, CheckpointClassManifestStore, "/work/build.snapshot", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"publish", "--quiet", "--manifest-config", "/etc/flatten/manifest.yaml",
		"--ref-location", "source-20260824=file:///mnt/source", "/work/build.snapshot",
	}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("argv = %#v, want %#v", args, want)
	}
	if indexOf(args, "--to-ref-location") >= 0 {
		t.Fatalf("Manifest-store checkpoint unexpectedly has output location: %#v", args)
	}
}

func TestBuildArtifactRefLocationsStrictlyConvertsURIs(t *testing.T) {
	locations, err := buildArtifactRefLocations(map[string]string{
		"b-20260825": "file:///mnt/b",
		"a-20260824": "file:///mnt/a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if locations["a-20260824"] != "/mnt/a" || locations["b-20260825"] != "/mnt/b" {
		t.Fatalf("locations = %#v", locations)
	}
	for _, invalid := range []map[string]string{
		{"relative": "relative/path"},
		{"remote": "https://example.test/path"},
		{"": "file:///mnt/path"},
	} {
		if _, err := buildArtifactRefLocations(invalid); err == nil {
			t.Fatalf("invalid locations accepted: %#v", invalid)
		}
	}
}

func indexOf(list []string, want string) int {
	for i, value := range list {
		if value == want {
			return i
		}
	}
	return -1
}
