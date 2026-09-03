package types

import "testing"

func TestResolveBuildTarget(t *testing.T) {
	explicit := &BuildTarget{Kind: BuildTargetSandbox}
	for _, tc := range []struct {
		name         string
		requested    *BuildTarget
		start, ready string
		want         BuildTarget
	}{
		{name: "auto image", want: BuildTarget{Kind: BuildTargetImage}},
		{name: "auto start", start: "serve", want: BuildTarget{Kind: BuildTargetSandbox, Memory: true}},
		{name: "auto ready only", ready: "probe", want: BuildTarget{Kind: BuildTargetSandbox, Memory: true}},
		{name: "explicit wins", requested: explicit, start: "serve", want: *explicit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveBuildTarget(tc.requested, tc.start, tc.ready); got != tc.want {
				t.Fatalf("ResolveBuildTarget() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestBuildTargetValidationAndArtifactKind(t *testing.T) {
	valid := []struct {
		target BuildTarget
		kind   Kind
	}{
		{target: BuildTarget{Kind: BuildTargetImage}, kind: KindImg},
		{target: BuildTarget{Kind: BuildTargetSandbox}, kind: KindSbx},
		{target: BuildTarget{Kind: BuildTargetSandbox, Memory: true}, kind: KindSnp},
	}
	for _, tc := range valid {
		if err := tc.target.Validate(); err != nil {
			t.Fatalf("Validate(%+v): %v", tc.target, err)
		}
		if got := tc.target.ArtifactKind(); got != tc.kind {
			t.Fatalf("ArtifactKind(%+v) = %q, want %q", tc.target, got, tc.kind)
		}
	}
	for _, target := range []BuildTarget{
		{}, {Kind: "unknown"}, {Kind: BuildTargetImage, Memory: true},
	} {
		if err := target.Validate(); err == nil {
			t.Fatalf("Validate(%+v) accepted invalid target", target)
		}
	}
}
