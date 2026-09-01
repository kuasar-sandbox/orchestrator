package types

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestParseProfile(t *testing.T) {
	for _, tt := range []struct {
		raw     string
		want    Profile
		wantErr bool
	}{
		{raw: "e2b", want: ProfileE2B},
		{raw: "bare", want: ProfileBare},
		{raw: "", wantErr: true},
		{raw: "other", wantErr: true},
	} {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := ParseProfile(tt.raw)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Fatalf("ParseProfile(%q) = %q, %v; want %q, error=%t", tt.raw, got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestTemplateIDPortableRef(t *testing.T) {
	key := strings.Repeat("a", 64)
	for _, tt := range []struct {
		kind Kind
		ref  string
	}{
		{kind: KindImg, ref: "manifest://" + key},
		{kind: KindSnp, ref: "manifest://" + key},
		{kind: KindImg, ref: "file://" + key + ".image@location:0198-build"},
		{kind: KindSnp, ref: "file://" + key + ".snapshot@location:0198-build"},
		{kind: KindSnp, ref: "file://" + key + ".bundle@location:0198-build"},
	} {
		want := TemplateID{Profile: ProfileE2B, Kind: tt.kind, Ref: tt.ref}
		got, err := ParseTemplateID(want.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("ParseTemplateID() = %#v, want %#v", got, want)
		}
	}
}

func TestTemplateIDRejectsNonPortableOrWrongArtifact(t *testing.T) {
	encode := func(ref string) string {
		return "e2b-snp-" + base64.RawURLEncoding.EncodeToString([]byte(ref))
	}
	for _, raw := range []string{
		"e2b-snp-not-base64!",
		encode("file:///tmp/root.snapshot"),
		encode("file://root.image@location:build"),
		encode("manifest://short"),
	} {
		if _, err := ParseTemplateID(raw); err == nil {
			t.Fatalf("ParseTemplateID(%q) succeeded", raw)
		}
	}
}

func TestTemplateIDLengthLimits(t *testing.T) {
	fixed := "file://" + strings.Repeat("a", 64) + ".snapshot@digest:" +
		strings.Repeat("b", 64) + "@location:"
	ref := fixed + strings.Repeat("c", MaxPortableRefBytes-len(fixed))
	id := TemplateID{Profile: ProfileBare, Kind: KindSnp, Ref: ref}.String()
	if len(id) != MaxTemplateIDBytes {
		t.Fatalf("maximum template ID length = %d, want %d", len(id), MaxTemplateIDBytes)
	}
	if _, err := ParseTemplateID(id); err != nil {
		t.Fatalf("maximum template ID rejected: %v", err)
	}
	if _, err := ParseTemplateID(TemplateID{Profile: ProfileBare, Kind: KindSnp, Ref: ref + "c"}.String()); err == nil {
		t.Fatal("overlong template ID accepted")
	}
}

func TestValidLocalSandboxID(t *testing.T) {
	for _, tt := range []struct {
		id   string
		want bool
	}{
		{id: "a", want: true},
		{id: "0", want: true},
		{id: "a0", want: true},
		{id: "sandbox-01-g7", want: true},
		{id: "a-z", want: true},
		{id: "a" + strings.Repeat("-", 55) + "z", want: true},
		{id: "", want: false},
		{id: strings.Repeat("a", 58), want: false},
		{id: "-sandbox", want: false},
		{id: "sandbox-", want: false},
		{id: "Sandbox", want: false},
		{id: "sandbox_id", want: false},
		{id: "sandbox.id", want: false},
		{id: "sandbox/id", want: false},
		{id: "sandébox", want: false},
	} {
		if got := ValidLocalSandboxID(tt.id); got != tt.want {
			t.Errorf("ValidLocalSandboxID(%q) = %t, want %t", tt.id, got, tt.want)
		}
	}
}

func TestSandboxStableID(t *testing.T) {
	for _, tt := range []struct {
		name string
		sb   *Sandbox
		want string
	}{
		{name: "nil", want: ""},
		{name: "standalone fallback", sb: &Sandbox{ID: "local"}, want: "local"},
		{name: "standalone imported StableID", sb: &Sandbox{ID: "target", StableIDValue: "source"}, want: "source"},
		{
			name: "cluster fallback",
			sb:   &Sandbox{ID: "stable-g1", Cluster: &ClusterSandboxContext{Group: "/g", RouteKey: "rk"}},
			want: "stable-g1",
		},
		{
			name: "cluster explicit StableID",
			sb: &Sandbox{
				ID: "stable-g1", Cluster: &ClusterSandboxContext{Group: "/g", RouteKey: "rk"},
				StableIDValue: "stable",
			},
			want: "stable",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.sb.StableID(); got != tt.want {
				t.Fatalf("StableID() = %q, want %q", got, tt.want)
			}
		})
	}
}
