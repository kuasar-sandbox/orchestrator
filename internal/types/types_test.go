package types

import (
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

func TestSandboxAuthSandboxID(t *testing.T) {
	for _, tt := range []struct {
		name string
		sb   *Sandbox
		want string
	}{
		{name: "nil", want: ""},
		{name: "standalone fallback", sb: &Sandbox{ID: "local"}, want: "local"},
		{name: "standalone imported subject", sb: &Sandbox{ID: "target", AuthSandboxIDValue: "source"}, want: "source"},
		{
			name: "cluster fallback",
			sb:   &Sandbox{ID: "stable-g1", Cluster: &ClusterSandboxContext{Group: "/g", RouteKey: "rk"}},
			want: "stable-g1",
		},
		{
			name: "cluster stable subject",
			sb: &Sandbox{
				ID: "stable-g1", Cluster: &ClusterSandboxContext{Group: "/g", RouteKey: "rk"},
				AuthSandboxIDValue: "stable",
			},
			want: "stable",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.sb.AuthSandboxID(); got != tt.want {
				t.Fatalf("AuthSandboxID() = %q, want %q", got, tt.want)
			}
		})
	}
}
