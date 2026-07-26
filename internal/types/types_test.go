package types

import "testing"

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
