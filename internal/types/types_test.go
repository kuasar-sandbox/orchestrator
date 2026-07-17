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
