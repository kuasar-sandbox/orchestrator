package types

import (
	"strings"
	"testing"
)

func TestValidateBuildID(t *testing.T) {
	for _, test := range []struct {
		name string
		id   string
		ok   bool
	}{
		{name: "one byte", id: "A", ok: true},
		{name: "forty eight bytes", id: strings.Repeat("z", 48), ok: true},
		{name: "mixed alphabet", id: "Build_ID-09", ok: true},
		{name: "empty", id: ""},
		{name: "forty nine bytes", id: strings.Repeat("z", 49)},
		{name: "slash", id: "build/id"},
		{name: "dot", id: "build.id"},
		{name: "space", id: "build id"},
		{name: "non ascii", id: "构建"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateBuildID(test.id); (err == nil) != test.ok {
				t.Fatalf("ValidateBuildID(%q) error = %v, want ok=%t", test.id, err, test.ok)
			}
			if got := ValidBuildID(test.id); got != test.ok {
				t.Fatalf("ValidBuildID(%q) = %t, want %t", test.id, got, test.ok)
			}
		})
	}
}
