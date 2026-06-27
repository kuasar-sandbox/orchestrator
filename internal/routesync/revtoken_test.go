package routesync

import "testing"

func TestRevToken(t *testing.T) {
	token := MakeRevToken("fp-a", 42)
	if token != "fp-a:42" {
		t.Fatalf("token=%q", token)
	}
	parsed, err := ParseRevToken(token)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Fingerprint != "fp-a" || parsed.Seq != 42 {
		t.Fatalf("parsed=%+v", parsed)
	}
	if seq, ok := CheckRevToken(token, "fp-a"); !ok || seq != 42 {
		t.Fatalf("check matched seq=%d ok=%v", seq, ok)
	}
	if seq, ok := CheckRevToken(token, "fp-b"); ok || seq != 0 {
		t.Fatalf("mismatched fingerprint seq=%d ok=%v", seq, ok)
	}
	for _, bad := range []string{"", "fp", "fp:", ":1", "fp:0", "fp:x"} {
		if _, err := ParseRevToken(bad); err == nil {
			t.Fatalf("ParseRevToken(%q) succeeded", bad)
		}
	}
}
