package cluster

import "testing"

func TestDeriveAccessToken(t *testing.T) {
	const key = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
	a, err := DeriveAccessToken(key, "sb-1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := DeriveAccessToken(key, "sb-1")
	if err != nil {
		t.Fatal(err)
	}
	c, err := DeriveAccessToken(key, "sb-2")
	if err != nil {
		t.Fatal(err)
	}
	if a == "" || a != b {
		t.Fatalf("token not deterministic: %q %q", a, b)
	}
	if a == c {
		t.Fatal("different sandbox ids derived the same access token")
	}
	if _, err := DeriveAccessToken("not-hex", "sb-1"); err == nil {
		t.Fatal("invalid auth key should fail")
	}
}
