package regcreds

import (
	"strings"
	"testing"
)

func TestSealOpenRoundTrip(t *testing.T) {
	mk := strings.Repeat("a", 64)
	c := Creds{Username: "u", Password: "p"}
	tok, err := Seal(mk, c)
	if err != nil || !strings.HasPrefix(tok, "kpt_") {
		t.Fatalf("seal: tok=%q err=%v", tok, err)
	}
	if got, err := Open(mk, tok); err != nil || got != c {
		t.Fatalf("open: got=%+v err=%v", got, err)
	}
	// a different tenant key cannot open the token.
	if _, err := Open(strings.Repeat("b", 64), tok); err == nil {
		t.Fatal("open with the wrong manifest key should fail")
	}
	// bearer token round-trips.
	tok2, _ := Seal(mk, Creds{Token: "bearer-xyz"})
	if got, _ := Open(mk, tok2); got.Token != "bearer-xyz" {
		t.Fatalf("bearer: %+v", got)
	}
	if _, err := Open(mk, "notatoken"); err == nil {
		t.Fatal("a non-kpt_ string should not open")
	}
	if _, err := Seal(mk, Creds{}); err == nil {
		t.Fatal("sealing empty creds should error")
	}
}

func TestDockerAuth(t *testing.T) {
	js, err := AssembleDockerAuth(Creds{Username: "u", Password: "p"})
	if err != nil || ValidateDockerAuth(js) != nil {
		t.Fatalf("assemble/validate: %q %v", js, err)
	}
	if c := CredsForImage(js, "registry.example.com/foo/bar:tag"); c.Username != "u" || c.Password != "p" {
		t.Fatalf("catch-all should match any host: %+v", c)
	}
	multi := `{"auths":{"reg.io":{"username":"ru","password":"rp"},"*":{"username":"cu","password":"cp"}}}`
	if c := CredsForImage(multi, "reg.io/x:y"); c.Username != "ru" {
		t.Fatalf("exact host should win: %+v", c)
	}
	if c := CredsForImage(multi, "other.io/x:y"); c.Username != "cu" {
		t.Fatalf("catch-all fallback: %+v", c)
	}
	if c := CredsForImage(`{"auths":{"*":{"auth":"dXNlcjpwYXNz"}}}`, "x/y:z"); c.Username != "user" || c.Password != "pass" {
		t.Fatalf("base64 auth decode: %+v", c)
	}
	if c := CredsForImage("", "x/y"); !c.Empty() {
		t.Fatal("empty json => empty creds")
	}
	if ValidateDockerAuth(`{"auths":{}}`) == nil {
		t.Fatal("empty auths should be rejected")
	}
}

func TestFlattenEnv(t *testing.T) {
	if e := (Creds{Token: "t"}).FlattenEnv(); e[EnvToken] != "t" || len(e) != 1 {
		t.Fatalf("token env: %v", e)
	}
	if e := (Creds{Username: "u", Password: "p"}).FlattenEnv(); e[EnvUsername] != "u" || e[EnvPassword] != "p" {
		t.Fatalf("basic env: %v", e)
	}
	if e := (Creds{}).FlattenEnv(); e != nil {
		t.Fatalf("empty env should be nil: %v", e)
	}
}

func TestRegistryHost(t *testing.T) {
	for ref, want := range map[string]string{
		"registry.example.com/foo:bar": "registry.example.com",
		"127.0.0.1:5000/x":             "127.0.0.1:5000",
		"localhost/y":                  "localhost",
		"alpine:3":                     "docker.io",
		"library/alpine":               "docker.io",
	} {
		if got := RegistryHost(ref); got != want {
			t.Errorf("RegistryHost(%q)=%q want %q", ref, got, want)
		}
	}
}
