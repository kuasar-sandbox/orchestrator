// Package regcreds models registry pull credentials and the opaque, manifest-key-
// bound "pull token" a tenant can mint (e2b-key-ctl seal-pull-token) and pass to a
// build via api_headers. Credentials are username/password or a bearer token — no
// host matching (a build pulls one image); the env mapping matches what flatten-ctl
// reads (FLATTEN_REGISTRY_*). Tenant-default credentials are stored as a (docker
// config.json-compatible) auths object so an operator can paste a real config.json,
// or have one auto-assembled from simple creds under a catch-all "*" entry.
package regcreds

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// Flatten env var names (mirror accelerator/pkg/remote/auth.go; token wins).
const (
	EnvToken    = "FLATTEN_REGISTRY_TOKEN"
	EnvUsername = "FLATTEN_REGISTRY_USERNAME"
	EnvPassword = "FLATTEN_REGISTRY_PASSWORD"
)

// Creds is a single set of registry pull credentials (no host).
type Creds struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Token    string `json:"token,omitempty"` // bearer
}

func (c Creds) Empty() bool { return c.Username == "" && c.Password == "" && c.Token == "" }

// FlattenEnv maps creds to the FLATTEN_REGISTRY_* env flatten-ctl reads. A bearer
// token wins over basic auth (same precedence as flatten's authenticator).
func (c Creds) FlattenEnv() map[string]string {
	switch {
	case c.Token != "":
		return map[string]string{EnvToken: c.Token}
	case c.Username != "":
		return map[string]string{EnvUsername: c.Username, EnvPassword: c.Password}
	default:
		return nil
	}
}

// --- tenant-default store form: a docker config.json "auths" object (+ a token
// extension for bearer defaults) ---

type dockerAuth struct {
	Auths map[string]dockerAuthEntry `json:"auths"`
}

type dockerAuthEntry struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Auth     string `json:"auth,omitempty"`  // base64(user:pass)
	Token    string `json:"token,omitempty"` // bearer (our extension)
}

// AssembleDockerAuth builds a one-entry auths object (catch-all "*") from simple
// creds — the `manifest-key add --registry-username/--password/--token` convenience.
func AssembleDockerAuth(c Creds) (string, error) {
	b, err := json.Marshal(dockerAuth{Auths: map[string]dockerAuthEntry{
		"*": {Username: c.Username, Password: c.Password, Token: c.Token},
	}})
	return string(b), err
}

// ValidateDockerAuth checks that s parses as a docker auths object (for the
// `--registry-auth <file>` path) and is non-empty.
func ValidateDockerAuth(s string) error {
	var d dockerAuth
	if err := json.Unmarshal([]byte(s), &d); err != nil {
		return fmt.Errorf("regcreds: registry-auth is not a docker config.json: %w", err)
	}
	if len(d.Auths) == 0 {
		return fmt.Errorf("regcreds: registry-auth has no auths")
	}
	return nil
}

// CredsForImage picks the creds for an image ref from a stored auths object:
// exact registry host → "*" catch-all → the only/first entry. Returns empty creds
// when the json is empty or no entry matches.
func CredsForImage(authJSON, imageRef string) Creds {
	if strings.TrimSpace(authJSON) == "" {
		return Creds{}
	}
	var d dockerAuth
	if json.Unmarshal([]byte(authJSON), &d) != nil {
		return Creds{}
	}
	host := RegistryHost(imageRef)
	if e, ok := d.Auths[host]; ok {
		return entryCreds(e)
	}
	if e, ok := d.Auths["*"]; ok {
		return entryCreds(e)
	}
	for _, e := range d.Auths { // single / first
		return entryCreds(e)
	}
	return Creds{}
}

func entryCreds(e dockerAuthEntry) Creds {
	c := Creds{Username: e.Username, Password: e.Password, Token: e.Token}
	if c.Username == "" && e.Auth != "" {
		if dec, err := base64.StdEncoding.DecodeString(e.Auth); err == nil {
			if u, p, ok := strings.Cut(string(dec), ":"); ok {
				c.Username, c.Password = u, p
			}
		}
	}
	return c
}

// RegistryHost extracts the registry host from an image ref (best-effort; defaults
// to docker.io when the first segment is not a host). Used only to pick a host entry
// from a multi-registry docker config.json — the "*" catch-all covers the rest.
func RegistryHost(ref string) string {
	ref = strings.TrimPrefix(strings.TrimPrefix(ref, "https://"), "http://")
	i := strings.IndexByte(ref, '/')
	if i < 0 {
		return "docker.io"
	}
	first := ref[:i]
	if first == "localhost" || strings.ContainsAny(first, ".:") {
		return first
	}
	return "docker.io"
}

// --- manifest-key-bound pull token (task-level creds via api_headers) ---

const tokenPrefix = "kpt_"

// sealKey derives the AES-256 key for a tenant's pull tokens from their manifest
// key. The tenant holds the manifest key (their root secret) and the orchestrator
// holds it encrypted at rest, so both can seal/open without sharing an operator key.
func sealKey(manifestKeyHex string) ([]byte, error) {
	if len(manifestKeyHex) != 64 || manifestKeyHex != strings.ToLower(manifestKeyHex) {
		return nil, fmt.Errorf("regcreds: manifest key must be 64 lowercase hex characters")
	}
	if raw, err := hex.DecodeString(manifestKeyHex); err != nil || len(raw) != 32 {
		return nil, fmt.Errorf("regcreds: manifest key must be 64 lowercase hex characters")
	}
	k := sha256.Sum256([]byte("kuasar-pull-token-v1:" + manifestKeyHex))
	return k[:], nil
}

// Seal AES-256-GCM-encrypts creds with the tenant's sealing key, returning an opaque
// "kpt_<base64url>" token safe to carry in api_headers (only the orchestrator, with
// the tenant's manifest key, can open it).
func Seal(manifestKeyHex string, c Creds) (string, error) {
	if c.Empty() {
		return "", fmt.Errorf("regcreds: no credentials to seal")
	}
	gcm, err := newGCM(manifestKeyHex)
	if err != nil {
		return "", err
	}
	pt, _ := json.Marshal(c)
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	ct := gcm.Seal(nonce, nonce, pt, nil)
	return tokenPrefix + base64.RawURLEncoding.EncodeToString(ct), nil
}

// Open decrypts a kpt_ token with the tenant's sealing key.
func Open(manifestKeyHex, token string) (Creds, error) {
	if !strings.HasPrefix(token, tokenPrefix) {
		return Creds{}, fmt.Errorf("regcreds: not a pull token")
	}
	gcm, err := newGCM(manifestKeyHex)
	if err != nil {
		return Creds{}, err
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, tokenPrefix))
	if err != nil {
		return Creds{}, fmt.Errorf("regcreds: bad token encoding: %w", err)
	}
	if len(raw) < gcm.NonceSize() {
		return Creds{}, fmt.Errorf("regcreds: short token")
	}
	nonce, ct := raw[:gcm.NonceSize()], raw[gcm.NonceSize():]
	pt, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return Creds{}, fmt.Errorf("regcreds: token open failed (wrong tenant?): %w", err)
	}
	var c Creds
	if err := json.Unmarshal(pt, &c); err != nil {
		return Creds{}, err
	}
	return c, nil
}

func newGCM(manifestKeyHex string) (cipher.AEAD, error) {
	key, err := sealKey(manifestKeyHex)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
