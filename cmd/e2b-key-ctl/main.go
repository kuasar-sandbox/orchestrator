// Command e2b-key-ctl is a credential tool for e2b. It mints e2b
// API keys from a tenant API secret, derives the default API secret associated
// with a manifest key, and generates root secrets without a database or an
// orchestrator connection. APISecret authenticates API requests; ManifestKey
// protects content and may use a configured Secret plugin reference.
//
//	e2b-key-ctl gen-apikey        [<API_SECRET>]
//	e2b-key-ctl derive-api-secret [<MANIFEST_KEY>]
//	e2b-key-ctl gen-key
//	e2b-key-ctl fingerprint       [<API_SECRET>]
//	e2b-key-ctl seal-pull-token [<MANIFEST_KEY>] --registry-username/-password | -token
//	                                            # opaque registry pull token for api_headers
//	e2b-key-ctl --version
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/secret"
	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/manifestkey"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
	"github.com/kuasar-sandbox/orchestrator/internal/version"
)

func main() {
	if version.PrintIfRequested("e2b-key-ctl") {
		return
	}
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "gen-apikey":
		ak, err := apikey.Mint(rootSecretArg(os.Args[2:], "API_SECRET"))
		check(err)
		fmt.Println(ak)
	case "derive-api-secret":
		fmt.Println(hex.EncodeToString(apikey.DeriveAPISecret(rootSecretArg(os.Args[2:], "MANIFEST_KEY"))))
	case "gen-key":
		raw := make([]byte, 32)
		_, err := rand.Read(raw)
		check(err)
		fmt.Println(hex.EncodeToString(raw))
	case "fingerprint":
		fmt.Println(hex.EncodeToString(apikey.FullFingerprint(rootSecretArg(os.Args[2:], "API_SECRET"))))
	case "seal-pull-token":
		check(sealPullToken(os.Args[2:]))
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  e2b-key-ctl gen-apikey        [<API_SECRET>]   # mint an e2b API key (or API_SECRET env)
  e2b-key-ctl derive-api-secret [<MANIFEST_KEY>] # derive the default API secret (or MANIFEST_KEY env)
  e2b-key-ctl gen-key                             # generate a random 32-byte root secret (64-hex)
  e2b-key-ctl fingerprint       [<API_SECRET>]   # print the full 64-hex API-secret fingerprint
  e2b-key-ctl seal-pull-token [<MANIFEST_KEY>] {--registry-username U --registry-password P | --registry-token T}
                                              # opaque pull token for the SDK's api_headers (X-Kuasar-Pull-Token)
  e2b-key-ctl --version`)
	os.Exit(2)
}

// sealPullToken seals registry pull creds into an opaque, manifest-key-bound token
// the tenant passes via api_headers on a build (the orchestrator opens it with the
// tenant's stored manifest key).
func sealPullToken(args []string) error {
	// Pull a leading positional manifest key out before flag parsing (Go's flag
	// package stops at the first positional), so both orderings work.
	manifestText := os.Getenv("MANIFEST_KEY")
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		manifestText, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet("seal-pull-token", flag.ExitOnError)
	user := fs.String("registry-username", "", "registry username")
	pass := fs.String("registry-password", "", "registry password")
	token := fs.String("registry-token", "", "registry bearer token (wins over username/password)")
	_ = fs.Parse(args)
	if manifestText == "" {
		return fmt.Errorf("provide MANIFEST_KEY as the first argument or env")
	}
	tok, err := regcreds.SealContext(context.Background(), manifestText, regcreds.Creds{Username: *user, Password: *pass, Token: *token})
	if err != nil {
		return err
	}
	fmt.Println(tok)
	return nil
}

// rootSecretArg reads a root secret from the first argument or the named
// environment variable. Only MANIFEST_KEY accepts Secret plugin references.
func rootSecretArg(args []string, envName string) []byte {
	text := os.Getenv(envName)
	if len(args) > 0 {
		text = args[0]
	}
	raw, err := resolveRootSecret(context.Background(), text, envName)
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2b-key-ctl:", err)
		os.Exit(2)
	}
	return raw
}

func resolveRootSecret(ctx context.Context, text, envName string) ([]byte, error) {
	if text == "" {
		return nil, fmt.Errorf("provide %s as an argument or env", envName)
	}
	if envName == "MANIFEST_KEY" {
		if !secret.IsReference(text) && (len(text) != 64 || text != strings.ToLower(text)) {
			return nil, fmt.Errorf("%s must be 64 lowercase hex chars (32 bytes)", envName)
		}
		key, err := manifestkey.Resolve(ctx, text)
		if err != nil {
			return nil, err
		}
		return key[:], nil
	}
	raw, err := hex.DecodeString(text)
	if err != nil || len(raw) != 32 || text != strings.ToLower(text) {
		clear(raw)
		return nil, fmt.Errorf("%s must be 64 lowercase hex chars (32 bytes)", envName)
	}
	return raw, nil
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2b-key-ctl:", err)
		os.Exit(1)
	}
}
