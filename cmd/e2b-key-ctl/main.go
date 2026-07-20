// Command e2b-key-ctl is a pure-derivation tool for e2b credentials. It mints e2b
// API keys from an AuthKey, generates random 32-byte keys, and prints
// fingerprints — no DB, config, or orchestrator state. ManifestKey is an
// independent content-encryption root and is used only for pull-token sealing.
//
//	e2b-key-ctl gen-apikey  [<AUTH_KEY>]       # derive an e2b API key (or AUTH_KEY env)
//	e2b-key-ctl gen-key                         # random 32-byte key (64-hex)
//	e2b-key-ctl fingerprint [<KEY>]            # 24-hex fingerprint
//	e2b-key-ctl seal-pull-token [<MANIFEST_KEY>] --registry-username/-password | -token
//	                                            # opaque registry pull token for api_headers
//	e2b-key-ctl version
package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
)

var version = "0.1.0-dev"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "gen-apikey":
		ak, err := apikey.Mint(keyArg(os.Args[2:], "AUTH_KEY", "AuthKey"))
		check(err)
		fmt.Println(ak)
	case "gen-key":
		raw := make([]byte, 32)
		_, err := rand.Read(raw)
		check(err)
		fmt.Println(hex.EncodeToString(raw))
	case "fingerprint":
		fmt.Println(hex.EncodeToString(apikey.Fingerprint(keyArg(os.Args[2:], "AUTH_KEY", "key"))))
	case "seal-pull-token":
		check(sealPullToken(os.Args[2:]))
	case "version", "-v", "--version":
		fmt.Println("e2b-key-ctl", version)
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  e2b-key-ctl gen-apikey  [<AUTH_KEY>]       # derive an e2b API key (or AUTH_KEY env)
  e2b-key-ctl gen-key                         # generate a random 32-byte key (64-hex)
  e2b-key-ctl fingerprint [<KEY>]            # print the 24-hex fingerprint (or AUTH_KEY env)
  e2b-key-ctl seal-pull-token [<MANIFEST_KEY>] {--registry-username U --registry-password P | --registry-token T}
                                              # opaque pull token for the SDK's api_headers (X-Kuasar-Pull-Token)
  e2b-key-ctl version`)
	os.Exit(2)
}

// sealPullToken seals registry pull creds into an opaque, manifest-key-bound token
// the tenant passes via api_headers on a build (the orchestrator opens it with the
// tenant's stored manifest key).
func sealPullToken(args []string) error {
	// Pull a leading positional manifest key out before flag parsing (Go's flag
	// package stops at the first positional), so both orderings work.
	mkHex := os.Getenv("MANIFEST_KEY")
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		mkHex, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet("seal-pull-token", flag.ExitOnError)
	user := fs.String("registry-username", "", "registry username")
	pass := fs.String("registry-password", "", "registry password")
	token := fs.String("registry-token", "", "registry bearer token (wins over username/password)")
	_ = fs.Parse(args)
	if mkHex == "" {
		return fmt.Errorf("provide MANIFEST_KEY as the first argument or env")
	}
	tok, err := regcreds.Seal(mkHex, regcreds.Creds{Username: *user, Password: *pass, Token: *token})
	if err != nil {
		return err
	}
	fmt.Println(tok)
	return nil
}

// keyArg reads one canonical 32-byte hexadecimal key from an argument or env.
func keyArg(args []string, envName, displayName string) []byte {
	hexKey := os.Getenv(envName)
	if len(args) > 0 {
		hexKey = args[0]
	}
	if hexKey == "" {
		fmt.Fprintf(os.Stderr, "e2b-key-ctl: provide %s as an argument or %s env\n", displayName, envName)
		os.Exit(2)
	}
	raw, err := hex.DecodeString(hexKey)
	if err != nil || len(raw) != 32 {
		fmt.Fprintf(os.Stderr, "e2b-key-ctl: %s must be 64 hex chars (32 bytes)\n", displayName)
		os.Exit(2)
	}
	return raw
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2b-key-ctl:", err)
		os.Exit(1)
	}
}
