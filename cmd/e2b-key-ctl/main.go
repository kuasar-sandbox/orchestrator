// Command e2b-key-ctl is a pure-derivation tool for e2b credentials. It mints e2b
// API keys from a tenant manifest key (the 64-hex root secret), generates new
// manifest keys, and prints fingerprints — no DB, config, or orchestrator state.
// The minted api key is what the e2b SDK uses (E2B_API_KEY); the manifest key
// stays with the operator and is registered via `orchestrator-ctl manifest-key add`.
//
//	e2b-key-ctl gen-apikey  [<MANIFEST_KEY>]   # derive an e2b API key (or MANIFEST_KEY env)
//	e2b-key-ctl gen-key                         # random 32-byte manifest key (64-hex)
//	e2b-key-ctl fingerprint [<MANIFEST_KEY>]   # 24-hex fingerprint (matches the allowlist)
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

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/regcreds"
)

var version = "0.1.0-dev"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "gen-apikey":
		ak, err := apikey.Mint(manifestKeyArg(os.Args[2:]))
		check(err)
		fmt.Println(ak)
	case "gen-key":
		raw := make([]byte, 32)
		_, err := rand.Read(raw)
		check(err)
		fmt.Println(hex.EncodeToString(raw))
	case "fingerprint":
		fmt.Println(hex.EncodeToString(apikey.Fingerprint(manifestKeyArg(os.Args[2:]))))
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
  e2b-key-ctl gen-apikey  [<MANIFEST_KEY>]   # derive an e2b API key (or MANIFEST_KEY env)
  e2b-key-ctl gen-key                         # generate a random 32-byte manifest key (64-hex)
  e2b-key-ctl fingerprint [<MANIFEST_KEY>]   # print the 24-hex fingerprint
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

// manifestKeyArg reads the 64-hex manifest key from the first arg or MANIFEST_KEY env.
func manifestKeyArg(args []string) []byte {
	hexKey := os.Getenv("MANIFEST_KEY")
	if len(args) > 0 {
		hexKey = args[0]
	}
	if hexKey == "" {
		fmt.Fprintln(os.Stderr, "e2b-key-ctl: provide MANIFEST_KEY as an argument or env")
		os.Exit(2)
	}
	raw, err := hex.DecodeString(hexKey)
	if err != nil || len(raw) != 32 {
		fmt.Fprintln(os.Stderr, "e2b-key-ctl: MANIFEST_KEY must be 64 hex chars (32 bytes)")
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
