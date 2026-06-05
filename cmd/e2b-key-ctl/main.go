// Command e2b-key-ctl is a pure-derivation tool for e2b credentials. It mints e2b
// API keys from a tenant manifest key (the 64-hex root secret), generates new
// manifest keys, and prints fingerprints — no DB, config, or orchestrator state.
// The minted api key is what the e2b SDK uses (E2B_API_KEY); the manifest key
// stays with the operator and is registered via `orchestrator-ctl manifest-key add`.
//
//	e2b-key-ctl gen-apikey  [<MANIFEST_KEY>]   # derive an e2b API key (or MANIFEST_KEY env)
//	e2b-key-ctl gen-key                         # random 32-byte manifest key (64-hex)
//	e2b-key-ctl fingerprint [<MANIFEST_KEY>]   # 24-hex fingerprint (matches the allowlist)
//	e2b-key-ctl version
package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
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
  e2b-key-ctl version`)
	os.Exit(2)
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
