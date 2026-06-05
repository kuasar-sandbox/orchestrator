package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/config"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/store"
)

// manifestKeyCmd implements `orchestrator-ctl manifest-key {add|remove|check|list}`
// — the create/build allowlist (the manifest_keys table). Keys are read from
// positional args or the MANIFEST_KEY env; output is fingerprints only (the 24-hex
// SHA256[:12] prefix), never the key material.
//
//	orchestrator-ctl manifest-key add    [--label L] <MANIFEST_KEY>...
//	orchestrator-ctl manifest-key remove                <MANIFEST_KEY>...
//	orchestrator-ctl manifest-key check                 <MANIFEST_KEY>...
//	orchestrator-ctl manifest-key list
func manifestKeyCmd(args []string, _ *slog.Logger) error {
	if len(args) == 0 {
		return fmt.Errorf("manifest-key: subcommand required: add|remove|check|list")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("manifest-key "+sub, flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/orchestrator-ctl/config.yaml", "config file")
	label := fs.String("label", "", "optional label (add)")
	_ = fs.Parse(rest)
	keys := fs.Args()
	if len(keys) == 0 {
		if v := os.Getenv("MANIFEST_KEY"); v != "" {
			keys = []string{v}
		}
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	box, err := secretbox.NewFromColonHex(cfg.EncryptionKeySpec())
	if err != nil {
		return err
	}
	st, err := store.Open(cfg.DBPath, box)
	if err != nil {
		return err
	}
	defer st.Close()
	ctx := context.Background()

	switch sub {
	case "add":
		if len(keys) == 0 {
			return fmt.Errorf("manifest-key add: provide MANIFEST_KEY arg(s) or env")
		}
		for _, k := range keys {
			fp, err := fingerprintHex(k)
			if err != nil {
				return err
			}
			added, err := st.AddManifestKey(ctx, k, *label)
			if err != nil {
				return err
			}
			if added {
				fmt.Printf("added    %s\n", fp)
			} else {
				fmt.Printf("exists   %s\n", fp)
			}
		}
	case "remove":
		if len(keys) == 0 {
			return fmt.Errorf("manifest-key remove: provide MANIFEST_KEY arg(s) or env")
		}
		for _, k := range keys {
			fp, err := fingerprintHex(k)
			if err != nil {
				return err
			}
			n, err := st.RemoveManifestKey(ctx, k)
			if err != nil {
				return err
			}
			if n > 0 {
				fmt.Printf("removed  %s\n", fp)
			} else {
				fmt.Printf("absent   %s\n", fp)
			}
		}
	case "check":
		if len(keys) == 0 {
			return fmt.Errorf("manifest-key check: provide MANIFEST_KEY arg(s) or env")
		}
		for _, k := range keys {
			fp, err := fingerprintHex(k)
			if err != nil {
				return err
			}
			ok, err := st.HasManifestKey(ctx, k)
			if err != nil {
				return err
			}
			word := "absent"
			if ok {
				word = "present"
			}
			fmt.Printf("%-8s %s\n", word, fp)
		}
	case "list":
		infos, err := st.ListManifestKeys(ctx)
		if err != nil {
			return err
		}
		for _, mi := range infos {
			fmt.Printf("%s  %-24s  %s\n", mi.Hash, mi.Label, time.Unix(mi.CreatedUnix, 0).UTC().Format(time.RFC3339))
		}
	default:
		return fmt.Errorf("manifest-key: unknown subcommand %q (want add|remove|check|list)", sub)
	}
	return nil
}

// fingerprintHex validates a 64-hex manifest key and returns its 24-hex fingerprint.
func fingerprintHex(manifestKeyHex string) (string, error) {
	raw, err := hex.DecodeString(manifestKeyHex)
	if err != nil || len(raw) != 32 {
		return "", fmt.Errorf("manifest-key: %q is not a 64-hex (32-byte) key", manifestKeyHex)
	}
	return hex.EncodeToString(apikey.Fingerprint(raw)), nil
}
