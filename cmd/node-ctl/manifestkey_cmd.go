package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
)

// buildRegistryAuth produces the tenant-default registry auth (a docker config.json)
// from either a --registry-auth file or simple --registry-username/--password/--token
// (auto-assembled under a catch-all "*" entry). Returns "" when none was given.
func buildRegistryAuth(file, user, pass, token string) (string, error) {
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("manifest-key: read --registry-auth: %w", err)
		}
		if err := regcreds.ValidateDockerAuth(string(b)); err != nil {
			return "", err
		}
		return string(b), nil
	}
	if user != "" || token != "" {
		return regcreds.AssembleDockerAuth(regcreds.Creds{Username: user, Password: pass, Token: token})
	}
	return "", nil
}

// manifestKeyCmd implements `node-ctl manifest-key {add|remove|check|list}`
// — the tenant APISecret/ManifestKey pair allowlist. It is a thin client of the
// running serve daemon's admin plane (the local control socket): the daemon is the
// sole owner of the table. Manifest keys are read from positional args or the
// MANIFEST_KEY env. APISecret is optional and otherwise derived at ingestion;
// output contains complete fingerprints only, never either secret.
// Admin authorization is by SO_PEERCRED (admin_pidfile allowlist, or the socket's
// 0600 perms — same uid / root — when admin_pidfile is unset).
//
//	node-ctl manifest-key add    [--api-secret S] [--label L] [--socket S] <MANIFEST_KEY>...
//	node-ctl manifest-key remove [--api-secret S]             [--socket S] <MANIFEST_KEY>...
//	node-ctl manifest-key check  [--api-secret S]             [--socket S] <MANIFEST_KEY>...
//	node-ctl manifest-key list              [--socket S]
func manifestKeyCmd(args []string, _ *slog.Logger) error {
	if len(args) == 0 {
		return fmt.Errorf("manifest-key: subcommand required: add|remove|check|list")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("manifest-key "+sub, flag.ExitOnError)
	socket := fs.String("socket", "", "orchestrator control socket (or NODE_CTL_SOCKET env)")
	apiSecret := fs.String("api-secret", os.Getenv("API_SECRET"), "optional APISecret; default derives from each ManifestKey")
	label := fs.String("label", "", "optional label (add)")
	ttl := fs.Duration("ttl", 0, "add: expire the key after this duration (e.g. 24h); 0 = never. Re-adding refreshes it.")
	regAuthFile := fs.String("registry-auth", "", "add: tenant-default registry creds as a docker config.json file")
	regUser := fs.String("registry-username", "", "add: tenant-default registry username (assembles a catch-all auth)")
	regPass := fs.String("registry-password", "", "add: tenant-default registry password")
	regToken := fs.String("registry-token", "", "add: tenant-default registry bearer token (assembles a catch-all auth)")
	_ = fs.Parse(rest)
	sock := resolveSocket(*socket)

	if sub == "list" {
		return manifestKeyList(sock)
	}
	registryAuth, err := buildRegistryAuth(*regAuthFile, *regUser, *regPass, *regToken)
	if err != nil {
		return err
	}

	keys := fs.Args()
	if len(keys) == 0 {
		if v := os.Getenv("MANIFEST_KEY"); v != "" {
			keys = []string{v}
		}
	}
	if len(keys) == 0 {
		return fmt.Errorf("manifest-key %s: provide MANIFEST_KEY arg(s) or env", sub)
	}
	if *apiSecret != "" && len(keys) != 1 {
		return fmt.Errorf("manifest-key %s: --api-secret requires exactly one MANIFEST_KEY", sub)
	}
	switch sub {
	case "add", "remove", "check":
		for _, k := range keys {
			req := configsock.AdminKeyRequest{
				Op: sub, ManifestKey: k, APISecret: *apiSecret, Label: *label,
				TTLSeconds: int64(ttl.Seconds()), RegistryAuth: registryAuth,
			}
			code, body, err := udsDo(sock, http.MethodPost, configsock.PathAdminManifestKey, nil, req)
			if err != nil {
				return err
			}
			var resp configsock.AdminKeyResponse
			_ = json.Unmarshal(body, &resp)
			if code != http.StatusOK || resp.Error != "" {
				return fmt.Errorf("manifest-key %s: %s", sub, firstNonEmpty(resp.Error, string(body)))
			}
			fmt.Printf("%-9s api=%s manifest=%s\n", resp.Status, resp.APISecretFingerprint, resp.ManifestKeyFingerprint)
		}
	default:
		return fmt.Errorf("manifest-key: unknown subcommand %q (want add|remove|check|list)", sub)
	}
	return nil
}

func manifestKeyList(sock string) error {
	code, body, err := udsDo(sock, http.MethodGet, configsock.PathAdminManifestKey, nil, nil)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("manifest-key list: %s", apiMessage(body))
	}
	var infos []configsock.AdminKeyInfo
	if err := json.Unmarshal(body, &infos); err != nil {
		return fmt.Errorf("manifest-key list: decode: %w", err)
	}
	for _, mi := range infos {
		exp := "never"
		if mi.ExpiresUnix > 0 {
			exp = time.Unix(mi.ExpiresUnix, 0).UTC().Format(time.RFC3339)
		}
		fmt.Printf("api=%s  manifest=%s  %-24s  created=%s  expires=%s\n",
			mi.APISecretFingerprint, mi.ManifestKeyFingerprint, mi.Label,
			time.Unix(mi.CreatedUnix, 0).UTC().Format(time.RFC3339), exp)
	}
	return nil
}
