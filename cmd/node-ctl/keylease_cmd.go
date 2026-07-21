package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/regcreds"
)

func buildRegistryAuth(file, user, pass, token string) (string, error) {
	if pass != "" && user == "" {
		return "", errors.New("key-lease: --registry-password requires --registry-username")
	}
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("key-lease: read --registry-auth: %w", err)
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

// keyLeaseCmd manages complete node-local AuthKey/ManifestKey leases through
// the running conductor's protected Unix socket.
func keyLeaseCmd(args []string, _ *slog.Logger) error {
	if len(args) == 0 {
		return fmt.Errorf("key-lease: subcommand required: put|drop|check|list")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("key-lease "+sub, flag.ExitOnError)
	socket := fs.String("socket", "", "orchestrator control socket (or NODE_CTL_SOCKET env)")
	group := fs.String("group", os.Getenv("KUASAR_GROUP"), "lease group (or KUASAR_GROUP env)")
	authKey := fs.String("auth-key", os.Getenv("AUTH_KEY"), "node API AuthKey (or AUTH_KEY env)")
	manifestKey := fs.String("manifest-key", os.Getenv("MANIFEST_KEY"), "content ManifestKey (or MANIFEST_KEY env)")
	label := fs.String("label", "", "optional label (put)")
	ttl := fs.Duration("ttl", 0, "put: lease lifetime; 0 means an explicit local non-expiring lease")
	regAuthFile := fs.String("registry-auth", "", "put: default registry credentials as docker config.json")
	regUser := fs.String("registry-username", "", "put: default registry username")
	regPass := fs.String("registry-password", "", "put: default registry password")
	regToken := fs.String("registry-token", "", "put: default registry bearer token")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	sock := resolveSocket(*socket)
	if sub == "list" {
		return keyLeaseList(sock)
	}
	if *group == "" || *authKey == "" || *manifestKey == "" {
		return fmt.Errorf("key-lease %s: --group, --auth-key and --manifest-key are required", sub)
	}
	if *ttl < 0 || (*ttl > 0 && *ttl < time.Second) {
		return errors.New("key-lease put: TTL must be zero or at least one second")
	}
	registryAuth, err := buildRegistryAuth(*regAuthFile, *regUser, *regPass, *regToken)
	if err != nil {
		return err
	}
	req := configsock.AdminKeyLeaseRequest{
		Op: sub, Group: *group, AuthKey: *authKey, ManifestKey: *manifestKey,
		Label: *label, TTLSeconds: int64(ttl.Seconds()), RegistryAuth: registryAuth,
	}
	code, body, err := udsDo(sock, http.MethodPost, configsock.PathAdminKeyLease, nil, req)
	if err != nil {
		return err
	}
	var resp configsock.AdminKeyLeaseResponse
	_ = json.Unmarshal(body, &resp)
	if code != http.StatusOK || resp.Error != "" {
		return fmt.Errorf("key-lease %s: %s", sub, firstNonEmpty(resp.Error, string(body)))
	}
	fmt.Printf("%-9s auth=%s manifest=%s\n", resp.Status, resp.AuthKeyFingerprint, resp.ManifestKeyFingerprint)
	return nil
}

func keyLeaseList(sock string) error {
	code, body, err := udsDo(sock, http.MethodGet, configsock.PathAdminKeyLease, nil, nil)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("key-lease list: %s", apiMessage(body))
	}
	var infos []configsock.AdminKeyLeaseInfo
	if err := json.Unmarshal(body, &infos); err != nil {
		return fmt.Errorf("key-lease list: decode: %w", err)
	}
	for _, info := range infos {
		expires := "never"
		if info.ExpiresUnix > 0 {
			expires = time.Unix(info.ExpiresUnix, 0).UTC().Format(time.RFC3339)
		}
		fmt.Printf("group=%s auth=%s manifest=%s label=%q created=%s expires=%s\n",
			info.Group, info.AuthKeyFingerprint, info.ManifestKeyFingerprint, info.Label,
			time.Unix(info.CreatedUnix, 0).UTC().Format(time.RFC3339), expires)
	}
	return nil
}
