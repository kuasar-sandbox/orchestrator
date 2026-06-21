package groupcfg

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// GroupHeader carries the group id on external provider requests (in a header,
// never the query string — the manifest_key is in the response body only, and
// group ids stay out of access logs).
const GroupHeader = "X-Kuasar-Sandbox-Group"

// External provider wire paths (cluster.md §6.2).
const (
	pathKey       = "/groupcfg/key"
	pathSandbox   = "/groupcfg/sandbox-config"
	pathPlacement = "/groupcfg/placement"
)

// func adapters so a closure satisfies a provider interface.
type keyFunc func(context.Context, string) (Key, bool, error)

func (f keyFunc) Key(ctx context.Context, g string) (Key, bool, error) { return f(ctx, g) }

type sandboxFunc func(context.Context, string) (SandboxConfig, bool, error)

func (f sandboxFunc) SandboxConfig(ctx context.Context, g string) (SandboxConfig, bool, error) {
	return f(ctx, g)
}

type placementFunc func(context.Context, string) (Placement, bool, error)

func (f placementFunc) Placement(ctx context.Context, g string) (Placement, bool, error) {
	return f(ctx, g)
}

// NewExternalKey / NewExternalSandbox / NewExternalPlacement build cache-fronted
// HTTP providers dialing addr (UDS path / host:port; tlsCfg non-nil = mTLS h2).
func NewExternalKey(addr string, tlsCfg *tls.Config, ttl time.Duration) KeyProvider {
	base, client := httpClient(addr, tlsCfg)
	c := newCache(ttl, func(ctx context.Context, g string) (Key, bool, error) {
		var k Key
		ok, err := getJSON(ctx, client, base, pathKey, g, &k)
		return k, ok, err
	})
	return keyFunc(c.lookup)
}

func NewExternalSandbox(addr string, tlsCfg *tls.Config, ttl time.Duration) SandboxConfigProvider {
	base, client := httpClient(addr, tlsCfg)
	c := newCache(ttl, func(ctx context.Context, g string) (SandboxConfig, bool, error) {
		var s SandboxConfig
		ok, err := getJSON(ctx, client, base, pathSandbox, g, &s)
		return s, ok, err
	})
	return sandboxFunc(c.lookup)
}

func NewExternalPlacement(addr string, tlsCfg *tls.Config, ttl time.Duration) PlacementProvider {
	base, client := httpClient(addr, tlsCfg)
	c := newCache(ttl, func(ctx context.Context, g string) (Placement, bool, error) {
		var p Placement
		ok, err := getJSON(ctx, client, base, pathPlacement, g, &p)
		return p, ok, err
	})
	return placementFunc(c.lookup)
}

func httpClient(addr string, tlsCfg *tls.Config) (string, *http.Client) {
	tr := &http.Transport{}
	base := "http://" + addr
	switch {
	case strings.HasPrefix(addr, "/"):
		base = "http://groupcfg"
		tr = &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}}
	case tlsCfg != nil:
		base = "https://" + addr
		tr = &http.Transport{TLSClientConfig: tlsCfg, ForceAttemptHTTP2: true}
	}
	return base, &http.Client{Timeout: 5 * time.Second, Transport: tr}
}

func getJSON(ctx context.Context, client *http.Client, base, path, group string, out any) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set(GroupHeader, group)
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return false, err
		}
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("groupcfg %s: %s", path, resp.Status)
	}
}
