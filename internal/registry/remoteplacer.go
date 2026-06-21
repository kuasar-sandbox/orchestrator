package registry

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// remotePlacer is a Placer that delegates to a standalone scaler over HTTP (the
// scaler subscribes the registry's view watches and serves POST /scaler/place;
// cluster.md §4.3/§5.2). The in-process scaler stays the default; this is the
// scale-out split. Because it implements Placer, the reserve hot path is
// unchanged — startReserve/ReserveBuild keep calling placer.PlaceSandbox.
type remotePlacer struct {
	base   string
	client *http.Client
}

// NewRemotePlacer builds a Placer that calls the scaler at addr (a UDS path, or
// host:port; tlsCfg non-nil = mTLS h2). placeTimeout bounds a placement call so a
// slow scaler can't consume the whole park budget (<=0 → 5s).
func NewRemotePlacer(addr string, tlsCfg *tls.Config, placeTimeout time.Duration) Placer {
	if placeTimeout <= 0 {
		placeTimeout = 5 * time.Second
	}
	p := &remotePlacer{}
	var transport http.RoundTripper
	switch {
	case strings.HasPrefix(addr, "/"):
		p.base = "http://scaler"
		transport = &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}}
	case tlsCfg != nil:
		p.base = "https://" + addr
		transport = &http.Transport{TLSClientConfig: tlsCfg, ForceAttemptHTTP2: true}
	default:
		p.base = "http://" + addr
		transport = &http.Transport{}
	}
	p.client = &http.Client{Timeout: placeTimeout, Transport: transport}
	return p
}

func (p *remotePlacer) PlaceSandbox(ctx context.Context, group, routeKey string) (string, error) {
	u := fmt.Sprintf("%s/scaler/place?group=%s&route_key=%s", p.base, url.QueryEscape(group), url.QueryEscape(routeKey))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return "", err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var out struct {
			NodeID string `json:"node_id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return "", err
		}
		if out.NodeID == "" {
			return "", ErrNoNode
		}
		return out.NodeID, nil
	case http.StatusConflict:
		return "", ErrNoNode // no eligible node
	default:
		return "", fmt.Errorf("scaler place: %s", resp.Status)
	}
}
