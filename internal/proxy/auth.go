package proxy

import (
	"net/http"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/envdsign"
)

// authorized enforces the data-plane access token. The e2b SDK (secure sandboxes,
// default since v2.0.0) sends the sandbox access token as the X-Access-Token header
// on data-plane calls; envd validates it in-guest, and we re-check it at the proxy
// (defense in depth, and the only check for ports envd does not front: the
// code-interpreter port and user floatingip ports).
//
// Exemptions: auth off and envd pre-signed /files URLs. The proxy verifies those
// signatures before forwarding; envd then verifies the same URL again in-guest.
func (p *Proxy) authorized(r *http.Request, route Route, port int) bool {
	mode := p.authMode()
	if mode == config.AuthOff {
		return true
	}
	if route.AccessToken == "" {
		if mode == config.AuthLog {
			p.log.Warn("data-plane route has no expected access token (log mode; forwarding anyway)",
				"has_header", r.Header.Get(envdsign.AccessTokenHeader) != "", "path", r.URL.Path)
			return true
		}
		return false
	}
	res := envdsign.CheckDataPlaneAuth(r, port, route.AccessToken, time.Now())
	if res.OK {
		return true
	}
	if mode == config.AuthLog {
		p.log.Warn("data-plane auth mismatch (log mode; forwarding anyway)",
			"has_header", r.Header.Get(envdsign.AccessTokenHeader) != "", "path", r.URL.Path, "err", res.Err)
		return true
	}
	return false
}
