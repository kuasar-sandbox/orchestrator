package proxy

import (
	"crypto/subtle"
	"net/http"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/config"
)

// authorized enforces the data-plane access token. The e2b SDK (secure sandboxes,
// default since v2.0.0) sends the sandbox access token as the X-Access-Token header
// on data-plane calls; envd validates it in-guest, and we re-check it at the proxy
// (defense in depth, and the only check for ports envd does not front: the
// code-interpreter port and user floatingip ports).
//
// Exemptions: auth off, a route with no token (bare), and envd pre-signed file
// URLs (a `signature` query, which envd itself validates — no header is sent).
func (p *Proxy) authorized(r *http.Request, route Route) bool {
	mode := p.authMode()
	if mode == config.AuthOff || route.AccessToken == "" {
		return true
	}
	if r.URL.Query().Get("signature") != "" {
		return true // envd pre-signed file URL; envd validates the signature
	}
	tok := r.Header.Get("X-Access-Token")
	if subtle.ConstantTimeCompare([]byte(tok), []byte(route.AccessToken)) == 1 {
		return true
	}
	if mode == config.AuthLog {
		p.log.Warn("data-plane access token mismatch (log mode; forwarding anyway)",
			"has_header", tok != "", "path", r.URL.Path)
		return true
	}
	return false
}
