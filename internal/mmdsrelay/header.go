package mmdsrelay

import (
	"regexp"
	"strings"
)

// tokenRE is the RFC 7230 §3.2.6 "token" grammar HTTP field names must match.
var tokenRE = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")

// deniedHeaderNames are Host/routing/forwarding/hop-by-hop headers the relay
// must never let a declared auth.header_name become — the relay handler
// already sets Host/Content-Length itself, and letting the caller pick one
// of these could let a declared endpoint smuggle or override
// transport-level framing.
var deniedHeaderNames = map[string]bool{
	"host":                true,
	"content-length":      true,
	"transfer-encoding":   true,
	"connection":          true,
	"keep-alive":          true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
	"te":                  true,
	"trailer":             true,
	"upgrade":             true,
	"x-forwarded-for":     true,
	"x-forwarded-host":    true,
	"x-forwarded-proto":   true,
	"forwarded":           true,
	"via":                 true,
}

// ValidAuthHeaderName reports whether name is a syntactically valid HTTP
// field name and not one of the denied transport/routing headers. Exported
// so mmdscfg can reject an invalid backend.auth.header_name at Create time
// with the exact same rule Fetch enforces at request time — otherwise a
// declaration that will always be rejected here (and so always fetch as a
// permanent 502) would be silently accepted at Create.
func ValidAuthHeaderName(name string) bool {
	if name == "" || !tokenRE.MatchString(name) {
		return false
	}
	return !deniedHeaderNames[strings.ToLower(name)]
}
