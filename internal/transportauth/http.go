package transportauth

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
)

const TrustDomain = "kuasar.internal"

type Role string

const (
	RoleRouter   Role = "router"
	RoleRegistry Role = "registry"
	RolePlacer   Role = "placer"
	RoleNode     Role = "node"
	RoleOperator Role = "operator"
)

func VerifyRequest(request *http.Request, allowed ...Role) error {
	if request == nil || request.TLS == nil || len(request.TLS.VerifiedChains) == 0 ||
		len(request.TLS.PeerCertificates) == 0 {
		return errors.New("transportauth: verified mutual TLS identity is required")
	}
	roles := make(map[Role]struct{}, len(allowed))
	for _, role := range allowed {
		roles[role] = struct{}{}
	}
	for _, identity := range request.TLS.PeerCertificates[0].URIs {
		role, _, ok := ParseIdentity(identity)
		if !ok {
			continue
		}
		if _, authorized := roles[role]; authorized {
			return nil
		}
	}
	return errors.New("transportauth: peer role is not authorized")
}

func ParseIdentity(identity *url.URL) (Role, string, bool) {
	if identity == nil || identity.Scheme != "spiffe" || identity.Host != TrustDomain ||
		identity.RawQuery != "" || identity.Fragment != "" {
		return "", "", false
	}
	parts := strings.Split(strings.Trim(identity.Path, "/"), "/")
	if len(parts) != 2 || parts[1] == "" {
		return "", "", false
	}
	role := Role(parts[0])
	switch role {
	case RoleRouter, RoleRegistry, RolePlacer, RoleNode, RoleOperator:
		return role, parts[1], true
	default:
		return "", "", false
	}
}

func Middleware(role Role, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if err := VerifyRequest(request, role); err != nil {
			http.Error(w, "untrusted internal identity", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, request)
	})
}
