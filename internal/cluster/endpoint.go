package cluster

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

const maxCanonicalEndpointBytes = 512

func ValidateTCPDataEndpoint(endpoint string) error {
	if endpoint == "" || len(endpoint) > maxCanonicalEndpointBytes || !utf8.ValidString(endpoint) ||
		strings.TrimSpace(endpoint) != endpoint {
		return errors.New("cluster: data endpoint must be a canonical TCP host:port")
	}
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" || strings.ContainsAny(host, "/?#") {
		return errors.New("cluster: data endpoint must be a canonical TCP host:port")
	}
	for _, value := range []byte(host) {
		if value < 0x21 || value == 0x7f {
			return errors.New("cluster: data endpoint must be a canonical TCP host:port")
		}
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > 65535 || net.JoinHostPort(host, strconv.Itoa(port)) != endpoint {
		return errors.New("cluster: data endpoint must be a canonical TCP host:port")
	}
	return nil
}

func ValidateCanonicalHTTPSBaseEndpoint(endpoint string) error {
	if endpoint == "" || len(endpoint) > maxCanonicalEndpointBytes || !utf8.ValidString(endpoint) ||
		strings.TrimSpace(endpoint) != endpoint {
		return errors.New("cluster: service endpoint must be a canonical HTTPS base URL")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Path != "" ||
		parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.ForceQuery || parsed.Opaque != "" {
		return errors.New("cluster: service endpoint must be a canonical HTTPS base URL")
	}
	hostname := parsed.Hostname()
	if hostname == "" || hostname != strings.ToLower(hostname) {
		return errors.New("cluster: service endpoint must use a canonical lowercase host")
	}
	canonicalHost := hostname
	if strings.Contains(hostname, ":") {
		canonicalHost = "[" + hostname + "]"
	}
	if portText := parsed.Port(); portText != "" {
		port, err := strconv.Atoi(portText)
		if err != nil || port <= 0 || port > 65535 {
			return errors.New("cluster: service endpoint port must be numeric and nonzero")
		}
		canonicalHost = net.JoinHostPort(hostname, strconv.Itoa(port))
	}
	if parsed.Host != canonicalHost || parsed.String() != endpoint {
		return errors.New("cluster: service endpoint must be a canonical HTTPS base URL")
	}
	return nil
}
