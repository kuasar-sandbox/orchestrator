package main

import (
	"net"
	"net/url"
	"strings"
)

func endpointServerName(endpoint string) string {
	if endpoint == "" || strings.HasPrefix(endpoint, "/") {
		return ""
	}
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		u, err := url.Parse(endpoint)
		if err == nil {
			endpoint = u.Host
		}
	}
	host, _, err := net.SplitHostPort(endpoint)
	if err == nil {
		return host
	}
	return endpoint
}
