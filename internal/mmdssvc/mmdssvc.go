// Package mmdssvc relays a validated MMDS service route to an operator-owned
// local HTTP service over a Unix-domain socket. No guest request object enters
// this package, structurally preventing header/query/body forwarding.
package mmdssvc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"time"
)

const (
	HeaderSandboxID = "E2b-Sandbox-Id"
	HeaderService   = "E2b-Sandbox-Service"

	requestTimeout      = 2 * time.Second
	maxResponseBytes    = 64 * 1024
	maxContentTypeBytes = 256
)

// Registry maps a conductor service name to its parsed absolute socket path.
type Registry map[string]string

func UnixSocketPath(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	if u.Scheme != "unix" || u.Host != "" || u.Path == "" || !filepath.IsAbs(u.Path) ||
		u.User != nil || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return "", fmt.Errorf("%q must be unix://<absolute-path> without host, query, or fragment", endpoint)
	}
	return u.Path, nil
}

func BuildRegistry(endpoints map[string]string) (Registry, error) {
	registry := make(Registry, len(endpoints))
	for name, endpoint := range endpoints {
		if name == "" {
			return nil, errors.New("MMDS service name must not be empty")
		}
		path, err := UnixSocketPath(endpoint)
		if err != nil {
			return nil, fmt.Errorf("MMDS service %q: %w", name, err)
		}
		registry[name] = path
	}
	return registry, nil
}

type Result struct {
	StatusCode  int
	ContentType string
	Body        []byte
}

// Call constructs a fresh fixed GET and returns only the status, bounded body,
// and validated Content-Type. Missing Content-Type defaults to text/plain.
func Call(ctx context.Context, socketPath, serviceName, exactPath, sandboxID string) Result {
	if socketPath == "" || !filepath.IsAbs(socketPath) || serviceName == "" {
		return Result{StatusCode: http.StatusServiceUnavailable, ContentType: "text/plain"}
	}
	callCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	var dialed atomic.Bool
	transport := &http.Transport{
		DisableCompression: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
			if err == nil {
				dialed.Store(true)
			}
			return conn, err
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, "http://mmds-service"+exactPath, nil)
	if err != nil {
		return Result{StatusCode: http.StatusBadGateway, ContentType: "text/plain"}
	}
	req.Host = "mmds-service"
	req.Header.Set(HeaderSandboxID, sandboxID)
	req.Header.Set(HeaderService, serviceName)
	// An explicitly present empty slice suppresses net/http's default User-Agent.
	req.Header["User-Agent"] = nil

	response, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			return Result{StatusCode: http.StatusGatewayTimeout, ContentType: "text/plain"}
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return Result{StatusCode: http.StatusGatewayTimeout, ContentType: "text/plain"}
		}
		if !dialed.Load() {
			return Result{StatusCode: http.StatusServiceUnavailable, ContentType: "text/plain"}
		}
		return Result{StatusCode: http.StatusBadGateway, ContentType: "text/plain"}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 599 {
		return Result{StatusCode: http.StatusBadGateway, ContentType: "text/plain"}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(body) > maxResponseBytes {
		return Result{StatusCode: http.StatusBadGateway, ContentType: "text/plain"}
	}
	contentType, ok := responseContentType(response.Header)
	if !ok {
		return Result{StatusCode: http.StatusBadGateway, ContentType: "text/plain"}
	}
	return Result{StatusCode: response.StatusCode, ContentType: contentType, Body: body}
}

func responseContentType(header http.Header) (string, bool) {
	values := header.Values("Content-Type")
	if len(values) == 0 {
		return "text/plain", true
	}
	if len(values) != 1 || values[0] == "" || len(values[0]) > maxContentTypeBytes {
		return "", false
	}
	if _, _, err := mime.ParseMediaType(values[0]); err != nil {
		return "", false
	}
	return values[0], true
}
